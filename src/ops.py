"""Operational instrumentation: replay speed control, ingest lag, heartbeats.

Three things a market-data team watches that the correctness layer does not
cover, because a pipeline can be perfectly correct and still be useless.

REPLAY SPEED. Replaying as-fast-as-possible measures how quickly the code can
chew a list. It cannot answer "does this hold up at 10x live rate", which is the
capacity question, because at full speed there is no notion of falling behind.
Pacing the replay to a wall-clock multiple of event time creates real backlog
when the consumer is too slow, and the backlog is the signal.

INGEST LAG. The distribution of (arrival time - event time). This is the number
an ops team actually watches, because it moves before anything breaks: a feed
that is drifting from 20ms to 200ms of lag is telling you about a problem you
still have time to fix. Reported as a distribution, never a mean -- lag is
long-tailed by nature and a mean hides exactly the excursions worth seeing.

HEARTBEAT LOSS vs DATA GAP. These are different failures and need different
responses, which is why conflating them is worse than not detecting either:

  data gap        no ticks, but the connection is alive and heartbeats arrive.
                  The market may simply be quiet. Flag the bars, do not page.
  heartbeat loss  no heartbeats either. The connection is dead or wedged and
                  we are BLIND -- a quiet market and a dead socket look
                  identical from the tick stream alone. Page immediately.

The distinction matters most at 3am: a quiet-market alert that pages someone
trains the team to ignore the alert that means the feed died.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field

from .replay import Tick


@dataclass
class PacedReplay:
    """Replay at a wall-clock multiple of event time.

    speed=1 replays in real time, speed=100 compresses an hour into 36 seconds.
    Because the pacing is real, a consumer that cannot keep up genuinely falls
    behind, and `backlog_ms` records by how much.
    """
    speed: float = 100.0
    max_sleep_s: float = 0.05

    def run(self, ticks: list[Tick], consume) -> dict:
        if not ticks:
            return {"ticks": 0}
        t0_event = ticks[0].event_time_ms
        t0_wall = time.perf_counter()
        max_backlog_ms = 0.0
        behind_count = 0

        for t in ticks:
            target_offset_s = ((t.event_time_ms - t0_event) / 1000.0) / self.speed
            actual_offset_s = time.perf_counter() - t0_wall
            drift_s = target_offset_s - actual_offset_s
            if drift_s > 0:
                time.sleep(min(drift_s, self.max_sleep_s))
            else:
                # Negative drift means the consumer is slower than the feed.
                backlog_ms = -drift_s * 1000.0 * self.speed
                max_backlog_ms = max(max_backlog_ms, backlog_ms)
                behind_count += 1
            consume(t)

        elapsed = time.perf_counter() - t0_wall
        span_s = (ticks[-1].event_time_ms - t0_event) / 1000.0
        return {
            "ticks": len(ticks),
            "requested_speed": self.speed,
            "achieved_speed": span_s / elapsed if elapsed else float("inf"),
            "elapsed_s": elapsed,
            "ticks_per_sec": len(ticks) / elapsed if elapsed else 0.0,
            "max_backlog_ms": max_backlog_ms,
            "ticks_behind": behind_count,
            "kept_up": behind_count == 0,
        }


@dataclass
class IngestLag:
    """Distribution of arrival - event time."""
    samples: list[float] = field(default_factory=list)

    def observe(self, event_time_ms: int, arrival_time_ms: int) -> None:
        self.samples.append(arrival_time_ms - event_time_ms)

    def percentile(self, p: float) -> float:
        if not self.samples:
            return 0.0
        s = sorted(self.samples)
        return s[min(int(round(p / 100 * (len(s) - 1))), len(s) - 1)]

    def report(self) -> dict:
        if not self.samples:
            return {"n": 0}
        return {
            "n": len(self.samples),
            "p50_ms": self.percentile(50),
            "p95_ms": self.percentile(95),
            "p99_ms": self.percentile(99),
            "max_ms": max(self.samples),
            "negative": sum(1 for s in self.samples if s < 0),
        }


@dataclass
class FeedHealth:
    """Separates a quiet market from a dead socket."""
    heartbeat_timeout_ms: int = 10_000
    data_gap_threshold_ms: int = 30_000

    last_tick_ms: int | None = None
    last_heartbeat_ms: int | None = None
    data_gaps: list[tuple[int, int]] = field(default_factory=list)
    heartbeat_losses: list[tuple[int, int]] = field(default_factory=list)

    def on_tick(self, event_time_ms: int) -> None:
        if (self.last_tick_ms is not None
                and event_time_ms - self.last_tick_ms > self.data_gap_threshold_ms):
            self.data_gaps.append((self.last_tick_ms, event_time_ms))
        self.last_tick_ms = event_time_ms

    def on_heartbeat(self, wall_ms: int) -> None:
        if (self.last_heartbeat_ms is not None
                and wall_ms - self.last_heartbeat_ms > self.heartbeat_timeout_ms):
            self.heartbeat_losses.append((self.last_heartbeat_ms, wall_ms))
        self.last_heartbeat_ms = wall_ms

    def status(self, wall_ms: int) -> dict:
        hb_age = (wall_ms - self.last_heartbeat_ms
                  if self.last_heartbeat_ms is not None else None)
        tick_age = (wall_ms - self.last_tick_ms
                    if self.last_tick_ms is not None else None)
        heartbeat_dead = hb_age is not None and hb_age > self.heartbeat_timeout_ms
        data_stale = tick_age is not None and tick_age > self.data_gap_threshold_ms

        if heartbeat_dead:
            # No heartbeat: we cannot distinguish a quiet market from a dead
            # socket, so we must assume the worst.
            state, action = "BLIND", "page: connection dead or wedged"
        elif data_stale:
            state, action = "QUIET", "flag bars suspect; do NOT page"
        else:
            state, action = "HEALTHY", "none"
        return {
            "state": state, "action": action,
            "heartbeat_age_ms": hb_age, "tick_age_ms": tick_age,
            "data_gaps": len(self.data_gaps),
            "heartbeat_losses": len(self.heartbeat_losses),
        }
