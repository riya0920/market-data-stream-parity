"""Replay harness with injected disorder.

A live WebSocket feed is not a test environment: it is unrepeatable, so a
correctness claim made against it cannot be re-checked. This module generates a
recorded tick session with known ground truth, then replays it with the four
disorders that actually break market-data pipelines:

  out-of-order   ticks arriving after later ticks (network paths, gateway fanout)
  duplicates     at-least-once delivery from the exchange or the broker
  gaps           feed silence -- the dangerous one, because nothing arrives to
                 tell you something is wrong
  heartbeat loss connection alive, data stopped

Ground truth is the clean tick list. Everything downstream is scored against it.
"""
from __future__ import annotations

import random
from dataclasses import dataclass


@dataclass(frozen=True)
class Tick:
    seq: int
    event_time_ms: int      # when the exchange stamped it
    price_minor: int        # integer minor units, never a float
    size: int

    def __hash__(self):
        return hash((self.seq, self.event_time_ms, self.price_minor, self.size))


def generate_session(n_ticks: int = 100_000, start_ms: int = 1_800_000_000_000,
                     ms_between: int = 150, seed: int = 13) -> list[Tick]:
    rng = random.Random(seed)
    price = 5_000_00
    ticks = []
    t = start_ms
    for i in range(n_ticks):
        price = max(1, price + rng.randint(-30, 30))
        t += max(1, int(rng.expovariate(1 / ms_between)))
        ticks.append(Tick(i, t, price, rng.randint(1, 500)))
    return ticks


@dataclass
class DisorderConfig:
    out_of_order_rate: float = 0.02
    max_delay_ms: int = 12_000
    duplicate_rate: float = 0.01
    gap_count: int = 3
    gap_duration_ms: int = 90_000

    # A late tick beyond this bound cannot revise its bar -- the bar is already
    # final and downstream consumers have acted on it. Choosing this number IS
    # the design decision: too small and you drop real data, too large and no bar
    # is ever trustworthy. 5s is stated, not assumed.
    watermark_bound_ms: int = 5_000


def replay(ticks: list[Tick], cfg: DisorderConfig, seed: int = 17):
    """Yields (arrival_order_index, tick). Ground truth stays `ticks`."""
    rng = random.Random(seed)

    # Carve gaps: drop every tick inside a silent window.
    if cfg.gap_count:
        span = ticks[-1].event_time_ms - ticks[0].event_time_ms
        gap_starts = sorted(rng.sample(
            range(ticks[0].event_time_ms, ticks[-1].event_time_ms - cfg.gap_duration_ms),
            cfg.gap_count))
        def in_gap(t):
            return any(g <= t.event_time_ms < g + cfg.gap_duration_ms for g in gap_starts)
        surviving = [t for t in ticks if not in_gap(t)]
    else:
        gap_starts, surviving = [], list(ticks)

    stream = []
    for t in surviving:
        delay = 0
        if rng.random() < cfg.out_of_order_rate:
            delay = rng.randint(1, cfg.max_delay_ms)
        stream.append((t.event_time_ms + delay, t))
        if rng.random() < cfg.duplicate_rate:
            stream.append((t.event_time_ms + delay + rng.randint(0, 50), t))

    stream.sort(key=lambda p: p[0])       # arrival order = processing time
    return [t for _, t in stream], gap_starts, surviving
