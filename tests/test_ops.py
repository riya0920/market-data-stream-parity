"""Replay pacing, ingest lag, and the heartbeat-vs-gap distinction."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.ops import FeedHealth, IngestLag, PacedReplay
from src.replay import Tick


def _ticks(n, step_ms=10, base=1_000_000):
    return [Tick(i, base + i * step_ms, 100_00, 1) for i in range(n)]


def test_paced_replay_respects_the_requested_speed():
    """At 100x, 5 seconds of event time must take about 50ms of wall clock."""
    ticks = _ticks(500, step_ms=10)          # 5s of event time
    r = PacedReplay(speed=100.0).run(ticks, lambda t: None)
    assert r["ticks"] == 500
    assert 0.02 < r["elapsed_s"] < 1.0
    assert r["achieved_speed"] > 5


def test_a_slow_consumer_falls_behind_and_the_backlog_is_recorded():
    """The point of pacing: at full speed nobody can fall behind, so capacity
    is unmeasurable."""
    ticks = _ticks(60, step_ms=1)
    import time

    def slow(_t):
        time.sleep(0.005)

    r = PacedReplay(speed=1000.0).run(ticks, slow)
    assert not r["kept_up"]
    assert r["ticks_behind"] > 0
    assert r["max_backlog_ms"] > 0


def test_ingest_lag_is_reported_as_a_distribution():
    lag = IngestLag()
    for i in range(99):
        lag.observe(1_000_000, 1_000_020)     # 20ms
    lag.observe(1_000_000, 1_002_000)         # one 2s excursion
    rep = lag.report()
    assert rep["p50_ms"] == 20
    assert rep["max_ms"] == 2000
    mean = sum(lag.samples) / len(lag.samples)
    assert mean < 40, "a mean hides the excursion; that is why p99 is reported"
    assert rep["p99_ms"] >= 20


def test_negative_lag_is_counted_not_hidden():
    """Arrival before event time means clock skew between us and the exchange.
    Silently clamping it to zero destroys the only evidence of the problem."""
    lag = IngestLag()
    lag.observe(1_000_000, 999_990)
    assert lag.report()["negative"] == 1


def test_quiet_market_does_not_page():
    h = FeedHealth()
    h.on_tick(1_000_000)
    h.on_heartbeat(1_000_000)
    s = h.status(1_040_000)          # 40s of no ticks, heartbeat also stale...
    # heartbeat timeout is shorter, so this reads as BLIND -- correct.
    assert s["state"] == "BLIND"

    h2 = FeedHealth()
    h2.on_tick(1_000_000)
    h2.on_heartbeat(1_039_000)       # heartbeat is fresh
    s2 = h2.status(1_040_000)
    assert s2["state"] == "QUIET"
    assert "do NOT page" in s2["action"]


def test_dead_socket_pages():
    """A quiet market and a dead socket look identical from the tick stream
    alone, so the absence of heartbeats is the only thing that separates them."""
    h = FeedHealth()
    h.on_tick(1_000_000)
    h.on_heartbeat(1_000_000)
    s = h.status(1_000_000 + 20_000)
    assert s["state"] == "BLIND"
    assert "page" in s["action"]


def test_data_gap_and_heartbeat_loss_are_counted_separately():
    h = FeedHealth()
    h.on_tick(1_000_000)
    h.on_tick(1_100_000)             # 100s gap in data
    h.on_heartbeat(1_000_000)
    h.on_heartbeat(1_100_000)        # and heartbeats also went missing
    assert h.status(1_100_000)["data_gaps"] == 1
    assert h.status(1_100_000)["heartbeat_losses"] == 1
