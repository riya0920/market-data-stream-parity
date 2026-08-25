"""Alert rules over the feed's health, and the ones it must not fire."""
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.alerting import (AlertConfig, evaluate, in_session, render_prometheus)
from src.ops import FeedHealth, IngestLag


def _ms(y=2026, mo=8, d=25, h=15, mi=0):
    """A wall clock in UTC. 15:00 UTC is inside the US equities session."""
    return int(datetime(y, mo, d, h, mi, tzinfo=timezone.utc).timestamp() * 1000)


IN_SESSION = _ms(h=15)
OUT_OF_SESSION = _ms(h=3)


def _lag(*samples):
    lag = IngestLag()
    for s in samples:
        lag.observe(0, s)
    return lag.report()


# ------------------------------------------- the decision that matters most
def test_tick_silence_alone_never_pages():
    """A quiet market looks exactly like a dead socket. No ticks arriving is
    the normal state of an illiquid instrument at 03:00 and the worst possible
    state of SPY at 10:00 -- so the rules alert on HEARTBEAT loss, never on
    tick silence alone."""
    h = FeedHealth()
    h.on_tick(IN_SESSION)
    h.on_heartbeat(IN_SESSION)
    # Hours of silence, but the heartbeat is current.
    alerts = evaluate(h, _lag(10), IN_SESSION + 500)
    assert not [a for a in alerts if a.severity == "page"]


def test_a_lost_heartbeat_pages():
    h = FeedHealth()
    h.on_heartbeat(IN_SESSION)
    alerts = evaluate(h, _lag(10), IN_SESSION + 60_000)
    pages = [a for a in alerts if a.severity == "page"]
    assert pages and pages[0].rule == "heartbeat_lost"
    assert "socket is dead" in pages[0].action


def test_a_current_heartbeat_does_not_page():
    h = FeedHealth()
    h.on_heartbeat(IN_SESSION)
    assert not [a for a in evaluate(h, _lag(10), IN_SESSION + 1_000)
                if a.rule == "heartbeat_lost"]


# --------------------------------------------------------------- the session
def test_the_session_boundaries_are_right():
    assert in_session(_ms(h=15)) is True             # 15:00 UTC, mid-session
    assert in_session(_ms(h=3)) is False             # 03:00 UTC, closed
    assert in_session(_ms(h=13, mi=29)) is False     # a minute before open
    assert in_session(_ms(h=13, mi=30)) is True      # the open


def test_weekends_are_out_of_session():
    saturday = _ms(y=2026, mo=8, d=29, h=15)
    assert datetime.fromtimestamp(saturday / 1000, tz=timezone.utc).weekday() == 5
    assert in_session(saturday) is False


def test_a_gap_outside_a_session_is_recorded_not_paged():
    """A gap rule that does not know about sessions fires every evening, and a
    rule that fires every evening is one nobody reads."""
    h = FeedHealth(data_gap_threshold_ms=1_000)
    h.on_tick(OUT_OF_SESSION)
    h.on_tick(OUT_OF_SESSION + 120_000)
    alerts = evaluate(h, _lag(10), OUT_OF_SESSION + 120_000)
    gap = [a for a in alerts if a.rule.startswith("data_gap")
           or a.rule == "no_data_in_session"]
    assert gap and gap[0].severity == "info"
    assert gap[0].owner == "none"


def test_a_gap_inside_a_session_pages_the_venue_not_the_feed_team():
    """The feed is alive and the venue stopped sending. That is the VENUE's
    problem and a different call from a dead socket -- routing it to the feed
    team wastes the one person who cannot fix it."""
    h = FeedHealth(data_gap_threshold_ms=1_000)
    h.on_tick(IN_SESSION)
    h.on_tick(IN_SESSION + 120_000)
    alerts = evaluate(h, _lag(10), IN_SESSION + 120_000)
    pages = [a for a in alerts if a.rule == "no_data_in_session"]
    assert pages and pages[0].severity == "page"
    assert pages[0].owner == "venue liaison"


def test_session_awareness_can_be_switched_off_and_then_it_pages():
    """Pinned so the session logic is doing the work rather than the
    threshold."""
    h = FeedHealth(data_gap_threshold_ms=1_000)
    h.on_tick(OUT_OF_SESSION)
    h.on_tick(OUT_OF_SESSION + 120_000)
    cfg = AlertConfig(session_aware=False)
    alerts = evaluate(h, _lag(10), OUT_OF_SESSION + 120_000, cfg)
    assert [a for a in alerts if a.rule == "no_data_in_session"]


# ------------------------------------------------------------------- lag
def test_lag_beyond_the_watermark_bound_pages_because_the_data_is_wrong():
    """Bars are built on event time with a 5s watermark bound, so lag beyond it
    means late ticks are dropped from their own bar. That is a correctness
    failure, not a latency one."""
    h = FeedHealth()
    h.on_heartbeat(IN_SESSION)
    alerts = evaluate(h, _lag(*([8_000] * 100)), IN_SESSION + 1_000)
    pages = [a for a in alerts if a.rule == "ingest_lag_high"]
    assert pages and pages[0].severity == "page"
    assert "DATA is wrong" in pages[0].action


def test_moderate_lag_warns_rather_than_pages():
    h = FeedHealth()
    h.on_heartbeat(IN_SESSION)
    alerts = evaluate(h, _lag(*([3_000] * 100)), IN_SESSION + 1_000)
    assert [a for a in alerts if a.rule == "ingest_lag_elevated"
            and a.severity == "warn"]
    assert not [a for a in alerts if a.rule == "ingest_lag_high"]


def test_low_lag_is_silent():
    h = FeedHealth()
    h.on_heartbeat(IN_SESSION)
    assert not [a for a in evaluate(h, _lag(*([50] * 100)), IN_SESSION + 1_000)
                if a.rule.startswith("ingest_lag")]


def test_no_samples_does_not_alert_on_a_lag_of_zero():
    """An empty report has no p99, and treating a missing measurement as a
    healthy one is how a dead collector reads as a healthy feed."""
    h = FeedHealth()
    h.on_heartbeat(IN_SESSION)
    assert not [a for a in evaluate(h, {"n": 0}, IN_SESSION + 1_000)
                if a.rule.startswith("ingest_lag")]


# ------------------------------------------------------------ clock skew
def test_negative_lag_is_a_clock_problem_and_says_so():
    """`arrival - event_time` mixes our delay with the venue's clock. A
    negative value means their clock is ahead of ours -- alerting on it as a
    slow feed sends the wrong team."""
    lag = IngestLag()
    lag.observe(1_000, 900)          # arrived 100ms "before" it happened
    alerts = evaluate(FeedHealth(), lag.report(), IN_SESSION)
    skew = [a for a in alerts if a.rule == "clock_skew"]
    assert skew and skew[0].owner == "platform"
    assert "NTP" in skew[0].action
    assert skew[0].severity != "page"


# ------------------------------------------------------------- ordering
def test_alerts_come_back_worst_first():
    h = FeedHealth(data_gap_threshold_ms=1_000)
    h.on_heartbeat(IN_SESSION - 60_000)
    h.on_tick(IN_SESSION)
    h.on_tick(IN_SESSION + 120_000)
    lag = IngestLag()
    for _ in range(100):
        lag.observe(0, 3_000)
    lag.observe(1_000, 900)
    alerts = evaluate(h, lag.report(), IN_SESSION + 120_000)
    sev = [a.severity for a in alerts]
    assert sev == sorted(sev, key=lambda s: {"page": 0, "warn": 1, "info": 2}[s])


def test_every_alert_names_an_owner_and_an_action():
    """A rule whose purpose is not written down gets tuned until it stops
    firing, and an alert with no owner is one everybody assumes is somebody
    else's."""
    h = FeedHealth(data_gap_threshold_ms=1_000)
    h.on_heartbeat(IN_SESSION - 60_000)
    h.on_tick(IN_SESSION)
    h.on_tick(IN_SESSION + 120_000)
    for a in evaluate(h, _lag(*([8_000] * 50)), IN_SESSION + 120_000):
        assert a.action.strip() and a.owner.strip()


# ------------------------------------------------------------- exporting
def test_the_state_is_exportable_as_metrics():
    """An alert that exists only in a Python list needs THIS process to be the
    thing that notices. Exporting lets SE-3's Prometheus and Alertmanager do
    the paging rather than this project growing its own notifier."""
    h = FeedHealth(data_gap_threshold_ms=1_000)
    h.on_tick(IN_SESSION)
    h.on_tick(IN_SESSION + 120_000)
    alerts = evaluate(h, _lag(*([8_000] * 50)), IN_SESSION + 120_000)
    text = render_prometheus(alerts, h, _lag(*([8_000] * 50)))

    assert 'feed_alerts{severity="page"}' in text
    assert "feed_data_gaps_total 1" in text
    assert "# TYPE feed_ingest_lag_ms gauge" in text
    for line in text.strip().splitlines():
        assert line.startswith("#") or len(line.split()) == 2, line
