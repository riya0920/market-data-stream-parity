"""Alert rules over the health signals, and the ones a market feed must not fire.

`src/ops.py` measures ingest lag, data gaps and heartbeat loss. `README.md`
listed alerting as not built: "Gap detection, heartbeat loss and ingest lag are
all measured [and nothing alerts on them]". A measurement nobody is woken by is
a number in a report somebody reads afterwards to explain the incident.

WHY A MARKET FEED IS THE HARD CASE FOR ALERTING, and every rule here is shaped
by it:

  A QUIET MARKET LOOKS EXACTLY LIKE A DEAD SOCKET. No ticks arriving is the
  normal state of an illiquid instrument at 03:00 and the worst possible state
  of SPY at 10:00. `FeedHealth` already separates them by requiring a HEARTBEAT
  -- the rules below alert on heartbeat loss, never on tick silence alone, and
  that is the single most important decision in the file.

  THE MARKET CLOSES. A gap rule that does not know about sessions fires every
  evening, and a rule that fires every evening is one nobody reads. Session
  awareness is not a nicety; it is what makes the rule survivable.

  LAG IS NOT LATENCY. `arrival - event_time` mixes our own delay with the
  venue's clock. A NEGATIVE lag means the venue's clock is ahead of ours, which
  is a clock problem and not a feed problem, and alerting on it as though the
  feed were slow sends the wrong team.

WHAT EACH RULE PROTECTS, stated because a rule whose purpose is not written
down gets tuned until it stops firing:

  heartbeat_lost      the socket is dead. THE one that must always page.
  ingest_lag_high     we are behind; bars are being built from stale ticks.
  data_gap            ticks stopped while the feed said it was alive -- which
                      is the venue's problem, not ours, and a different call.
  clock_skew          negative lag. Not the feed's fault; do not page the feed
                      team.
  no_data_in_session  ticks stopped AND we are inside a trading session.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, time, timezone

# Thresholds, declared here rather than discovered in a dashboard config. Each
# is the number an on-call engineer is woken by, so each carries its reasoning.

# A heartbeat is the feed telling us it is alive. Missing two consecutive ones
# is a dead socket; missing one is a lost packet.
HEARTBEAT_LOST_MS = 20_000

# p99 ingest lag. Bars are built on event time with a watermark bound of 5s, so
# lag beyond that means late ticks are being dropped from their own bar -- the
# data is wrong, not merely slow.
LAG_P99_PAGE_MS = 5_000
LAG_P99_WARN_MS = 2_000

# A gap inside a session, with the feed alive. Below this, thin trading.
SESSION_GAP_MS = 60_000

# US equities regular session, UTC. Declared as a constant rather than computed
# from a calendar because this repo has no holiday calendar, and a session rule
# that silently ignores holidays is better than one that pretends to know them.
SESSION_OPEN_UTC = time(13, 30)
SESSION_CLOSE_UTC = time(20, 0)


@dataclass
class Alert:
    severity: str          # page | warn | info
    rule: str
    detail: str
    action: str
    owner: str             # who this is actually for


@dataclass
class AlertConfig:
    heartbeat_lost_ms: int = HEARTBEAT_LOST_MS
    lag_p99_page_ms: int = LAG_P99_PAGE_MS
    lag_p99_warn_ms: int = LAG_P99_WARN_MS
    session_gap_ms: int = SESSION_GAP_MS
    session_aware: bool = True


def in_session(wall_ms: int) -> bool:
    """Is this instant inside the regular US equities session?

    Weekends excluded. Holidays are NOT, and that is stated rather than faked:
    a session rule that claims to know the holiday calendar and does not is
    worse than one that admits it, because the first is trusted.
    """
    dt = datetime.fromtimestamp(wall_ms / 1000, tz=timezone.utc)
    if dt.weekday() >= 5:
        return False
    return SESSION_OPEN_UTC <= dt.timetz().replace(tzinfo=None) <= SESSION_CLOSE_UTC


def evaluate(health, lag_report: dict, now_ms: int,
             cfg: AlertConfig | None = None) -> list:
    """Turn the measurements into alerts. Returns worst-first."""
    cfg = cfg or AlertConfig()
    out = []

    # ---- heartbeat: the socket itself ---------------------------------
    hb = getattr(health, "last_heartbeat_ms", None)
    if hb is not None:
        silent = now_ms - hb
        if silent > cfg.heartbeat_lost_ms:
            out.append(Alert(
                "page", "heartbeat_lost",
                "no heartbeat for {:,}ms (threshold {:,}ms)".format(
                    silent, cfg.heartbeat_lost_ms),
                "the socket is dead -- reconnect and replay from the last "
                "committed offset",
                "feed team"))

    # ---- ingest lag: are we behind? ------------------------------------
    p99 = lag_report.get("p99_ms")
    if p99 is not None and lag_report.get("n"):
        if p99 > cfg.lag_p99_page_ms:
            out.append(Alert(
                "page", "ingest_lag_high",
                "p99 ingest lag {:,}ms over a {:,}ms watermark bound".format(
                    int(p99), cfg.lag_p99_page_ms),
                "late ticks are being dropped from their own bar -- the DATA "
                "is wrong, not merely slow",
                "feed team"))
        elif p99 > cfg.lag_p99_warn_ms:
            out.append(Alert(
                "warn", "ingest_lag_elevated",
                "p99 ingest lag {:,}ms".format(int(p99)),
                "watch; not yet dropping ticks", "feed team"))

    # ---- negative lag: a CLOCK problem, not a feed problem --------------
    negative = lag_report.get("negative", 0)
    if negative:
        out.append(Alert(
            "warn", "clock_skew",
            "{:,} tick(s) arrived BEFORE their event time".format(negative),
            "the venue's clock is ahead of ours, or ours is behind. Do not "
            "page the feed team for this -- it is an NTP question",
            "platform"))

    # ---- data gaps: ticks stopped while the feed said it was alive ------
    gaps = list(getattr(health, "data_gaps", []) or [])
    if gaps:
        worst = max(b - a for a, b in gaps)
        inside = [g for g in gaps
                  if not cfg.session_aware or in_session(g[0])]
        if inside and worst > cfg.session_gap_ms:
            out.append(Alert(
                "page", "no_data_in_session",
                "{} gap(s) inside a session, worst {:,}ms".format(
                    len(inside), worst),
                "the feed is alive and the venue stopped sending -- this is "
                "the VENUE's problem and a different call from a dead socket",
                "venue liaison"))
        elif gaps:
            out.append(Alert(
                "info", "data_gap_outside_session",
                "{} gap(s), worst {:,}ms, none inside a session".format(
                    len(gaps), worst),
                "expected outside trading hours -- recorded, not actioned",
                "none"))

    order = {"page": 0, "warn": 1, "info": 2}
    return sorted(out, key=lambda a: order.get(a.severity, 9))


def render_prometheus(alerts: list, health, lag_report: dict) -> str:
    """The same state as scrapeable metrics.

    An alert that exists only in a Python list requires this process to be the
    thing that notices. Exporting it lets the alerting that already exists in
    SE-3 -- Prometheus rules, Alertmanager routing -- do the paging, rather
    than this project growing its own notifier.
    """
    L = [
        "# HELP feed_alerts Active alerts by severity.",
        "# TYPE feed_alerts gauge",
    ]
    for sev in ("page", "warn", "info"):
        L.append('feed_alerts{{severity="{}"}} {}'.format(
            sev, sum(1 for a in alerts if a.severity == sev)))
    L += [
        "# HELP feed_ingest_lag_ms Ingest lag percentiles.",
        "# TYPE feed_ingest_lag_ms gauge",
    ]
    for q in ("p50_ms", "p95_ms", "p99_ms"):
        if q in lag_report:
            L.append('feed_ingest_lag_ms{{quantile="{}"}} {}'.format(
                q.split("_")[0], lag_report[q]))
    L += [
        "# HELP feed_data_gaps_total Data gaps observed.",
        "# TYPE feed_data_gaps_total counter",
        "feed_data_gaps_total {}".format(len(getattr(health, "data_gaps", []) or [])),
        "# HELP feed_heartbeat_losses_total Heartbeat losses observed.",
        "# TYPE feed_heartbeat_losses_total counter",
        "feed_heartbeat_losses_total {}".format(
            len(getattr(health, "heartbeat_losses", []) or [])),
    ]
    return "\n".join(L) + "\n"
