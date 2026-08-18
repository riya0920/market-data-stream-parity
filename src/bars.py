"""Event-time OHLCV bar construction with watermarks, emit-and-revise, and a
batch recomputation to check it against.

Event time vs processing time, and the failure story for the wrong choice:
processing-time windows put a tick into whatever bar happened to be open when the
packet arrived. A 3-second GC pause therefore moves trades between minutes. Every
bar is then a function of your infrastructure's mood, two replays of the same
session disagree, and the parity check below is impossible to pass by
construction. Event time makes bars a property of the market instead.

Emit-and-revise: a bar is emitted PROVISIONAL as soon as its window closes, and
finalised once the watermark passes `window_end + bound`. Late ticks inside the
bound revise the bar and bump its revision number. Consumers can tell the two
apart -- a bar carries `is_final`, and acting on a provisional bar is then the
consumer's informed choice rather than an accident.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .replay import Tick


@dataclass
class Bar:
    bucket_start_ms: int
    open_minor: int
    high_minor: int
    low_minor: int
    close_minor: int
    volume: int
    tick_count: int
    first_seq: int
    last_seq: int
    revision: int = 0
    is_final: bool = False
    suspect: bool = False       # quality flag ON the data, not beside it
    suspect_reason: str = ""

    def key(self):
        """Comparable payload for the parity check. Revision and finality are
        excluded on purpose: batch has no notion of either, so including them
        would make parity fail for a reason that is not a data difference."""
        return (self.bucket_start_ms, self.open_minor, self.high_minor,
                self.low_minor, self.close_minor, self.volume, self.tick_count)


def _bucket(ts_ms: int, width_ms: int) -> int:
    return (ts_ms // width_ms) * width_ms


def _apply(bar: Bar | None, t: Tick, bucket: int) -> Bar:
    if bar is None:
        return Bar(bucket, t.price_minor, t.price_minor, t.price_minor,
                   t.price_minor, t.size, 1, t.seq, t.seq)
    # Open and close are decided by EVENT-TIME order (approximated by seq, which
    # is the exchange's own sequencing), not by arrival order. A late tick that
    # belongs at the start of the bar must not overwrite the open.
    if t.seq < bar.first_seq:
        bar.open_minor = t.price_minor
        bar.first_seq = t.seq
    if t.seq > bar.last_seq:
        bar.close_minor = t.price_minor
        bar.last_seq = t.seq
    bar.high_minor = max(bar.high_minor, t.price_minor)
    bar.low_minor = min(bar.low_minor, t.price_minor)
    bar.volume += t.size
    bar.tick_count += 1
    return bar


@dataclass
class StreamStats:
    duplicates_suppressed: int = 0
    late_within_bound: int = 0
    late_beyond_bound: int = 0
    revisions_emitted: int = 0
    gaps_detected: list[tuple[int, int]] = field(default_factory=list)


def build_streaming(arrivals: list[Tick], width_ms: int = 60_000,
                    watermark_bound_ms: int = 5_000,
                    gap_threshold_ms: int = 30_000):
    """Single-pass, arrival-ordered, as a stream processor would see it."""
    bars: dict[int, Bar] = {}
    late_events: list[Tick] = []
    seen: set[tuple[int, int]] = set()
    stats = StreamStats()
    watermark = 0
    max_event_seen = None

    for t in arrivals:
        # -- duplicate suppression (exchange seq + event time is the identity)
        ident = (t.seq, t.event_time_ms)
        if ident in seen:
            stats.duplicates_suppressed += 1
            continue
        seen.add(ident)

        # -- gap detection on event time
        if (max_event_seen is not None
                and t.event_time_ms - max_event_seen > gap_threshold_ms):
            stats.gaps_detected.append((max_event_seen, t.event_time_ms))

        bucket = _bucket(t.event_time_ms, width_ms)
        bucket_end = bucket + width_ms

        # Lateness is measured against the STREAM's clock (the highest event time
        # seen so far), not against the tick's own timestamp -- a tick is never
        # late relative to itself. This is the distinction that makes the
        # revision count mean something: without it, every ordinary in-window
        # tick looks like a revision and the metric is noise.
        is_late = max_event_seen is not None and bucket_end <= max_event_seen

        max_event_seen = max(max_event_seen or 0, t.event_time_ms)
        watermark = max(watermark, max_event_seen - watermark_bound_ms)

        # Finalise everything the watermark has passed, BEFORE placing this tick.
        for b in bars.values():
            if not b.is_final and b.bucket_start_ms + width_ms <= watermark:
                b.is_final = True

        existing = bars.get(bucket)
        if existing is not None and existing.is_final:
            # Beyond the bound: the bar is closed and consumers have acted on it.
            # The tick goes to the late-events table and the bar is marked
            # suspect. It is NOT silently dropped and it does NOT retroactively
            # change a number someone has already traded on.
            stats.late_beyond_bound += 1
            late_events.append(t)
            existing.suspect = True
            existing.suspect_reason = "late tick arrived after finalisation"
            continue

        bar = _apply(existing, t, bucket)
        if is_late and existing is not None:
            stats.late_within_bound += 1
            bar.revision += 1
            stats.revisions_emitted += 1
        bars[bucket] = bar

    for b in bars.values():
        b.is_final = True

    # Mark bars adjacent to a detected gap as suspect: consumers must be able to
    # SEE that a bar is built on a hole, not just find an alert in a dashboard.
    for gap_start, gap_end in stats.gaps_detected:
        for b in bars.values():
            if gap_start - width_ms <= b.bucket_start_ms <= gap_end + width_ms:
                b.suspect = True
                b.suspect_reason = b.suspect_reason or "adjacent to feed gap"

    return dict(sorted(bars.items())), late_events, stats


def build_batch(ticks: list[Tick], width_ms: int = 60_000) -> dict[int, Bar]:
    """The recomputation from the archive. Deliberately naive and order-free:
    it sorts by event time and folds. If the streaming path is right, these
    agree exactly."""
    bars: dict[int, Bar] = {}
    for t in sorted(ticks, key=lambda x: (x.event_time_ms, x.seq)):
        bucket = _bucket(t.event_time_ms, width_ms)
        bars[bucket] = _apply(bars.get(bucket), t, bucket)
    for b in bars.values():
        b.is_final = True
    return dict(sorted(bars.items()))


def parity(stream_bars: dict[int, Bar], batch_bars: dict[int, Bar]) -> dict:
    """The centrepiece. Streaming must equal batch, or the exceptions must be
    bounded and named."""
    mismatches = []
    only_stream = sorted(set(stream_bars) - set(batch_bars))
    only_batch = sorted(set(batch_bars) - set(stream_bars))
    for k in sorted(set(stream_bars) & set(batch_bars)):
        if stream_bars[k].key() != batch_bars[k].key():
            mismatches.append((k, stream_bars[k], batch_bars[k]))
    total = max(len(batch_bars), 1)
    return {
        "bars_batch": len(batch_bars),
        "bars_stream": len(stream_bars),
        "only_in_stream": only_stream,
        "only_in_batch": only_batch,
        "mismatched": mismatches,
        "mismatches_per_million": 1_000_000 * len(mismatches) / total,
        "exact": not mismatches and not only_stream and not only_batch,
    }
