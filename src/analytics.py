"""Derived analytics, computed in-stream and verifiable against batch.

Three quantities, chosen because each breaks differently under disorder:

  VWAP              a running ratio of two sums. Order-independent, so streaming
                    and batch agree trivially -- which makes it the control: if
                    VWAP disagrees, the tick populations differ, not the maths.
  realised vol      depends on the ORDER of returns, so a late tick inserted in
                    the middle changes it. This is the one that exposes a
                    pipeline that appends late data instead of re-inserting it.
  price jumps       a threshold on a rolling z-score. Sensitive to the warm-up
                    window, so streaming and batch disagree at the start of the
                    session unless the warm-up is handled identically.

Every one is defined here ONCE and called from both paths. Two implementations
of "the same" metric is how streaming/batch parity dies -- the code drifts, and
the parity check ends up measuring the difference between two authors rather
than the difference between two pipelines.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field

from .replay import Tick

JUMP_SIGMA = 4.0
VOL_WINDOW = 50


@dataclass
class Analytics:
    """Incremental state. `update` is called once per accepted tick, in event-time
    order; `snapshot` returns the derived values."""
    notional: int = 0
    volume: int = 0
    returns: list[float] = field(default_factory=list)
    last_price: int | None = None
    jumps: list[tuple[int, int, float]] = field(default_factory=list)

    def update(self, tick: Tick) -> None:
        self.notional += tick.price_minor * tick.size
        self.volume += tick.size

        if self.last_price is not None and self.last_price > 0:
            r = math.log(tick.price_minor / self.last_price)
            self.returns.append(r)
            if len(self.returns) > VOL_WINDOW:
                window = self.returns[-VOL_WINDOW:]
                mean = sum(window) / len(window)
                var = sum((x - mean) ** 2 for x in window) / (len(window) - 1)
                sd = math.sqrt(var)
                if sd > 0 and abs(r - mean) > JUMP_SIGMA * sd:
                    self.jumps.append((tick.seq, tick.event_time_ms,
                                       (r - mean) / sd))
        self.last_price = tick.price_minor

    def vwap_minor(self) -> float:
        return self.notional / self.volume if self.volume else 0.0

    def realised_vol(self) -> float:
        """Annualised-ish: sqrt of the sum of squared log returns. No calendar
        scaling applied, because the session length here is arbitrary and a
        scaled number would invite comparison against real market vol."""
        if len(self.returns) < 2:
            return 0.0
        return math.sqrt(sum(r * r for r in self.returns))

    def snapshot(self) -> dict:
        return {
            "vwap_minor": round(self.vwap_minor(), 6),
            "realised_vol": round(self.realised_vol(), 9),
            "n_returns": len(self.returns),
            "n_jumps": len(self.jumps),
            "volume": self.volume,
        }


def stream_with_reorder(arrivals: list[Tick], bound_ms: int) -> tuple[Analytics, int]:
    """Streaming path with a reorder buffer. Returns (analytics, dropped_late).

    Why this exists: an order-dependent metric CANNOT be computed by folding
    arrivals in arrival order. Realised volatility is a function of the sequence
    of returns, so appending a tick that belongs 3 seconds earlier fabricates two
    spurious returns -- one jumping backwards to it and one jumping forward
    again. Measured on this data, the naive version produced 812 jump flags
    against batch's 2, and a realised vol 59% too high. That is not a tolerance
    to document; it is a wrong number.

    A real stream processor does what this does: hold ticks in a buffer keyed by
    event time and release them, in event-time order, only once the watermark has
    passed them. Ticks arriving beyond the bound are already released and are
    counted as dropped rather than folded in out of order.

    The cost is exactly the watermark bound in added latency, which is the price
    of correctness for any order-dependent metric and is why the bound is a
    published design parameter rather than an implementation detail.
    """
    buffer: list[Tick] = []
    seen: set[tuple[int, int]] = set()
    a = Analytics()
    watermark = 0
    max_event = 0
    dropped_late = 0

    for t in arrivals:
        ident = (t.seq, t.event_time_ms)
        if ident in seen:
            continue
        seen.add(ident)

        if t.event_time_ms <= watermark:
            dropped_late += 1        # already released; folding it in would reorder
            continue

        buffer.append(t)
        max_event = max(max_event, t.event_time_ms)
        watermark = max_event - bound_ms

        # Release everything the watermark has passed, in event-time order.
        buffer.sort(key=lambda x: (x.event_time_ms, x.seq))
        cut = 0
        for i, b in enumerate(buffer):
            if b.event_time_ms <= watermark:
                a.update(b)
                cut = i + 1
            else:
                break
        if cut:
            del buffer[:cut]

    for b in sorted(buffer, key=lambda x: (x.event_time_ms, x.seq)):
        a.update(b)
    return a, dropped_late


def compute_batch(ticks: list[Tick]) -> dict:
    """Batch path: sort by event time, fold through the SAME Analytics class.

    Sharing the class is the point. A separate batch implementation would be a
    second opinion about the metric definition, and the parity check would then
    be measuring authorship rather than pipeline behaviour.
    """
    a = Analytics()
    for t in sorted(ticks, key=lambda x: (x.event_time_ms, x.seq)):
        a.update(t)
    return a.snapshot()


def compare(stream: dict, batch: dict, tolerance_bps: float = 1.0) -> dict:
    """Compare snapshots. VWAP is compared in bps because it is a price; counts
    are compared exactly, because an off-by-one in a count is never rounding."""
    out = {}
    vs, vb = stream["vwap_minor"], batch["vwap_minor"]
    vwap_bps = abs(vs - vb) / vb * 10_000 if vb else 0.0
    out["vwap_diff_bps"] = vwap_bps
    out["vwap_within_tolerance"] = vwap_bps <= tolerance_bps

    vol_s, vol_b = stream["realised_vol"], batch["realised_vol"]
    out["vol_rel_diff"] = abs(vol_s - vol_b) / vol_b if vol_b else 0.0

    for key in ("n_returns", "n_jumps", "volume"):
        out[key + "_match"] = stream[key] == batch[key]
        out[key + "_stream"] = stream[key]
        out[key + "_batch"] = batch[key]
    out["exact"] = (out["vwap_within_tolerance"]
                    and all(out[k + "_match"] for k in ("n_returns", "n_jumps", "volume")))
    return out
