"""Per-key watermarks and the slow-partition problem.

A single global watermark across many symbols is the default mistake, and it
fails in both directions:

  ONE QUIET SYMBOL STALLS EVERYTHING. A global watermark is min(per-key), so an
  illiquid name that has not traded for ten minutes holds the watermark back and
  no bar finalises for ANY symbol. Consumers see the whole board go stale
  because one instrument is quiet.

  ONE BUSY SYMBOL FINALISES EVERYTHING. Take max(per-key) instead and the
  opposite happens: a heavily traded name drags the watermark forward and the
  quiet symbol's bars are finalised before its ticks arrive, so its late data is
  silently discarded.

The correct answer is per-key watermarks with an idle timeout: each symbol
advances on its own event time, and a symbol that goes idle beyond a bound has
its watermark advanced by wall clock so it does not stall its own pipeline
forever. Both parts are needed, and the idle timeout is the part people forget.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .bars import Bar, build_streaming
from .replay import Tick


@dataclass
class SymbolState:
    symbol: str
    watermark: int = 0
    max_event_seen: int = 0
    tick_count: int = 0
    idle_advances: int = 0


@dataclass
class MultiSymbolProcessor:
    """Routes ticks per symbol and tracks watermarks independently."""
    width_ms: int = 60_000
    bound_ms: int = 5_000
    idle_timeout_ms: int = 120_000
    states: dict[str, SymbolState] = field(default_factory=dict)
    buffers: dict[str, list[Tick]] = field(default_factory=dict)

    def ingest(self, symbol: str, tick: Tick, wall_clock_ms: int) -> None:
        st = self.states.setdefault(symbol, SymbolState(symbol))
        self.buffers.setdefault(symbol, []).append(tick)
        st.tick_count += 1
        st.max_event_seen = max(st.max_event_seen, tick.event_time_ms)
        st.watermark = max(st.watermark, st.max_event_seen - self.bound_ms)

        # Idle advance: a symbol that has not traded recently must not hold its
        # own bars open forever. Wall clock is used ONLY here, and only to
        # advance a stalled key -- never to place a tick in a bar.
        for other, other_st in self.states.items():
            if other == symbol:
                continue
            if wall_clock_ms - other_st.max_event_seen > self.idle_timeout_ms:
                new_wm = wall_clock_ms - self.idle_timeout_ms - self.bound_ms
                if new_wm > other_st.watermark:
                    other_st.watermark = new_wm
                    other_st.idle_advances += 1

    def global_watermark_min(self) -> int:
        """What a naive implementation would use. Kept so the stall is measurable."""
        return min((s.watermark for s in self.states.values()), default=0)

    def global_watermark_max(self) -> int:
        return max((s.watermark for s in self.states.values()), default=0)

    def build_all(self) -> dict[str, dict[int, Bar]]:
        out = {}
        for symbol, ticks in self.buffers.items():
            bars, _late, _stats = build_streaming(ticks, self.width_ms, self.bound_ms)
            out[symbol] = bars
        return out

    def stall_report(self) -> dict:
        """How far behind the slowest key holds a global watermark.

        This number is the argument for per-key watermarks, expressed in
        milliseconds of unnecessary staleness.
        """
        if not self.states:
            return {"stall_ms": 0, "slowest": None, "fastest": None}
        slowest = min(self.states.values(), key=lambda s: s.watermark)
        fastest = max(self.states.values(), key=lambda s: s.watermark)
        return {
            "stall_ms": fastest.watermark - slowest.watermark,
            "slowest": slowest.symbol,
            "fastest": fastest.symbol,
            "idle_advances": sum(s.idle_advances for s in self.states.values()),
            "per_symbol": {s.symbol: {"watermark": s.watermark, "ticks": s.tick_count,
                                      "idle_advances": s.idle_advances}
                           for s in self.states.values()},
        }
