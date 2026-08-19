"""Derived analytics parity, per-key watermarks, and bar-emit latency.

Adds the three things run_parity.py did not measure: whether the in-stream
analytics agree with batch, what a single global watermark costs across symbols,
and how long a bar actually takes to emit.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from src.analytics import (Analytics, compare, compute_batch,
                           stream_with_reorder)
from src.bars import build_streaming
from src.multisymbol import MultiSymbolProcessor
from src.replay import DisorderConfig, generate_session, replay

WIDTH_MS = 60_000


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    return s[min(int(round(p / 100 * (len(s) - 1))), len(s) - 1)]


def main() -> int:
    cfg = DisorderConfig()
    truth = generate_session()
    arrivals, _gaps, surviving = replay(truth, cfg)

    # ---- 1. analytics parity ----------------------------------------------
    print("=" * 78)
    print("1. IN-STREAM ANALYTICS vs BATCH")
    print("-" * 78)
    naive = Analytics()
    seen = set()
    for t in arrivals:
        ident = (t.seq, t.event_time_ms)
        if ident in seen:
            continue          # same duplicate suppression as the bar path
        seen.add(ident)
        naive.update(t)
    naive_snap = naive.snapshot()

    stream_obj, dropped_late = stream_with_reorder(arrivals, cfg.watermark_bound_ms)
    stream_snap = stream_obj.snapshot()
    batch_snap = compute_batch(surviving)
    cmp = compare(stream_snap, batch_snap)

    print("{:<18}{:>16}{:>18}{:>16}".format(
        "metric", "naive append", "streaming (reorder)", "batch"))
    print("{:<18}{:>16.4f}{:>18.4f}{:>16.4f}".format(
        "VWAP (minor)", naive_snap["vwap_minor"], stream_snap["vwap_minor"],
        batch_snap["vwap_minor"]))
    print("{:<18}{:>16.6f}{:>18.6f}{:>16.6f}".format(
        "realised vol", naive_snap["realised_vol"], stream_snap["realised_vol"],
        batch_snap["realised_vol"]))
    print("{:<18}{:>16,}{:>18,}{:>16,}".format(
        "volume", naive_snap["volume"], stream_snap["volume"], batch_snap["volume"]))
    print("{:<18}{:>16,}{:>18,}{:>16,}".format(
        "return count", naive_snap["n_returns"], stream_snap["n_returns"],
        batch_snap["n_returns"]))
    print("{:<18}{:>16,}{:>18,}{:>16,}".format(
        "jump flags", naive_snap["n_jumps"], stream_snap["n_jumps"],
        batch_snap["n_jumps"]))
    print("-" * 78)
    print("VWAP difference : {:.4f} bps  ({})".format(
        cmp["vwap_diff_bps"],
        "within 1bp tolerance" if cmp["vwap_within_tolerance"] else "OUT OF TOLERANCE"))
    print("realised vol    : {:.4%} relative difference".format(cmp["vol_rel_diff"]))

    print("late ticks dropped beyond the {}ms bound: {:,}  (this is why the volume"
          .format(cfg.watermark_bound_ms, dropped_late))
    print("column differs from batch -- batch sees every tick at once)")

    print("\nThe 'naive append' column is the bug, kept and measured rather than")
    print("deleted. VWAP ties everywhere: it is a ratio of two sums and does not")
    print("care about order. Realised vol and jump flags do NOT tie under naive")
    print("appending, because both are functions of the SEQUENCE of returns -- a")
    print("tick folded in seconds out of place fabricates two spurious returns,")
    print("one jumping back to it and one jumping forward again.")
    print("\nBlunt version of what that produced: {} jump flags against batch's {},"
          .format(naive_snap["n_jumps"], batch_snap["n_jumps"]))
    print("and realised vol {:.0%} too high. An earlier draft of this file called"
          .format(naive_snap["realised_vol"] / batch_snap["realised_vol"] - 1))
    print("that 'expected streaming/batch divergence' and moved on. It is not a")
    print("tolerance -- it is a wrong number with a comfortable label on it.")
    print("\nThe fix is a reorder buffer: hold ticks, release them in event-time")
    print("order once the watermark passes. Cost is exactly the watermark bound in")
    print("added latency, which is why that bound is a published parameter and not")
    print("an implementation detail.")
    print("\n'Investigate or tolerate?' -- the {:.4f}bp VWAP gap is tolerated because"
          .format(cmp["vwap_diff_bps"]))
    print("its cause is known and bounded (dropped beyond-bound ticks). An")
    print("unexplained gap of the same size would not be.")

    # ---- 2. per-key watermarks --------------------------------------------
    print("\n" + "=" * 78)
    print("2. PER-KEY WATERMARKS: what one quiet symbol costs")
    print("-" * 78)
    proc = MultiSymbolProcessor()
    base = truth[0].event_time_ms
    liquid = [t for t in surviving if t.seq % 3 != 0]
    # An illiquid symbol that stops trading a third of the way in.
    cutoff = base + (surviving[-1].event_time_ms - base) // 3
    illiquid = [t for t in surviving if t.seq % 3 == 0 and t.event_time_ms <= cutoff]

    # Interleave by event time. Feeding one symbol's whole history and then the
    # other's would never exercise the idle timeout, because the quiet symbol
    # would still be "current" at the moment its ticks are fed.
    merged = sorted([("LIQUID", t) for t in liquid] + [("ILLIQUID", t) for t in illiquid],
                    key=lambda p: p[1].event_time_ms)
    wall = 0
    for symbol, t in merged:
        wall = max(wall, t.event_time_ms)
        proc.ingest(symbol, t, wall)

    rep = proc.stall_report()
    print("{:<12}{:>12}{:>18}{:>16}".format(
        "symbol", "ticks", "watermark", "idle advances"))
    for sym, d in rep["per_symbol"].items():
        print("{:<12}{:>12,}{:>18,}{:>16}".format(
            sym, d["ticks"], d["watermark"], d["idle_advances"]))
    print("-" * 78)
    print("global watermark if min() across keys : {:,}".format(
        proc.global_watermark_min()))
    print("global watermark if max() across keys : {:,}".format(
        proc.global_watermark_max()))
    print("staleness a single global watermark would impose: {:,} ms ({:.1f} min)"
          .format(rep["stall_ms"], rep["stall_ms"] / 60_000))
    print("idle advances applied to the quiet symbol: {:,}".format(rep["idle_advances"]))
    print("\nWith a single min() watermark the quiet symbol holds the whole board")
    print("back and NO bar finalises for ANY instrument. With max(), the busy")
    print("symbol drags the watermark forward and the quiet symbol's late ticks are")
    print("discarded before they even arrive. Both are wrong; they are wrong in")
    print("opposite directions, which is why picking one and moving on feels safe.")
    print("\nWhat the numbers above show is the idle timeout doing its job: the")
    print("quiet symbol's watermark is held exactly {:,}ms behind the fast one --"
          .format(rep["stall_ms"]))
    print("the configured idle timeout -- instead of the {:.0f} minutes it would"
          .format((truth[-1].event_time_ms - cutoff) / 60_000))
    print("otherwise drift. Without the idle advance a symbol that stops trading")
    print("never finalises another bar for the rest of the session.")
    print("\nThe tradeoff is explicit: the idle timeout trades correctness for")
    print("liveness. Any tick arriving from that symbol more than {:,}ms late is"
          .format(proc.idle_timeout_ms))
    print("now beyond its watermark and will be dropped. That is a real cost and")
    print("the right owner of the number is whoever consumes the quiet symbol.")

    # ---- 3. bar-emit latency ----------------------------------------------
    print("\n" + "=" * 78)
    print("3. BAR-EMIT LATENCY")
    print("-" * 78)
    latencies = []
    t0 = time.perf_counter()
    chunk, emitted = [], set()
    for t in arrivals:
        chunk.append(t)
        if len(chunk) >= 500:
            start = time.perf_counter()
            bars, _l, _s = build_streaming(chunk, WIDTH_MS, cfg.watermark_bound_ms)
            for k, b in bars.items():
                if b.is_final and k not in emitted:
                    emitted.add(k)
                    latencies.append((time.perf_counter() - start) * 1000)
            chunk = []
    total_s = time.perf_counter() - t0

    print("bars finalised          : {:,}".format(len(latencies)))
    print("emit latency p50 / p95  : {:.3f}ms / {:.3f}ms".format(
        percentile(latencies, 50), percentile(latencies, 95)))
    print("throughput              : {:,.0f} ticks/sec".format(len(arrivals) / total_s))
    print("\nThis is in-process latency on a laptop: no broker, no network hop, no")
    print("serialisation, no checkpointing. It measures the bar-construction code")
    print("and nothing else, which is the only thing it is offered as.")
    print("=" * 78)
    return 0 if cmp["vwap_within_tolerance"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
