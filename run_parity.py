"""Streaming vs batch parity, plus the rebuild drill.

Two parity runs, and the difference between them is the whole lesson:

  A) streaming (disordered arrivals) vs batch over the SAME surviving ticks
     -- this must be EXACT. Any mismatch is a bug in the streaming path.
  B) streaming vs batch over the ORIGINAL ticks (including the ones the feed
     gap swallowed) -- this must NOT be exact, and the gap-detection flags are
     what tell a consumer which bars to distrust. A pipeline that reported
     "parity: 100%" here would be lying by choosing a convenient baseline.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from src.bars import build_batch, build_streaming, parity
from src.replay import DisorderConfig, generate_session, replay

WIDTH_MS = 60_000


def main() -> int:
    cfg = DisorderConfig()
    truth = generate_session()
    arrivals, gap_starts, surviving = replay(truth, cfg)

    print("=" * 76)
    print("REPLAY")
    print("-" * 76)
    print("recorded ticks            : {:,}".format(len(truth)))
    print("survived the feed gaps    : {:,}  ({} gaps of {}s)".format(
        len(surviving), cfg.gap_count, cfg.gap_duration_ms // 1000))
    print("delivered to the pipeline : {:,}  (includes duplicates)".format(len(arrivals)))
    print("injected disorder         : {:.0%} out-of-order (<= {}ms), {:.0%} duplicates"
          .format(cfg.out_of_order_rate, cfg.max_delay_ms, cfg.duplicate_rate))
    print("watermark bound           : {}ms".format(cfg.watermark_bound_ms))

    t0 = time.perf_counter()
    stream_bars, late, stats = build_streaming(
        arrivals, WIDTH_MS, cfg.watermark_bound_ms)
    stream_secs = time.perf_counter() - t0

    print("\n" + "=" * 76)
    print("STREAMING")
    print("-" * 76)
    print("bars emitted              : {:,}".format(len(stream_bars)))
    print("duplicates suppressed     : {:,}".format(stats.duplicates_suppressed))
    print("late, within bound        : {:,}  -> bar revised".format(stats.late_within_bound))
    print("late, beyond bound        : {:,}  -> late-events table, bar marked suspect"
          .format(stats.late_beyond_bound))
    print("bar revisions emitted     : {:,}".format(stats.revisions_emitted))
    print("gaps detected             : {}".format(len(stats.gaps_detected)))
    print("bars flagged suspect      : {:,}".format(
        sum(1 for b in stream_bars.values() if b.suspect)))
    print("throughput                : {:,.0f} ticks/sec (single-threaded CPython,"
          " Windows 11 laptop)".format(len(arrivals) / stream_secs))

    # ---- A) parity against the same tick population -----------------------
    batch_surviving = build_batch(surviving, WIDTH_MS)
    a = parity(stream_bars, batch_surviving)
    print("\n" + "=" * 76)
    print("PARITY A: streaming vs batch, SAME ticks (must be exact)")
    print("-" * 76)
    print("bars batch / stream       : {:,} / {:,}".format(a["bars_batch"], a["bars_stream"]))
    print("mismatched bars           : {}".format(len(a["mismatched"])))
    print("bars only in one side     : {} / {}".format(
        len(a["only_in_stream"]), len(a["only_in_batch"])))
    print("mismatches per million    : {:.1f}".format(a["mismatches_per_million"]))
    # Documented, bounded exception list. Beyond-bound late ticks are excluded
    # from their bars ON PURPOSE -- revising a finalised bar would silently
    # change a number consumers already acted on -- so batch, which sees every
    # tick at once, legitimately disagrees on exactly those bars. The claim is
    # not "no mismatches"; it is "every mismatch is one of these, and every one
    # of them is flagged suspect on the data".
    explained, unexplained = [], []
    for k, s_bar, b_bar in a["mismatched"]:
        (explained if s_bar.suspect and "late tick" in s_bar.suspect_reason
         else unexplained).append(k)
    print("mismatched, explained by beyond-bound late ticks : {}".format(len(explained)))
    print("mismatched, UNEXPLAINED                          : {}".format(len(unexplained)))
    print("VERDICT                   : {}".format(
        "EXACT" if a["exact"] else
        ("EXACT except {} documented late-arrival exceptions, all flagged suspect"
         .format(len(explained)) if not unexplained else
         "UNEXPLAINED MISMATCHES -- streaming path is wrong")))
    for k in unexplained[:5]:
        print("   unexplained bucket {}".format(k))

    # ---- B) parity against the full truth ---------------------------------
    batch_truth = build_batch(truth, WIDTH_MS)
    b_res = parity(stream_bars, batch_truth)
    print("\n" + "=" * 76)
    print("PARITY B: streaming vs batch over ORIGINAL ticks (must NOT be exact)")
    print("-" * 76)
    print("mismatched bars           : {}".format(len(b_res["mismatched"])))
    print("bars missing from stream  : {}".format(len(b_res["only_in_batch"])))
    print("of the affected bars, how many did the pipeline flag suspect?")
    affected = set(k for k, _, _ in b_res["mismatched"]) | set(b_res["only_in_batch"])
    flagged = sum(1 for k in affected
                  if k in stream_bars and stream_bars[k].suspect)
    missing = sum(1 for k in affected if k not in stream_bars)
    print("   affected bars          : {}".format(len(affected)))
    print("   present and flagged    : {}".format(flagged))
    print("   absent entirely        : {}  <- a consumer sees a HOLE, not a wrong bar"
          .format(missing))
    print("\nThis is the number that matters operationally: data lost to a feed")
    print("outage cannot be recovered by cleverness, so the requirement is that a")
    print("consumer can always tell the difference between 'quiet market' and")
    print("'we were blind'. Flags live ON the bars, not only in an alert.")

    # ---- rebuild drill -----------------------------------------------------
    print("\n" + "=" * 76)
    print("REBUILD DRILL: recompute one hour from the archive")
    print("-" * 76)
    span_ms = truth[-1].event_time_ms - truth[0].event_time_ms
    # The window MUST be aligned to bar boundaries. Rebuilding an arbitrary
    # timespan re-derives the edge bars from a partial tick population and
    # produces two bars that are wrong in a way that looks like a parity bug.
    # This is the actual trap in a reprocessing runbook, so the alignment is
    # explicit rather than incidental.
    raw_start = truth[0].event_time_ms + span_ms // 3
    hour_start = (raw_start // WIDTH_MS) * WIDTH_MS
    hour_end = min(hour_start + 3_600_000, truth[-1].event_time_ms)
    hour_end = (hour_end // WIDTH_MS) * WIDTH_MS
    print("session span              : {:.2f} hours".format(span_ms / 3_600_000))
    print("window (bar-aligned)      : [{}, {})".format(hour_start, hour_end))
    window = [t for t in surviving if hour_start <= t.event_time_ms < hour_end]
    t0 = time.perf_counter()
    rebuilt = build_batch(window, WIDTH_MS)
    secs = time.perf_counter() - t0
    compared = [k for k in rebuilt if hour_start <= k < hour_end]
    diffs = [k for k in compared if rebuilt[k].key() != batch_surviving[k].key()]
    same = not diffs
    print("ticks in window           : {:,}".format(len(window)))
    print("bars rebuilt              : {:,} in {:.3f}s".format(len(rebuilt), secs))
    print("bars compared             : {:,}".format(len(compared)))
    print("bars differing            : {}".format(len(diffs)))
    print("matches the original bars : {}".format(same))
    print("\nA rebuild that reproduces the archive bit-for-bit is what makes a")
    print("logic fix safe to deploy: you can replay history through the new code")
    print("and diff, instead of arguing about what the old code would have done.")
    print("=" * 76)
    return 0 if not unexplained and same else 1


if __name__ == "__main__":
    raise SystemExit(main())
