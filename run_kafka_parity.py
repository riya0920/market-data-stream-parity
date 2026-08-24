"""The in-process log against a REAL Kafka broker, on the same ticks.

    python run_kafka_parity.py --ticks 20000

`src/log.py` implements the subset of Kafka's semantics this pipeline depends
on, and its README was careful about what that did not include. A broker is now
available, so the claim gets tested instead of asserted: run the same ticks
through both logs, consume both, build bars from both, and diff.

WHAT AGREEMENT WOULD AND WOULD NOT PROVE. Identical bars establish that the
in-process log preserves what the pipeline actually reads out of a log --
per-key ordering and complete delivery. It does not establish that the imitation
is Kafka: replication, leader election, ISR and exactly-once are not exercised
by a single-broker cluster either, and those are the reasons to run Kafka.

So this is a parity check on the semantics that were claimed, not a coronation.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from src.bars import build_batch
from src.kafka_log import BOOTSTRAP, KafkaConsumerGroup, KafkaLog, available
from src.log import ConsumerGroup, PartitionedLog
from src.replay import Tick, generate_session


def _to_tick(v: dict) -> Tick:
    return Tick(**v)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ticks", type=int, default=20_000)
    ap.add_argument("--bootstrap", default=BOOTSTRAP)
    ap.add_argument("--partitions", type=int, default=4)
    args = ap.parse_args()

    print("=" * 80)
    print("IN-PROCESS LOG vs A REAL KAFKA BROKER")
    print("=" * 80)

    if not available(args.bootstrap):
        print("no broker at {}. Start one, or run the in-process".format(args.bootstrap))
        print("pipeline instead:  python run_pipeline.py")
        return 1

    ticks = generate_session(n_ticks=args.ticks)
    # This session has no symbol column -- generate_session emits one stream.
    # Key on the sequence number modulo a small alphabet so partitioning is
    # actually exercised: a single key would put everything on one partition
    # and the parity check would prove nothing about keyed routing.
    keys = ["k{}".format(t.seq % 8) for t in ticks]
    items = [(k, t.__dict__) for k, t in zip(keys, ticks)]
    print("ticks        : {:,}".format(len(ticks)))
    print("keys         : {}".format(sorted(set(keys))))
    print("partitions   : {}".format(args.partitions))

    # ------------------------------------------------------- in-process
    print("\n" + "-" * 80)
    print("1. IN-PROCESS LOG")
    print("-" * 80)
    # Wipe the whole directory, not just *.jsonl. A previous run left both its
    # segments AND its committed consumer offsets, so the second run reported
    # "written 16,000, consumed 8,000" -- which is correct consumer behaviour
    # (it resumed from its commit) and reads like a bug in the report.
    import shutil

    root = ROOT / "data" / "parity_log"
    if root.exists():
        shutil.rmtree(root)
    local = PartitionedLog(root, partitions=args.partitions)
    t0 = time.perf_counter()
    local.append_many(items)
    local_write = time.perf_counter() - t0

    group = ConsumerGroup(local, "parity")
    local_records = []
    for p in range(args.partitions):
        while True:
            batch = group.poll(p, max_records=1000)
            if not batch:
                break
            local_records.extend(batch)
            group.commit(p, batch[-1].offset + 1)
    print("written   : {:,} in {:.2f}s".format(local.total_records(), local_write))
    print("consumed  : {:,}".format(len(local_records)))

    # ------------------------------------------------------------ kafka
    print("\n" + "-" * 80)
    print("2. REAL KAFKA")
    print("-" * 80)
    topic = "parity-{}".format(int(time.time()))
    kl = KafkaLog(topic, args.bootstrap, partitions=args.partitions)
    t0 = time.perf_counter()
    kl.append_many(items)
    kafka_write = time.perf_counter() - t0

    kg = KafkaConsumerGroup(kl, "parity-group")
    kafka_records = kg.drain()
    kg.commit()
    print("written   : {:,} in {:.2f}s".format(kl.total_records(), kafka_write))
    print("consumed  : {:,}".format(len(kafka_records)))
    print("lag after commit: {}".format(kg.lag()))

    # ----------------------------------------------------------- parity
    print("\n" + "=" * 80)
    print("3. DO THE PIPELINES AGREE?")
    print("-" * 80)

    local_bars = build_batch([_to_tick(r.value) for r in local_records])
    kafka_bars = build_batch([_to_tick(r.value) for r in kafka_records
                              if "probe" not in r.value])

    print("{:<34}{:>14}{:>14}".format("", "in-process", "kafka"))
    print("{:<34}{:>14,}{:>14,}".format("records consumed", len(local_records),
                                        len(kafka_records)))
    print("{:<34}{:>14,}{:>14,}".format("bars built", len(local_bars),
                                        len(kafka_bars)))

    mismatched = []
    for bucket, lb in sorted(local_bars.items()):
        kb = kafka_bars.get(bucket)
        if kb is None:
            mismatched.append((bucket, "absent in kafka"))
            continue
        for field in ("open_minor", "high_minor", "low_minor", "close_minor",
                      "volume", "tick_count"):
            a, b = getattr(lb, field, None), getattr(kb, field, None)
            if a != b:
                mismatched.append((bucket, "{}: {} vs {}".format(field, a, b)))
                break
    only_kafka = set(kafka_bars) - set(local_bars)

    print("{:<34}{:>14}{:>14}".format("bars only on this side", len(
        set(local_bars) - set(kafka_bars)), len(only_kafka)))
    print("{:<34}{:>29}".format("MISMATCHED BARS", len(mismatched)))
    for bucket, why in mismatched[:5]:
        print("   bucket {}: {}".format(bucket, why))

    if not mismatched and not only_kafka:
        print("\nEXACT. Every bar agrees on open, high, low, close, volume and")
        print("trade count. The in-process log preserves what this pipeline")
        print("actually reads out of a log -- per-key ordering and complete")
        print("delivery -- and that is now measured rather than claimed.")

    # -------------------------------------------------- where they differ
    print("\n" + "=" * 80)
    print("4. WHERE THEY DO **NOT** AGREE, AND WHY THAT IS FINE")
    print("-" * 80)
    sym = sorted(set(keys))[0]
    mine = local.partition_for(sym)
    theirs = kl.partition_for(sym)
    agree = sum(1 for k in sorted(set(keys))
                if local.partition_for(k) == kl.partition_for(k))
    print("partition for {!r}: in-process {}  kafka {}".format(sym, mine, theirs))
    print("keys whose partition agrees: {} of {}".format(agree, len(set(keys))))
    print()
    if agree == len(set(keys)):
        print("They happen to agree on every key here, which is coincidence at")
        print("4 partitions and 8 keys rather than a shared algorithm -- two")
        print("different hashes mod 4 collide often. Do not read it as the two")
        print("partitioners being the same.")
    print()
    print("`PartitionedLog` uses a stable")
    print("non-Python hash so assignment survives a restart -- Python's hash()")
    print("on str is salted per process, so the obvious implementation")
    print("reshuffles every partition on every restart. Kafka uses murmur2 on")
    print("the key bytes.")
    print()
    print("Reimplementing murmur2 to make these agree would be writing a second")
    print("copy of the thing under test. The property that matters is that all")
    print("records for ONE key land on ONE partition, which both satisfy, and")
    print("bar parity above is what proves it end to end.")
    print()
    print("Reporting only the number that agrees would not be a parity check.")

    print("\n" + "=" * 80)
    print("5. WHAT A SINGLE BROKER STILL CANNOT SHOW")
    print("-" * 80)
    print("Replication, leader election, ISR shrink, and exactly-once")
    print("transactions -- the actual reasons to run Kafka -- need more than one")
    print("broker. This cluster has one. So the in-process log's 'not built'")
    print("list loses its first line and keeps the rest, and that is the honest")
    print("bookkeeping.")
    print("=" * 80)

    kg.close()
    kl.close()
    return 1 if (mismatched or only_kafka) else 0


if __name__ == "__main__":
    raise SystemExit(main())
