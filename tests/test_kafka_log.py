"""The Kafka backend, and what it proves about the in-process one.

Skipped when no broker is reachable, so the suite still runs anywhere. When one
IS reachable, these test the semantics `src/log.py` claimed to imitate --
per-key ordering, committed offsets, crash-resume -- against the real thing.
"""
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

pytest.importorskip("kafka")

from src.bars import build_batch
from src.kafka_log import BOOTSTRAP, KafkaConsumerGroup, KafkaLog, available
from src.log import ConsumerGroup, PartitionedLog
from src.replay import Tick, generate_session

pytestmark = pytest.mark.skipif(
    not available(BOOTSTRAP),
    reason="no Kafka broker at {}".format(BOOTSTRAP))


@pytest.fixture
def topic():
    return "test-{}".format(int(time.time() * 1000))


def _items(n=400):
    ticks = generate_session(n_ticks=n)
    return [("k{}".format(t.seq % 4), t.__dict__) for t in ticks]


def test_everything_produced_is_consumed(topic):
    items = _items(300)
    log = KafkaLog(topic, partitions=4)
    log.append_many(items)
    group = KafkaConsumerGroup(log, "g-all")
    got = group.drain()
    group.close()
    log.close()
    assert len(got) == len(items)


def test_records_for_one_key_land_on_one_partition(topic):
    """The property the pipeline actually depends on. Two keys may share a
    partition; one key must never span two, or per-key ordering is gone."""
    log = KafkaLog(topic, partitions=4)
    log.append_many(_items(400))
    group = KafkaConsumerGroup(log, "g-key")
    got = group.drain()
    group.close()
    log.close()

    by_key = {}
    for r in got:
        by_key.setdefault(r.key, set()).add(r.partition)
    spread = {k: v for k, v in by_key.items() if len(v) > 1}
    assert not spread, "keys split across partitions: {}".format(spread)


def test_offsets_are_monotonic_within_a_partition(topic):
    log = KafkaLog(topic, partitions=4)
    log.append_many(_items(300))
    group = KafkaConsumerGroup(log, "g-off")
    got = group.drain()
    group.close()
    log.close()

    seen = {}
    for r in got:
        prev = seen.get(r.partition)
        assert prev is None or r.offset > prev, (
            "partition {} went backwards: {} after {}".format(
                r.partition, r.offset, prev))
        seen[r.partition] = r.offset


def test_a_committed_group_resumes_rather_than_replays(topic):
    """The crash-resume property. A second consumer in the SAME group must not
    re-read what the first one committed, or every restart double-counts."""
    log = KafkaLog(topic, partitions=4)
    log.append_many(_items(200))

    first = KafkaConsumerGroup(log, "g-resume")
    got_first = first.drain()
    first.commit()
    first.close()
    assert got_first

    second = KafkaConsumerGroup(log, "g-resume")
    got_second = second.drain(max_records=10)
    second.close()
    log.close()
    assert not got_second, "committed records were replayed"


def test_a_different_group_reads_from_the_beginning(topic):
    """Groups are independent. If they were not, adding a consumer would steal
    records from an existing pipeline."""
    log = KafkaLog(topic, partitions=4)
    log.append_many(_items(200))

    a = KafkaConsumerGroup(log, "g-a")
    got_a = a.drain()
    a.commit()
    a.close()

    b = KafkaConsumerGroup(log, "g-b")
    got_b = b.drain()
    b.close()
    log.close()
    assert len(got_a) == len(got_b) == 200


def test_lag_is_zero_after_a_full_drain_and_commit(topic):
    log = KafkaLog(topic, partitions=4)
    log.append_many(_items(200))
    group = KafkaConsumerGroup(log, "g-lag")
    group.drain()
    group.commit()
    lag = group.lag()
    group.close()
    log.close()
    assert sum(lag.values()) == 0, lag


def test_bars_from_kafka_match_bars_from_the_in_process_log(topic, tmp_path):
    """The parity claim, as a test rather than a script.

    Identical bars establish that the in-process log preserves what this
    pipeline reads out of a log. It does NOT establish that the imitation is
    Kafka -- replication, leader election and exactly-once are not exercised by
    a single-broker cluster.
    """
    items = _items(600)

    local = PartitionedLog(tmp_path / "log", partitions=4)
    local.append_many(items)
    lg = ConsumerGroup(local, "parity")
    local_records = []
    for p in range(4):
        while True:
            batch = lg.poll(p, max_records=500)
            if not batch:
                break
            local_records.extend(batch)
            lg.commit(p, batch[-1].offset + 1)

    log = KafkaLog(topic, partitions=4)
    log.append_many(items)
    group = KafkaConsumerGroup(log, "parity-k")
    kafka_records = group.drain()
    group.close()
    log.close()

    local_bars = build_batch([Tick(**r.value) for r in local_records])
    kafka_bars = build_batch([Tick(**r.value) for r in kafka_records])

    assert set(local_bars) == set(kafka_bars)
    for bucket, lb in local_bars.items():
        kb = kafka_bars[bucket]
        for f in ("open_minor", "high_minor", "low_minor", "close_minor",
                  "volume", "tick_count"):
            assert getattr(lb, f) == getattr(kb, f), (bucket, f)


def test_the_two_partitioners_do_not_agree_and_that_is_expected(topic):
    """Kafka partitions on murmur2; PartitionedLog uses its own stable hash.
    Reimplementing murmur2 to force agreement would be writing a second copy of
    the thing under test, so the difference is asserted rather than removed."""
    log = KafkaLog(topic, partitions=4)
    local = PartitionedLog(Path(topic + "-tmp"), partitions=4)
    keys = ["k{}".format(i) for i in range(16)]
    disagreements = sum(1 for k in keys
                        if local.partition_for(k) != log.partition_for(k))
    log.close()
    assert disagreements > 0, (
        "the two partitioners agreed on all 16 keys, which would mean one of "
        "them changed to match the other")
