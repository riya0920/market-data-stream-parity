"""The same log interface, backed by a real Kafka broker.

`src/log.py` implements the subset of Kafka semantics this pipeline depends on
-- keyed partitions, durable offsets, consumer groups, at-least-once delivery --
so those properties could be TESTED rather than assumed. Its README was careful
to say what that did not give you: replication, leader election, ISR, exactly-
once, and any broker at all.

A broker is now available, so this backend exists to answer the question the
in-process log could only pose: **does the pipeline still produce the same bars
when the log is real?** `run_kafka_parity.py` runs both and diffs them.

WHY A PARITY TEST RATHER THAN A REPLACEMENT. Swapping the in-process log out
would delete the thing that makes the parity check possible. Keeping both, and
asserting they agree, is what turns "we implemented Kafka's semantics" from a
claim into a measurement -- and it is the only way to find the places where the
imitation was subtly wrong.

THE PARTITIONER IS THE INTERESTING PART. `PartitionedLog` uses a stable non-
Python hash so partition assignment survives a restart (Python's `hash()` on
str is salted per process). Kafka uses **murmur2** on the key bytes, and the two
do not agree -- so the same key lands in a different partition on each side.
That is not a bug in either, and it means bar-level parity holds while
partition-level assignment does not. `run_kafka_parity.py` reports both, because
a parity claim that quietly compares only what agrees is not a parity claim.
"""
from __future__ import annotations

import json
from dataclasses import dataclass

BOOTSTRAP = "127.0.0.1:9092"


@dataclass
class KafkaRecord:
    """Mirrors `log.Record` so consumers do not care which log they are on."""
    partition: int
    offset: int
    key: str
    value: dict
    timestamp_ms: int = 0


class KafkaLog:
    """Producer side, with the same `append` / `append_many` surface."""

    def __init__(self, topic: str, bootstrap: str = BOOTSTRAP,
                 partitions: int = 4, replication: int = 1):
        from kafka import KafkaAdminClient, KafkaProducer
        from kafka.admin import NewTopic
        from kafka.errors import TopicAlreadyExistsError

        self.topic = topic
        self.bootstrap = bootstrap
        self.partitions = partitions

        admin = KafkaAdminClient(bootstrap_servers=bootstrap,
                                 request_timeout_ms=20_000)
        try:
            admin.create_topics([NewTopic(topic, partitions, replication)])
        except TopicAlreadyExistsError:
            pass
        finally:
            admin.close()

        # acks='all' is the only setting compatible with the durability this
        # pipeline already claims. acks=1 acknowledges once the LEADER has the
        # record, so a leader failure between the ack and the replication loses
        # an acknowledged write -- which is exactly the class of loss the
        # in-process log's fsync-on-append was written to avoid.
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            linger_ms=5,
            retries=5)

    def append(self, key: str, value: dict) -> KafkaRecord:
        meta = self.producer.send(self.topic, key=key, value=value).get(timeout=30)
        return KafkaRecord(meta.partition, meta.offset, key, value)

    def append_many(self, items) -> int:
        futures = [self.producer.send(self.topic, key=k, value=v)
                   for k, v in items]
        for f in futures:
            f.get(timeout=60)
        self.producer.flush()
        return len(futures)

    def partition_for(self, key: str) -> int:
        """Kafka's own answer, obtained by asking it rather than reimplementing.

        Kafka partitions on murmur2 of the key bytes. Reimplementing that here
        to make the two logs agree would be writing a second implementation of
        the thing under test, so the parity report states the disagreement
        instead.
        """
        meta = self.producer.send(self.topic, key=key, value={"probe": True}
                                  ).get(timeout=30)
        return meta.partition

    def high_water_mark(self, partition: int) -> int:
        from kafka import KafkaConsumer, TopicPartition

        c = KafkaConsumer(bootstrap_servers=self.bootstrap,
                          consumer_timeout_ms=5_000)
        tp = TopicPartition(self.topic, partition)
        c.assign([tp])
        c.seek_to_end(tp)
        end = c.position(tp)
        c.close()
        return end

    def total_records(self) -> int:
        return sum(self.high_water_mark(p) for p in range(self.partitions))

    def close(self) -> None:
        self.producer.flush()
        self.producer.close()


class KafkaConsumerGroup:
    """Consumer side with committed offsets, so crash-resume is the broker's job.

    The in-process version persists offsets to a file and reloads them. This
    hands the same responsibility to Kafka's group coordinator, which is the
    part that could not be imitated: the in-process log has no coordinator, so
    it cannot rebalance, cannot detect a dead consumer, and cannot hand a
    partition to a live one.
    """

    def __init__(self, log: KafkaLog, group_id: str,
                 auto_offset_reset: str = "earliest"):
        from kafka import KafkaConsumer

        self.log = log
        self.group_id = group_id
        self.consumer = KafkaConsumer(
            log.topic,
            bootstrap_servers=log.bootstrap,
            group_id=group_id,
            enable_auto_commit=False,      # commit AFTER processing, not before
            auto_offset_reset=auto_offset_reset,
            key_deserializer=lambda b: b.decode("utf-8") if b else None,
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            # Generous, because a fresh group has to JOIN before it sees
            # anything and the join is slow on a loaded single broker. An
            # 8-second timeout returned zero records against a topic holding
            # 8,000 -- the consumer had not finished joining, and "consumed 0"
            # is indistinguishable from "the topic is empty" unless you know
            # that.
            consumer_timeout_ms=30_000,
            max_poll_records=500)

    def drain(self, max_records: int | None = None) -> list:
        """Everything currently available, not one poll's worth.

        A single poll is NOT a drain -- that mistake in an earlier build made
        the harness report the remainder as consumer lag and then claim it had
        consumed everything. It was correct behaviour for the consumer and a bug
        in the thing measuring it.
        """
        import time

        out = []
        idle_deadline = time.monotonic() + 30.0
        selector_faults = 0
        while time.monotonic() < idle_deadline:
            try:
                batch = self.consumer.poll(timeout_ms=2_000, max_records=500)
            except ValueError as exc:
                # kafka-python on Windows: a socket is closed underneath the
                # selector and the next poll raises "Invalid file descriptor:
                # -1". It is a client defect rather than anything about the
                # broker or the data, and the client reconnects on the
                # following poll -- so this retries a bounded number of times
                # instead of either crashing or silently returning short.
                #
                # Swallowing it without a bound would be the dangerous version:
                # a drain that quietly returns fewer records than the topic
                # holds looks exactly like an empty topic.
                if "file descriptor" not in str(exc):
                    raise
                selector_faults += 1
                if selector_faults > 20:
                    raise RuntimeError(
                        "consumer selector faulted {} times; refusing to "
                        "report a partial drain as complete".format(
                            selector_faults)) from exc
                time.sleep(0.25)
                continue
            if not batch:
                continue
            for tp, msgs in batch.items():
                for m in msgs:
                    out.append(KafkaRecord(m.partition, m.offset, m.key,
                                           m.value, m.timestamp))
            # Records arrived, so reset the idle clock. Draining means "until
            # nothing more comes", not "until one poll is empty" -- a poll can
            # return empty simply because the fetch had not landed yet.
            idle_deadline = time.monotonic() + 8.0
            if max_records and len(out) >= max_records:
                break
        self.selector_faults = selector_faults
        return out

    def commit(self) -> None:
        """Commit AFTER processing. Committing first converts at-least-once
        into at-most-once, and a crash then loses records nobody replays."""
        self.consumer.commit()

    def lag(self) -> dict:
        from kafka import TopicPartition

        out = {}
        for p in range(self.log.partitions):
            tp = TopicPartition(self.log.topic, p)
            committed = self.consumer.committed(tp) or 0
            out[p] = self.log.high_water_mark(p) - committed
        return out

    def close(self) -> None:
        self.consumer.close()


def available(bootstrap: str = BOOTSTRAP) -> bool:
    """Is a broker reachable? Used to skip rather than fail."""
    try:
        from kafka import KafkaAdminClient

        a = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=5_000)
        a.close()
        return True
    except Exception:                                        # noqa: BLE001
        return False
