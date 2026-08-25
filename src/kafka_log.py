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
        else:
            # WAIT FOR LEADER ELECTION before returning. `create_topics` returns
            # once the controller has accepted the topic, not once every
            # partition has an elected leader -- and a producer that sends into
            # that gap gets NotLeaderForPartitionError.
            #
            # This is not hypothetical tidiness: it is the error the pipeline
            # actually hit the first time it ran against the broker. The
            # producer already sets retries=5, and that was not enough, because
            # the retry budget is spent before the metadata propagates.
            #
            # It is a genuine distributed-systems race rather than a client
            # defect, and the fix belongs here rather than in a retry loop at
            # every call site -- a topic with no leader is not ready to be
            # written to, so "ready" is what the constructor should mean.
            self._await_leaders(admin, topic, partitions)
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

    @staticmethod
    def _await_leaders(admin, topic: str, partitions: int,
                       timeout_s: float = 30.0) -> None:
        """Wait until every partition is described, with a leader and no error.

        A NECESSARY CHECK AND NOT A SUFFICIENT ONE, which is worth stating
        because the first version of this method assumed it was sufficient and
        the produce still failed.

        `describe_topics` reports `leader=1` for every partition IMMEDIATELY
        after creation -- measured, not guessed. The controller has assigned
        leadership in cluster metadata, but the broker has not necessarily
        finished transitioning to leader for those partitions locally, and a
        produce landing in that window gets NotLeaderForPartitionError.

        So this catches the slow cases (topic not yet visible, a partition
        genuinely leaderless, a non-zero error_code) and `append_many` carries a
        retry for the fast one that no metadata query can see. Readiness for
        writing is only observable by writing.
        """
        import time

        deadline = time.monotonic() + timeout_s
        last = None
        while time.monotonic() < deadline:
            try:
                meta = admin.describe_topics([topic])
            except Exception as exc:                        # noqa: BLE001
                last = exc
                time.sleep(0.3)
                continue
            parts = (meta[0].get("partitions") if meta else None) or []
            bad = [p.get("partition") for p in parts
                   if p.get("leader", -1) < 0 or p.get("error_code", 0)]
            if len(parts) >= partitions and not bad:
                return
            last = "partitions={} not_ready={}".format(len(parts), bad)
            time.sleep(0.3)
        raise RuntimeError(
            "topic {!r} still not fully led after {}s ({}). Refusing to return "
            "a log that would fail on first write."
            .format(topic, timeout_s, last))

    def append(self, key: str, value: dict) -> KafkaRecord:
        meta = self.producer.send(self.topic, key=key, value=value).get(timeout=30)
        return KafkaRecord(meta.partition, meta.offset, key, value)

    def append_many(self, items, attempts: int = 5) -> int:
        """Produce every item, retrying the ones that hit a RETRIABLE error.

        WHY A CALLER-LEVEL RETRY EXISTS WHEN THE PRODUCER ALREADY HAS
        `retries=5`, because that looks redundant and is not.

        A freshly created topic reports an elected leader in the controller's
        metadata before the broker has finished transitioning to leader for
        those partitions locally. `describe_topics` therefore answers
        `leader=1` immediately and is NOT a readiness signal -- an earlier
        version of this class waited on exactly that and still failed, which is
        how the distinction was found.

        The producer's own retries are consumed inside a few hundred
        milliseconds of backoff and give up before the broker is ready. A
        caller-level retry with a metadata refresh between attempts spans the
        gap, and only the FAILED items are resent -- resending the whole batch
        would duplicate everything that already succeeded, turning a transient
        error into permanent data corruption.

        Only NotLeaderForPartitionError and its siblings are retried. A
        serialisation failure or an oversized record is not transient and
        retrying it just fails five times more slowly.
        """
        import time

        from kafka.errors import (KafkaTimeoutError, NotLeaderForPartitionError,
                                  RequestTimedOutError)

        RETRIABLE = (NotLeaderForPartitionError, RequestTimedOutError,
                     KafkaTimeoutError)

        pending = list(items)
        sent = 0
        for attempt in range(attempts):
            futures = [(k, v, self.producer.send(self.topic, key=k, value=v))
                       for k, v in pending]
            failed = []
            for k, v, f in futures:
                try:
                    f.get(timeout=60)
                    sent += 1
                except RETRIABLE:
                    failed.append((k, v))
            self.producer.flush()
            if not failed:
                return sent
            pending = failed
            # Refresh metadata and back off before trying the stragglers again.
            self.producer._metadata.request_update()
            time.sleep(0.5 * (attempt + 1))

        raise RuntimeError(
            "{} of {} records still failing with a retriable error after {} "
            "attempts; refusing to report a partial produce as complete"
            .format(len(pending), len(items), attempts))

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


def available(bootstrap: str = BOOTSTRAP, attempts: int = 3,
              request_timeout_ms: int = 15_000) -> bool:
    """Is a broker reachable? Used to skip rather than fail.

    Retried, and with a longer budget than the 5s this used to allow, because
    A SKIP THAT LOOKS LIKE A PASS IS THE WORST OUTCOME A CONDITIONAL TEST CAN
    PRODUCE. A short probe against a broker that is up but busy returns False,
    the caller prints "skipping", the suite goes green, and nothing anywhere
    says the thing under test never ran.

    This was observed rather than anticipated: the same broker answered a
    standalone probe and failed the identical call inside a pipeline run,
    because it was loaded and the 5s window was not enough. The probe was wrong,
    not the broker.
    """
    import time

    from kafka import KafkaAdminClient

    for i in range(attempts):
        try:
            a = KafkaAdminClient(bootstrap_servers=bootstrap,
                                 request_timeout_ms=request_timeout_ms)
            a.close()
            return True
        except Exception:                                    # noqa: BLE001
            if i < attempts - 1:
                time.sleep(1.0)
    return False


def delete_topics(prefixes, bootstrap: str = BOOTSTRAP) -> list:
    """Remove throwaway topics left behind by earlier runs.

    Every parity run creates a uniquely-named topic and never removes it. On a
    long-lived broker those accumulate, and each one costs metadata, open file
    handles and log-recovery time at startup -- a broker carrying a hundred dead
    test topics is measurably slower to start and answer, which is how the
    availability probe above started failing in the first place.

    Prefix-scoped rather than "delete everything": internal topics
    (__consumer_offsets, __cluster_metadata) must survive, and deleting a topic
    is not reversible.
    """
    from kafka import KafkaAdminClient

    admin = KafkaAdminClient(bootstrap_servers=bootstrap,
                             request_timeout_ms=30_000)
    try:
        doomed = [t for t in admin.list_topics()
                  if any(t.startswith(p) for p in prefixes)
                  and not t.startswith("__")]
        if doomed:
            admin.delete_topics(doomed)
        return sorted(doomed)
    finally:
        admin.close()
