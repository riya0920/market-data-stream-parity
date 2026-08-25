"""One interface over the in-process log and a real Kafka broker.

`run_pipeline.py` ran only on `PartitionedLog`. Kafka existed in
`run_kafka_parity.py`, off to one side, comparing the two on a synthetic
workload. That is a useful check and it is not the same as the pipeline
actually running on a broker -- a parity test proves two implementations agree
on what it thought to compare, and the pipeline is where the assumptions that
were never written down live.

THE ABSTRACTION LEAKS, AND PRETENDING OTHERWISE IS THE BUG. The two consumer
models are not the same shape:

  IN-PROCESS   `poll(partition)`. The caller names the partition it wants. It
               can read partition 2 and ignore the rest, because there is no
               coordinator and nothing to negotiate with.

  KAFKA        the group coordinator ASSIGNS partitions to members. A consumer
               does not choose; it subscribes and is told. `poll()` returns
               whatever the assignment yields, and asking for a specific
               partition means abandoning the group and assigning manually --
               which gives up rebalancing, liveness detection, and every other
               reason to use a consumer group.

So the shared interface is the INTERSECTION, not the union: `drain_all()`, which
both can honestly implement. Per-partition polling stays on the in-process
backend where it is real, and any pipeline code written against it does not
port. Naming that here is the point -- the alternative is a wrapper that
silently emulates per-partition polling on Kafka by filtering the assignment,
which works in a test with one consumer and is wrong the moment there are two.

WHAT IS PRESERVED ACROSS BOTH, and what is not. Two entries here were written
wrong first and are corrected from the measurement rather than softened:

  PRESERVED      per-key ordering. Each backend puts a key in exactly one
                 partition and returns its records in order.

  NOT PRESERVED  the partition NUMBER. This originally claimed both backends
                 "partition on the same stable hash". They do not. The
                 in-process log uses its own stable function; Kafka uses
                 murmur2. On the recorded session both symbols hash to
                 partition 0 on the file log and to 1 and 3 on Kafka -- same
                 keys, same partition count, different mapping.

                 Both are stable across restarts, which is the property that
                 matters and the reason neither uses Python's `hash()`. But
                 committed offsets do not port between them (an offset is a
                 position IN a partition), any hardcoded key-to-partition
                 mapping is backend-specific, and the skew differs: the file
                 log put every symbol in one partition while Kafka spread them
                 over two. With few keys, the choice of hash is a capacity
                 decision.

  NOT GUARANTEED global interleaving. The file backend drains
                 partition-by-partition, so its order is deterministic. Kafka
                 guarantees per-partition order only. On this recording a Kafka
                 drain happened to come back GROUPED by partition, because the
                 whole topic fits in one or two fetches -- an artifact of size,
                 not a promise. Code relying on it passes every test written
                 against small data and breaks at volume, which is the worst
                 available failure schedule. This project's bars are safe under
                 either because they sort by event time within each symbol.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass
class Record:
    """The common record shape. Deliberately narrower than either backend's own
    record: anything one has and the other does not stays out, because a field
    that is present on one path and absent on the other is worse than no field.
    """
    partition: int
    offset: int
    key: str
    value: dict


class Backend:
    name = "abstract"
    supports_partition_polling = False

    def append_many(self, items) -> int:
        raise NotImplementedError

    def consumer(self, group_id: str):
        raise NotImplementedError

    def high_water_mark(self, partition: int) -> int:
        raise NotImplementedError

    def total_records(self) -> int:
        return sum(self.high_water_mark(p) for p in range(self.partitions))

    def close(self) -> None:
        pass


class FileBackend(Backend):
    name = "file"
    supports_partition_polling = True

    def __init__(self, root: Path, partitions: int = 4, **kw):
        from .log import PartitionedLog

        self.log = PartitionedLog(root, partitions=partitions, **kw)
        self.partitions = partitions

    def append_many(self, items) -> int:
        return self.log.append_many(items)

    def high_water_mark(self, partition: int) -> int:
        return self.log.high_water_mark(partition)

    def consumer(self, group_id: str):
        return FileConsumer(self.log, group_id)


class FileConsumer:
    def __init__(self, log, group_id: str):
        from .log import ConsumerGroup

        self.log = log
        self.group = ConsumerGroup(log, group_id)

    def drain_all(self) -> list:
        """Everything currently available, across every partition.

        Loops per partition until empty rather than polling once: a single poll
        returns at most max_records and leaves the rest as lag, which is correct
        for the consumer and a bug in anything that then reports "drained".
        """
        out = []
        for p in range(self.log.partitions):
            while True:
                batch = self.group.poll(p)
                if not batch:
                    break
                for r in batch:
                    out.append(Record(p, r.offset, r.key, r.value))
                self._last = (p, batch[-1].offset + 1)
                self.group.commit(p, batch[-1].offset + 1)
        return out

    def total_lag(self) -> int:
        return self.group.total_lag()

    def close(self) -> None:
        pass


class KafkaBackend(Backend):
    name = "kafka"
    supports_partition_polling = False

    def __init__(self, topic: str, partitions: int = 4,
                 bootstrap: str = "127.0.0.1:9092", **kw):
        from .kafka_log import KafkaLog

        self.log = KafkaLog(topic, bootstrap=bootstrap, partitions=partitions, **kw)
        self.partitions = partitions

    def append_many(self, items) -> int:
        return self.log.append_many(items)

    def high_water_mark(self, partition: int) -> int:
        return self.log.high_water_mark(partition)

    def consumer(self, group_id: str):
        return KafkaConsumer(self.log, group_id)

    def close(self) -> None:
        self.log.close()


class KafkaConsumer:
    def __init__(self, log, group_id: str):
        from .kafka_log import KafkaConsumerGroup

        self.log = log
        self.group = KafkaConsumerGroup(log, group_id)

    def drain_all(self) -> list:
        recs = self.group.drain()
        out = [Record(r.partition, r.offset, r.key, r.value) for r in recs]
        if out:
            self.group.commit()
        return out

    def total_lag(self) -> int:
        return sum(self.group.lag().values())

    def close(self) -> None:
        self.group.close()


def make(kind: str, *, root: Path | None = None, topic: str | None = None,
         partitions: int = 4, **kw) -> Backend:
    if kind == "file":
        return FileBackend(root, partitions=partitions, **kw)
    if kind == "kafka":
        return KafkaBackend(topic, partitions=partitions, **kw)
    raise ValueError("unknown backend {!r}".format(kind))
