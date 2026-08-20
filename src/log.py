"""A durable partitioned log with offsets, consumer groups and checkpoints.

**This is not Kafka and does not claim to be.** It is the subset of Kafka's
semantics that the correctness of this pipeline actually depends on, implemented
locally so those semantics can be tested rather than assumed:

  PARTITIONS         records are keyed (by symbol) and a key always lands in the
                     same partition. That is what preserves per-key ordering,
                     which is the only ordering guarantee Kafka gives and the
                     only one the bar builder needs.
  DURABLE OFFSETS    every record has a monotonic offset within its partition,
                     and the log is append-only on disk. A consumer that dies
                     resumes from its committed offset rather than from the
                     beginning or from "now".
  CONSUMER GROUPS    a group commits offsets independently, so two consumers
                     (say, the bar builder and the archiver) read the same log
                     at different positions without interfering.
  AT-LEAST-ONCE      offsets are committed AFTER processing. A crash between
                     processing and commit replays the record -- which is why
                     every consumer downstream has to be idempotent, and why the
                     bar builder suppresses duplicates by (seq, event_time).

What is deliberately absent, because faking it would be worse than the gap:
replication, leader election, ISR, exactly-once transactions, compaction, and
any notion of a broker at all. Those are the reasons to run Kafka; this exists
so that "the consumer resumes from its offset" is a tested property here rather
than a hope pinned on infrastructure this repo does not have.
"""
from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Record:
    partition: int
    offset: int
    key: str
    value: dict


class PartitionedLog:
    def __init__(self, root: Path, partitions: int = 4):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.partitions = partitions
        self._locks = [threading.Lock() for _ in range(partitions)]
        for p in range(partitions):
            self._path(p).touch(exist_ok=True)

    def _path(self, partition: int) -> Path:
        return self.root / "partition-{}.log".format(partition)

    def partition_for(self, key: str) -> int:
        """Stable hash, NOT Python's hash(): PYTHONHASHSEED randomises str
        hashing per process, so a key would land in a different partition after
        a restart and per-key ordering would silently break across it."""
        h = 0
        for ch in key:
            h = (h * 31 + ord(ch)) & 0xFFFFFFFF
        return h % self.partitions

    def append(self, key: str, value: dict) -> Record:
        p = self.partition_for(key)
        with self._locks[p]:
            path = self._path(p)
            offset = sum(1 for _ in path.open("r", encoding="utf-8"))
            with path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps({"key": key, "value": value}) + "\n")
                fh.flush()
            return Record(p, offset, key, value)

    def append_many(self, items: list[tuple[str, dict]]) -> int:
        by_partition: dict[int, list] = {}
        for key, value in items:
            by_partition.setdefault(self.partition_for(key), []).append((key, value))
        written = 0
        for p, rows in by_partition.items():
            with self._locks[p]:
                with self._path(p).open("a", encoding="utf-8") as fh:
                    for key, value in rows:
                        fh.write(json.dumps({"key": key, "value": value}) + "\n")
                        written += 1
                    fh.flush()
        return written

    def read(self, partition: int, from_offset: int = 0,
             limit: int | None = None) -> list[Record]:
        out = []
        with self._path(partition).open("r", encoding="utf-8") as fh:
            for offset, line in enumerate(fh):
                if offset < from_offset:
                    continue
                d = json.loads(line)
                out.append(Record(partition, offset, d["key"], d["value"]))
                if limit and len(out) >= limit:
                    break
        return out

    def high_water_mark(self, partition: int) -> int:
        with self._path(partition).open("r", encoding="utf-8") as fh:
            return sum(1 for _ in fh)

    def total_records(self) -> int:
        return sum(self.high_water_mark(p) for p in range(self.partitions))


class ConsumerGroup:
    """Committed offsets per (group, partition), persisted to disk."""

    def __init__(self, log: PartitionedLog, group_id: str):
        self.log = log
        self.group_id = group_id
        self.path = log.root / "offsets-{}.json".format(group_id)
        self.offsets: dict[int, int] = self._load()

    def _load(self) -> dict[int, int]:
        if not self.path.exists():
            return {}
        return {int(k): v for k, v in json.loads(
            self.path.read_text(encoding="utf-8")).items()}

    def _persist(self) -> None:
        self.path.write_text(json.dumps({str(k): v for k, v in self.offsets.items()}),
                             encoding="utf-8")

    def poll(self, partition: int, max_records: int = 500) -> list[Record]:
        return self.log.read(partition, self.offsets.get(partition, 0), max_records)

    def commit(self, partition: int, offset: int) -> None:
        """Commit the offset of the NEXT record to read.

        Called after processing, which is what makes delivery at-least-once: a
        crash between processing and commit replays the batch. Committing first
        would make it at-most-once and lose records on crash, which for market
        data means a hole in a bar nobody can see.
        """
        self.offsets[partition] = offset
        self._persist()

    def lag(self, partition: int) -> int:
        return self.log.high_water_mark(partition) - self.offsets.get(partition, 0)

    def total_lag(self) -> int:
        return sum(self.lag(p) for p in range(self.log.partitions))
