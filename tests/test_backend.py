"""One interface over two logs, and the places it honestly cannot be one."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src import backend as be
from src.kafka_log import available

ITEMS = [("BTC/USD", {"symbol": "BTC/USD", "seq": i, "price_minor": 100 + i})
         for i in range(50)] + \
        [("ETH/USD", {"symbol": "ETH/USD", "seq": i, "price_minor": 200 + i})
         for i in range(50)]


@pytest.fixture
def file_backend(tmp_path):
    b = be.make("file", root=tmp_path / "log", partitions=4)
    yield b
    b.close()


# ------------------------------------------------------------ file backend
def test_the_file_backend_round_trips_everything(file_backend):
    assert file_backend.append_many(ITEMS) == 100
    c = file_backend.consumer("g")
    assert len(c.drain_all()) == 100


def test_draining_loops_rather_than_polling_once(file_backend):
    """A single poll returns at most max_records and leaves the rest as lag,
    which is correct for the consumer and a bug in anything that then reports
    'drained'."""
    file_backend.append_many(ITEMS)
    c = file_backend.consumer("g")
    assert len(c.drain_all()) == 100
    assert c.total_lag() == 0


def test_two_groups_read_the_same_log_independently(file_backend):
    file_backend.append_many(ITEMS)
    a = file_backend.consumer("a")
    b = file_backend.consumer("b")
    assert len(a.drain_all()) == 100
    assert b.total_lag() == 100, "one group's progress moved another's position"
    assert len(b.drain_all()) == 100


def test_a_key_lands_in_exactly_one_partition(file_backend):
    """Per-key ordering means nothing if a key is spread across partitions, so
    it is checked rather than assumed."""
    file_backend.append_many(ITEMS)
    seen = {}
    for r in file_backend.consumer("g").drain_all():
        seen.setdefault(r.value["symbol"], set()).add(r.partition)
    assert all(len(p) == 1 for p in seen.values()), seen


def test_the_common_record_is_narrower_than_either_backends_own(file_backend):
    """A field present on one path and absent on the other is worse than no
    field: it works until the day the other backend is used."""
    file_backend.append_many(ITEMS[:1])
    r = file_backend.consumer("g").drain_all()[0]
    assert set(vars(r)) == {"partition", "offset", "key", "value"}


def test_only_the_file_backend_claims_partition_polling():
    """The abstraction leak, declared rather than hidden. Kafka's coordinator
    assigns partitions; a consumer does not choose. Emulating per-partition
    polling by filtering the assignment works with one consumer and is wrong the
    moment there are two."""
    assert be.FileBackend.supports_partition_polling is True
    assert be.KafkaBackend.supports_partition_polling is False


def test_an_unknown_backend_is_refused():
    with pytest.raises(ValueError, match="unknown backend"):
        be.make("redis")


# ----------------------------------------------------------------- kafka
kafka_only = pytest.mark.skipif(
    not available(),
    reason="no Kafka broker at 127.0.0.1:9092")


@kafka_only
def test_the_kafka_backend_round_trips_everything():
    import time

    b = be.make("kafka", topic="beq-{}".format(int(time.time())), partitions=4)
    try:
        assert b.append_many(ITEMS) == 100
        c = b.consumer("g")
        recs = c.drain_all()
        assert len(recs) == 100
        c.close()
    finally:
        b.close()


@kafka_only
def test_a_key_lands_in_one_kafka_partition_too():
    import time

    b = be.make("kafka", topic="bkey-{}".format(int(time.time())), partitions=4)
    try:
        b.append_many(ITEMS)
        c = b.consumer("g")
        seen = {}
        for r in c.drain_all():
            seen.setdefault(r.value["symbol"], set()).add(r.partition)
        c.close()
        assert seen and all(len(p) == 1 for p in seen.values()), seen
    finally:
        b.close()


@kafka_only
def test_the_two_backends_do_NOT_agree_on_which_partition_a_key_goes_to(tmp_path):
    """The claim the first version of docs/BACKEND_PARITY.md got wrong.

    It asserted both backends "partition on the same stable hash". They do not:
    the in-process log uses its own function and Kafka uses murmur2. On the
    recorded session both symbols hash to partition 0 on the file log and to 1
    and 3 on Kafka.

    Both are stable across restarts, which is the property that matters. But
    they are not the same, so committed offsets do not port between backends --
    an offset is a position IN a partition, and partition 2 does not hold the
    same keys on both sides.
    """
    import time

    f = be.make("file", root=tmp_path / "l", partitions=4)
    k = be.make("kafka", topic="bcmp-{}".format(int(time.time())), partitions=4)
    try:
        f.append_many(ITEMS)
        k.append_many(ITEMS)

        def mapping(backend, gid):
            c = backend.consumer(gid)
            out = {}
            for r in c.drain_all():
                out.setdefault(r.value["symbol"], set()).add(r.partition)
            c.close()
            return {s: sorted(p) for s, p in out.items()}

        fm, km = mapping(f, "gf"), mapping(k, "gk")
        assert set(fm) == set(km)
        assert fm != km, (
            "the two backends agreed on every partition; if this is now stable "
            "the offset-portability warning in docs/BACKEND_PARITY.md needs "
            "rechecking rather than trusting"
        )
    finally:
        f.close()
        k.close()


@kafka_only
def test_writing_to_a_freshly_created_topic_survives_the_leader_race():
    """The race the pipeline actually hit on its first run against the broker,
    and the wrong fix it defeated first.

    `create_topics` returns once the controller accepts the topic. A producer
    sending immediately gets NotLeaderForPartitionError, and the producer's own
    retries=5 is spent inside a few hundred ms of backoff before the broker is
    ready.

    The obvious fix -- wait until describe_topics reports a leader -- does NOT
    work, and that is the finding. describe_topics answers leader=1 for all four
    partitions immediately; the controller has assigned leadership while the
    broker has not finished transitioning locally. No metadata query can see the
    gap. Readiness for writing is only observable by writing, so append_many
    retries the failed items with a metadata refresh between attempts.
    """
    import time

    from src.kafka_log import KafkaLog

    log = KafkaLog("blead-{}".format(int(time.time())), partitions=4)
    try:
        # No sleep in the test: whatever makes this work has to be inside.
        n = log.append_many([("BTC/USD", {"symbol": "BTC/USD", "seq": i})
                             for i in range(20)])
        assert n == 20
    finally:
        log.close()


@kafka_only
def test_describe_topics_reports_a_leader_before_the_broker_can_be_written_to():
    """Pinning the measurement that disproved the obvious fix, because without
    it the retry in append_many looks like redundant belt-and-braces over the
    producer's own retries and would eventually be deleted."""
    import time

    from kafka import KafkaAdminClient
    from kafka.admin import NewTopic

    topic = "bmeta-{}".format(int(time.time()))
    admin = KafkaAdminClient(bootstrap_servers="127.0.0.1:9092",
                             request_timeout_ms=20_000)
    try:
        admin.create_topics([NewTopic(topic, 4, 1)])
        meta = admin.describe_topics([topic])
        parts = (meta[0].get("partitions") if meta else None) or []
        assert len(parts) == 4
        assert all(p.get("leader", -1) >= 0 for p in parts), (
            "if describe_topics now reports leaderless partitions right after "
            "creation, it HAS become a usable readiness signal and the retry "
            "in append_many can be reconsidered")
    finally:
        admin.close()


@kafka_only
def test_only_retriable_errors_are_retried():
    """A serialisation failure is not transient, and retrying it just fails five
    times more slowly while looking like a broker problem."""
    import time

    from src.kafka_log import KafkaLog

    log = KafkaLog("bnonretry-{}".format(int(time.time())), partitions=2)
    try:
        with pytest.raises(Exception) as exc:
            log.append_many([("k", {"bad": object()})])
        assert "retriable" not in str(exc.value).lower(), (
            "a non-serialisable value was treated as a transient broker error")
    finally:
        log.close()


@kafka_only
def test_the_availability_probe_retries_rather_than_reporting_a_false_skip():
    """A skip that looks like a pass is the worst outcome a conditional test can
    produce. The 5s single-shot probe this replaced returned False against a
    broker that was up but busy -- the caller printed "skipping", the suite went
    green, and nothing said the thing under test never ran."""
    import inspect

    from src.kafka_log import available as probe

    sig = inspect.signature(probe)
    assert sig.parameters["attempts"].default > 1
    assert sig.parameters["request_timeout_ms"].default >= 15_000
    assert probe() is True
