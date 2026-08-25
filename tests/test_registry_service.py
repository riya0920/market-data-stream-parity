"""The registry as a service, and what only a service can do."""
import json
import sys
import threading
import time
from pathlib import Path
from urllib import error, request

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import registry_service as rs
from src.schema_registry import IncompatibleSchema, Registry

BASE = [{"name": "symbol", "type": "string"},
        {"name": "price", "type": "float"},
        {"name": "size", "type": "float"}]


@pytest.fixture
def service():
    # Port 0 lets the OS pick, so a stale server from another run cannot make
    # this suite pass against the wrong process -- which is the kind of false
    # green a fixed port invites.
    rs._REG = Registry()
    srv = rs.serve(port=0)
    host, port = srv.server_address[0], srv.server_address[1]
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    time.sleep(0.2)
    yield "http://{}:{}".format(host, port)
    srv.shutdown()


@pytest.fixture
def client(service):
    return rs.RegistryClient(base=service)


def _get(base, path):
    with request.urlopen(base + path, timeout=3) as r:
        return json.loads(r.read())


# ------------------------------------------------------------- over HTTP
def test_a_schema_registers_over_the_network(client):
    assert client.register("trade", {"fields": BASE}) == 1


def test_a_compatible_change_bumps_the_version(client):
    client.register("trade", {"fields": BASE})
    v = client.register("trade", {"fields": BASE + [
        {"name": "venue", "type": "string", "required": False,
         "default": "unknown"}]})
    assert v == 2


def test_a_breaking_change_is_refused_with_409(client, service):
    """409, the code Confluent uses, because this is a conflict with existing
    state rather than a malformed request. A 400 would tell a producer its
    payload was wrong when the payload is fine and the HISTORY disagrees."""
    client.register("trade", {"fields": BASE})
    with pytest.raises(IncompatibleSchema):
        client.register("trade", {"fields": [
            {"name": "symbol", "type": "string"},
            {"name": "px", "type": "float"}]})

    raw = json.dumps({"fields": [{"name": "symbol", "type": "string"},
                                 {"name": "px", "type": "float"}]}).encode()
    req = request.Request(service + "/subjects/trade/versions", data=raw,
                          headers={"Content-Type": "application/json"},
                          method="POST")
    with pytest.raises(error.HTTPError) as exc:
        request.urlopen(req, timeout=3)
    assert exc.value.code == 409


# ---------------------------------- what an in-process registry cannot do
def test_a_second_process_sees_what_the_first_registered(client, service):
    """The whole point. In-process, two producers hold contradictory ideas of
    what version 3 is and both believe they are right; a service is the single
    thing that can say no to the second, and only because it saw the first."""
    client.register("trade", {"fields": BASE})

    other = rs.RegistryClient(base=service)          # a different "process"
    with pytest.raises(IncompatibleSchema):
        other.register("trade", {"fields": [
            {"name": "symbol", "type": "string"},
            {"name": "px", "type": "float"}]})


def test_a_consumer_resolves_a_version_it_never_registered(client, service):
    """A record stamped `__version: 1` is unreadable to a consumer that has only
    seen version 2, unless it can ask someone. That is the reason the version
    travels with the record."""
    client.register("trade", {"fields": BASE})
    client.register("trade", {"fields": BASE + [
        {"name": "venue", "type": "string", "required": False, "default": "x"}]})

    consumer = rs.RegistryClient(base=service)
    v1 = consumer.version("trade", 1)
    assert [f["name"] for f in v1["fields"]] == ["symbol", "price", "size"]
    assert "venue" not in [f["name"] for f in v1["fields"]]


def test_an_unknown_version_is_404_and_not_a_fallback_to_latest(client):
    """Reading history under the wrong schema is worse than refusing to read
    it, so the service must not quietly serve `latest`."""
    client.register("trade", {"fields": BASE})
    with pytest.raises(rs.RegistryClient.NotFound):
        client.version("trade", 99)


def test_a_404_is_not_reported_as_the_registry_being_down(client):
    """A REAL BUG THIS HAD. `HTTPError` subclasses `URLError`, so a 404
    "version not found" was caught by the unreachable branch and reported as
    "registry unreachable" -- an answer being reported as an outage.

    Under fail_mode="cache" it was worse than wrong: it would have served a
    STALE schema in answer to a version the registry says does not exist, which
    is exactly what the 404 message forbids.
    """
    client.register("trade", {"fields": BASE})
    with pytest.raises(rs.RegistryClient.NotFound):
        client.version("trade", 99)
    # And specifically NOT the outage exception.
    try:
        client.version("trade", 99)
    except rs.RegistryClient.Unavailable:                    # pragma: no cover
        pytest.fail("a 404 was reported as the registry being unreachable")
    except rs.RegistryClient.NotFound:
        pass


def test_a_cached_client_does_not_paper_over_a_404():
    """The same failure with the dangerous flag set."""
    c = rs.RegistryClient(base="http://127.0.0.1:9", fail_mode="cache")
    c._cache[("trade", "99")] = {"fields": [], "version": 99}
    # Nothing is listening on port 9 -> genuinely unavailable -> cache is used.
    assert c.version("trade", 99)["version"] == 99
    assert c.served_from_cache == 1


# ---------------------------------------------- it is now a DEPENDENCY
def test_an_unreachable_registry_refuses_by_default():
    """The question an in-process registry cannot pose. `refuse` is right when
    an unvalidated record is worse than no record -- which is the case for
    anything a consumer computes money from."""
    c = rs.RegistryClient(base="http://127.0.0.1:9", timeout=0.5)
    assert c.fail_mode == "refuse"
    with pytest.raises(rs.RegistryClient.Unavailable):
        c.version("trade", 1)


def test_cache_mode_keeps_producing_and_counts_it():
    """`cache` is right when the stream stopping is worse than a stale schema,
    and only safe because a schema changes far less often than a record. The
    count is what stops it being invisible."""
    c = rs.RegistryClient(base="http://127.0.0.1:9", fail_mode="cache",
                          timeout=0.5)
    c._cache[("trade", "1")] = {"version": 1, "fields": BASE}
    assert c.version("trade", 1)["version"] == 1
    assert c.served_from_cache == 1


def test_an_invalid_fail_mode_is_refused():
    """Not deciding is the actual error: a client that silently falls back to
    cache under a `refuse` expectation produces unvalidated records during
    exactly the incident nobody is watching."""
    with pytest.raises(ValueError, match="refuse | cache"):
        rs.RegistryClient(fail_mode="whatever")


# ------------------------------------------------------- the other paths
def test_subjects_and_versions_are_listed(client, service):
    client.register("trade", {"fields": BASE})
    client.register("quote", {"fields": BASE})
    assert _get(service, "/subjects")["subjects"] == ["quote", "trade"]
    assert _get(service, "/subjects/trade/versions")["versions"] == [1]


def test_latest_resolves(client, service):
    client.register("trade", {"fields": BASE})
    client.register("trade", {"fields": BASE + [
        {"name": "venue", "type": "string", "required": False, "default": "x"}]})
    assert _get(service, "/subjects/trade/versions/latest")["version"] == 2


def test_compatibility_can_be_dry_run_without_registering(client, service):
    """A producer should be able to ask "would this break anything?" without
    committing to it -- otherwise the only way to find out is to do it."""
    client.register("trade", {"fields": BASE})
    raw = json.dumps({"fields": [{"name": "symbol", "type": "string"},
                                 {"name": "px", "type": "float"}]}).encode()
    req = request.Request(
        service + "/compatibility/subjects/trade/versions/latest", data=raw,
        headers={"Content-Type": "application/json"}, method="POST")
    with request.urlopen(req, timeout=3) as r:
        out = json.loads(r.read())
    assert out["is_compatible"] is False and out["messages"]
    # And it did NOT register.
    assert _get(service, "/subjects/trade/versions")["versions"] == [1]


def test_the_subject_config_reports_its_compatibility_mode(client, service):
    client.register("trade", {"fields": BASE, "compatibility": "FULL"})
    assert _get(service, "/config/trade")["compatibilityLevel"] == "FULL"


def test_an_unknown_subject_is_404_not_an_empty_list(client, service):
    """An empty list says "this subject exists and has no versions", which is
    not the same as "no such subject" and sends a reader looking in the wrong
    place."""
    with pytest.raises(error.HTTPError) as exc:
        _get(service, "/subjects/nope/versions")
    assert exc.value.code == 404
