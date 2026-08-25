"""The schema registry as a SERVICE, which is what makes it a registry.

    python registry_service.py            # serve on 127.0.0.1:8081
    python registry_service.py --check    # start, exercise, report, exit

`README.md` listed this open: "A registry SERVICE. Confluent's is a service --
producers and consumers [share one over the network]." `src/schema_registry.py`
is an in-process object, and an in-process registry is not a registry: it is a
dictionary that each process gets its own copy of.

WHAT CHANGES WHEN IT CROSSES A NETWORK, and every one of these is the reason the
distinction matters rather than a detail of transport:

  ONE ARBITER OF COMPATIBILITY. In-process, two producers can hold contradictory
  ideas of what version 3 is and both believe they are right. A service is the
  single thing that says no -- and it can only say no to the second one if it
  saw the first.

  THE CONSUMER CAN RESOLVE A VERSION IT NEVER REGISTERED. A record stamped
  `__version: 2` is unreadable to a consumer that has only ever seen version 3,
  unless it can ask someone. That is the whole reason the version travels with
  the record, and in-process the answer is unavailable to anyone but the writer.

  IT IS A DEPENDENCY, WITH A DEPENDENCY'S FAILURE MODES. Down, slow, or
  partitioned. The in-process version cannot be any of those, which is precisely
  why building on it hides the question of what a producer does when the
  registry is unreachable -- see `/subjects` handling below and
  `RegistryClient.fail_mode`.

DELIBERATELY CONFLUENT-SHAPED, on the paths that matter:
  GET  /subjects
  GET  /subjects/{s}/versions
  GET  /subjects/{s}/versions/{v}          ({v} may be `latest`)
  POST /subjects/{s}/versions              register, returns {"id": n}
  POST /compatibility/subjects/{s}/versions/{v}   dry-run a change
  GET  /config/{s}                         the subject's compatibility mode

Not because compatibility with Confluent is claimed -- it is not, and the wire
format for Avro ids differs -- but because a reader who knows that API can see
at a glance what this does and does not implement.
"""
from __future__ import annotations

import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib import error, request

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from src.schema_registry import (Field, IncompatibleSchema, Registry, Schema,
                                 check_compatibility)

HOST, PORT = "127.0.0.1", 8081

# One registry, shared by every request, behind a lock. The lock is the point:
# `register` is a read-modify-write on the subject's version list, and two
# producers registering at once is the exact race a SERVICE exists to arbitrate.
# Getting this wrong would make the service a slower version of the in-process
# object rather than a stronger one.
_REG = Registry()
_LOCK = threading.Lock()


def _schema_from_json(subject: str, body: dict) -> Schema:
    fields = [Field(f["name"], f["type"], f.get("required", True),
                    f.get("default"))
              for f in body.get("fields", [])]
    return Schema(subject, body.get("version", 0), fields,
                  compatibility=body.get("compatibility", "BACKWARD"))


def _schema_to_json(s: Schema) -> dict:
    return {
        "subject": s.subject, "version": s.version,
        "compatibility": s.compatibility,
        "fields": [{"name": f.name, "type": f.type, "required": f.required,
                    "default": f.default} for f in s.fields],
    }


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_a):            # quiet; the caller reports
        pass

    def _send(self, code: int, payload: dict) -> None:
        raw = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    # ------------------------------------------------------------- reads
    def do_GET(self) -> None:
        parts = [p for p in self.path.split("?")[0].split("/") if p]
        try:
            if parts == ["subjects"]:
                with _LOCK:
                    return self._send(200, {"subjects": sorted(_REG.subjects)})

            if len(parts) == 3 and parts[0] == "subjects" and parts[2] == "versions":
                with _LOCK:
                    versions = [s.version for s in _REG.subjects.get(parts[1], [])]
                if not versions:
                    return self._send(404, {"error_code": 40401,
                                            "message": "subject not found"})
                return self._send(200, {"versions": versions})

            if len(parts) == 4 and parts[0] == "subjects" and parts[2] == "versions":
                subject, want = parts[1], parts[3]
                with _LOCK:
                    known = _REG.subjects.get(subject, [])
                    if not known:
                        return self._send(404, {"error_code": 40401,
                                                "message": "subject not found"})
                    if want == "latest":
                        s = known[-1]
                    else:
                        match = [x for x in known if x.version == int(want)]
                        if not match:
                            # THE FAILURE THIS SERVICE EXISTS TO PREVENT: a
                            # consumer holding a record stamped with a version
                            # nobody can resolve. 404 rather than falling back
                            # to latest -- reading history under the wrong
                            # schema is worse than refusing to read it.
                            return self._send(404, {
                                "error_code": 40402,
                                "message": "version {} not found for {}. NOT "
                                           "falling back to latest: reading a "
                                           "record under a schema that did not "
                                           "write it is worse than refusing "
                                           "it.".format(want, subject)})
                        s = match[0]
                return self._send(200, _schema_to_json(s))

            if len(parts) == 2 and parts[0] == "config":
                with _LOCK:
                    known = _REG.subjects.get(parts[1], [])
                if not known:
                    return self._send(404, {"error_code": 40401,
                                            "message": "subject not found"})
                return self._send(200, {"compatibilityLevel": known[-1].compatibility})

            return self._send(404, {"message": "no such path"})
        except Exception as exc:                             # noqa: BLE001
            return self._send(500, {"message": str(exc)})

    # ------------------------------------------------------------ writes
    def do_POST(self) -> None:
        n = int(self.headers.get("Content-Length", 0))
        try:
            body = json.loads(self.rfile.read(n) or b"{}")
        except json.JSONDecodeError as exc:
            return self._send(400, {"message": "bad json: {}".format(exc)})

        parts = [p for p in self.path.split("?")[0].split("/") if p]

        # POST /subjects/{s}/versions  -- register
        if len(parts) == 3 and parts[0] == "subjects" and parts[2] == "versions":
            subject = parts[1]
            try:
                with _LOCK:
                    version = _REG.register(_schema_from_json(subject, body))
                return self._send(200, {"id": version, "version": version})
            except IncompatibleSchema as exc:
                # 409, the same code Confluent uses, because this is a conflict
                # with existing state rather than a malformed request.
                return self._send(409, {"error_code": 409,
                                        "message": str(exc)})

        # POST /compatibility/subjects/{s}/versions/{v}  -- dry run
        if (len(parts) == 5 and parts[0] == "compatibility"
                and parts[1] == "subjects" and parts[3] == "versions"):
            subject = parts[2]
            with _LOCK:
                known = _REG.subjects.get(subject, [])
                if not known:
                    return self._send(404, {"error_code": 40401,
                                            "message": "subject not found"})
                current = known[-1]
                proposed = _schema_from_json(subject, body)
                problems = check_compatibility(current, proposed,
                                               current.compatibility)
            return self._send(200, {"is_compatible": not problems,
                                    "messages": problems})

        return self._send(404, {"message": "no such path"})


def serve(host: str = HOST, port: int = PORT) -> ThreadingHTTPServer:
    return ThreadingHTTPServer((host, port), Handler)


class RegistryClient:
    """What a producer or consumer holds instead of the Registry object.

    `fail_mode` is the question an in-process registry cannot pose:

        "refuse"  a producer that cannot reach the registry does not produce.
                  Correct when an unvalidated record is worse than no record --
                  which is the case for anything a consumer will compute money
                  from.
        "cache"   serve the last known schema and keep producing. Correct when
                  the stream stopping is worse than a stale schema, and only
                  safe because a schema changes far less often than a record.

    Neither is right in general, which is why it is a parameter. What IS wrong
    is not deciding: a client that silently falls back to cache under a
    "refuse" expectation produces unvalidated records during exactly the
    incident nobody is watching.
    """

    def __init__(self, base: str = "http://{}:{}".format(HOST, PORT),
                 fail_mode: str = "refuse", timeout: float = 2.0):
        if fail_mode not in ("refuse", "cache"):
            raise ValueError("fail_mode must be refuse | cache")
        self.base = base.rstrip("/")
        self.fail_mode = fail_mode
        self.timeout = timeout
        self._cache: dict = {}
        self.served_from_cache = 0

    class Unavailable(Exception):
        pass

    def _get(self, path: str) -> dict:
        req = request.Request(self.base + path,
                              headers={"Accept": "application/json"})
        with request.urlopen(req, timeout=self.timeout) as r:
            return json.loads(r.read())

    def register(self, subject: str, schema: dict) -> int:
        raw = json.dumps(schema).encode()
        req = request.Request(
            "{}/subjects/{}/versions".format(self.base, subject), data=raw,
            headers={"Content-Type": "application/json"}, method="POST")
        try:
            with request.urlopen(req, timeout=self.timeout) as r:
                return json.loads(r.read())["version"]
        except error.HTTPError as exc:
            if exc.code == 409:
                raise IncompatibleSchema(
                    json.loads(exc.read()).get("message", "incompatible")) from exc
            raise

    class NotFound(Exception):
        """The registry answered, and the answer is that this does not exist."""

    def version(self, subject: str, version) -> dict:
        key = (subject, str(version))
        try:
            out = self._get("/subjects/{}/versions/{}".format(subject, version))
            self._cache[key] = out
            return out
        except error.HTTPError as exc:
            # A RESPONSE IS NOT AN OUTAGE, and conflating them was a real bug
            # here: `HTTPError` subclasses `URLError`, so a 404 "version not
            # found" was caught by the unreachable branch and reported as
            # "registry unreachable".
            #
            # Under fail_mode="cache" that was worse than wrong -- it would have
            # served a STALE schema in answer to a version the registry says
            # does not exist, which is precisely the "reading a record under a
            # schema that did not write it" the 404 message forbids.
            raise RegistryClient.NotFound(
                "registry answered {} for {} v{}: {}".format(
                    exc.code, subject, version,
                    exc.read().decode(errors="replace")[:200])) from exc
        except (error.URLError, OSError) as exc:
            # Connection-level only: nothing answered.
            if self.fail_mode == "cache" and key in self._cache:
                self.served_from_cache += 1
                return self._cache[key]
            raise RegistryClient.Unavailable(
                "registry unreachable and fail_mode={}: {}".format(
                    self.fail_mode, exc)) from exc
