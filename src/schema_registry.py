"""A schema registry, and the compatibility rules that make one worth having.

`src/log.py` stores JSON with no schema, which means a producer that renames a
field breaks every consumer silently: the record still parses, the field is
simply absent, and downstream code sees `None` where a price used to be. Nothing
raises. The bar is computed from a missing value and published.

That is the failure a registry prevents, and it prevents it by REFUSING THE
PRODUCER, not by patching the consumer. The check happens at registration time,
before a single bad record is written -- which is the only place it can be
cheap. Once the record is in the log, every consumer has to cope with it forever.

THE FOUR COMPATIBILITY MODES, and which one you want depends on who you cannot
redeploy:

  BACKWARD    a NEW reader can read data written under the OLD schema.
              Allows: deleting a field, adding an OPTIONAL field.
              Want this when you upgrade consumers first -- the usual case,
              because a consumer that cannot read history cannot replay.

  FORWARD     an OLD reader can read data written under the NEW schema.
              Allows: adding a field, deleting an OPTIONAL one.
              Want this when you upgrade producers first, or when you cannot
              upgrade some consumer at all.

  FULL        both. Only optional fields may be added or removed.

  NONE        anything goes, which is what this project had.

THE ASYMMETRY THAT CATCHES PEOPLE. Adding a required field is FORWARD compatible
and NOT backward compatible; deleting a required field is the reverse. So "add a
field" is safe or unsafe depending entirely on which direction you need, and a
registry set to the wrong mode is worse than none because it issues an approval.

TYPE WIDENING is directional. `int -> float` is fine for a reader expecting a
float and breaks a reader expecting an int, so it is backward compatible and not
forward compatible.

AND HERE IS THE LIMIT OF THE WHOLE IDEA. Narrowing `float -> int` PASSES the
forward check, and the check is right: an old reader expecting a float accepts
an integer without complaint, so nothing about the representation breaks. What
breaks is the value -- every price is truncated at the producer. Compatibility is
an algebra over TYPES; truncation is a statement about MEANING, and no amount of
schema checking reaches it. A registry tells you whether a change will break a
reader. It cannot tell you whether the data is still true, and treating a green
compatibility check as a review is the mistake it most reliably enables.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field

WIDENINGS = {("int", "float"), ("int", "number"), ("float", "number")}


class IncompatibleSchema(Exception):
    """Raised at REGISTRATION time, which is the only cheap place to raise."""


@dataclass
class Field:
    name: str
    type: str
    required: bool = True
    default: object = None


@dataclass
class Schema:
    subject: str
    version: int
    fields: list
    compatibility: str = "BACKWARD"

    @property
    def by_name(self) -> dict:
        return {f.name: f for f in self.fields}

    def fingerprint(self) -> str:
        return json.dumps(
            [[f.name, f.type, f.required] for f in sorted(
                self.fields, key=lambda f: f.name)], sort_keys=True)

    def validate(self, record: dict) -> list:
        """Problems with one record, as a list. Empty means it conforms."""
        problems = []
        for f in self.fields:
            if f.name not in record or record[f.name] is None:
                if f.required and f.default is None:
                    problems.append("missing required field {!r}".format(f.name))
                continue
            if not _type_ok(record[f.name], f.type):
                problems.append("field {!r} is {}, expected {}".format(
                    f.name, type(record[f.name]).__name__, f.type))
        return problems


def _type_ok(value, declared: str) -> bool:
    if declared in ("number", "float"):
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if declared == "int":
        return isinstance(value, int) and not isinstance(value, bool)
    if declared == "string":
        return isinstance(value, str)
    if declared == "bool":
        return isinstance(value, bool)
    return True


# ------------------------------------------------------------- compatibility
def _backward_problems(old: Schema, new: Schema) -> list:
    """Can a NEW reader read OLD data?"""
    problems = []
    for name, f in new.by_name.items():
        prev = old.by_name.get(name)
        if prev is None:
            if f.required and f.default is None:
                problems.append(
                    "added required field {!r} with no default: old records do "
                    "not carry it".format(name))
            continue
        if prev.type != f.type and (prev.type, f.type) not in WIDENINGS:
            problems.append("field {!r} changed type {} -> {}".format(
                name, prev.type, f.type))
        if f.required and not prev.required:
            problems.append(
                "field {!r} became required: old records may omit it".format(name))
    return problems


def _forward_problems(old: Schema, new: Schema) -> list:
    """Can an OLD reader read NEW data? The mirror image."""
    problems = []
    for name, f in old.by_name.items():
        nxt = new.by_name.get(name)
        if nxt is None:
            if f.required and f.default is None:
                problems.append(
                    "removed required field {!r}: old readers still expect it"
                    .format(name))
            continue
        if f.type != nxt.type and (nxt.type, f.type) not in WIDENINGS:
            problems.append("field {!r} changed type {} -> {}".format(
                name, f.type, nxt.type))
    return problems


def check_compatibility(old: Schema, new: Schema, mode: str) -> list:
    mode = mode.upper()
    if mode == "NONE":
        return []
    if mode == "BACKWARD":
        return _backward_problems(old, new)
    if mode == "FORWARD":
        return _forward_problems(old, new)
    if mode == "FULL":
        return _backward_problems(old, new) + _forward_problems(old, new)
    raise ValueError("unknown compatibility mode: {!r}".format(mode))


@dataclass
class Registry:
    subjects: dict = field(default_factory=dict)

    def register(self, schema: Schema, compatibility: str | None = None) -> int:
        """Register a new version. Raises rather than accepting a breaking change.

        The mode comes from the CURRENT schema unless overridden, so a producer
        cannot loosen the rule in the same request that breaks it -- which is
        exactly what a producer under deadline pressure will try.
        """
        history = self.subjects.setdefault(schema.subject, [])
        if not history:
            schema.version = 1
            history.append(schema)
            return 1

        latest = history[-1]
        mode = compatibility or latest.compatibility
        problems = check_compatibility(latest, schema, mode)
        if problems:
            raise IncompatibleSchema(
                "{} v{} -> v{} violates {} compatibility: {}".format(
                    schema.subject, latest.version, latest.version + 1, mode,
                    "; ".join(problems)))
        schema.version = latest.version + 1
        schema.compatibility = mode
        history.append(schema)
        return schema.version

    def latest(self, subject: str) -> Schema:
        return self.subjects[subject][-1]

    def get(self, subject: str, version: int) -> Schema:
        for s in self.subjects[subject]:
            if s.version == version:
                return s
        raise KeyError("{} v{}".format(subject, version))

    def envelope(self, subject: str, record: dict) -> dict:
        """Stamp a record with the schema version that produced it.

        Without the stamp a consumer has to GUESS which schema a record was
        written under, and the usual guess is "the latest", which is wrong for
        every record in the archive written before the last change. A replay is
        the moment that guess costs something.
        """
        schema = self.latest(subject)
        problems = schema.validate(record)
        if problems:
            raise IncompatibleSchema(
                "record does not conform to {} v{}: {}".format(
                    subject, schema.version, "; ".join(problems)))
        return {"__subject": subject, "__version": schema.version, **record}

    def read(self, envelope: dict) -> tuple:
        """(record, schema it was written under). Reads history correctly."""
        subject = envelope.get("__subject")
        version = envelope.get("__version")
        if subject is None or version is None:
            return ({k: v for k, v in envelope.items()
                     if not k.startswith("__")}, None)
        return ({k: v for k, v in envelope.items()
                 if not k.startswith("__")}, self.get(subject, version))
