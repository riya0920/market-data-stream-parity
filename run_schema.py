"""What a schema change costs, with and without a registry.

    python run_schema.py

The demonstration is the point: the same producer change is run twice, once
against the raw JSON log this project had and once against the registry, and the
difference is where the failure lands.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from src.schema_registry import (Field, IncompatibleSchema, Registry, Schema,
                                 check_compatibility)

V1 = Schema("trade", 1, [
    Field("symbol", "string"),
    Field("price", "float"),
    Field("size", "float"),
    Field("ts", "float"),
])

RENAMED = Schema("trade", 0, [
    Field("symbol", "string"),
    Field("px", "float"),                 # price renamed
    Field("size", "float"),
    Field("ts", "float"),
])

ADD_OPTIONAL = Schema("trade", 0, [
    Field("symbol", "string"),
    Field("price", "float"),
    Field("size", "float"),
    Field("ts", "float"),
    Field("venue", "string", required=False, default="unknown"),
])

ADD_REQUIRED = Schema("trade", 0, [
    Field("symbol", "string"),
    Field("price", "float"),
    Field("size", "float"),
    Field("ts", "float"),
    Field("venue", "string"),
])

NARROWED = Schema("trade", 0, [
    Field("symbol", "string"),
    Field("price", "int"),                # float -> int
    Field("size", "float"),
    Field("ts", "float"),
])


def _vwap(rows):
    num = sum(r.get("price", 0) * r.get("size", 0) for r in rows
              if r.get("price") is not None)
    den = sum(r.get("size", 0) for r in rows if r.get("price") is not None)
    return num / den if den else float("nan")


def main() -> int:
    print("=" * 78)
    print("SCHEMA EVOLUTION")
    print("=" * 78)

    good = [{"symbol": "BTC/USD", "price": 100.0 + i, "size": 1.0, "ts": float(i)}
            for i in range(10)]

    print("1. WITHOUT A REGISTRY -- the producer renames `price` to `px`")
    print("-" * 78)
    renamed_rows = [{"symbol": r["symbol"], "px": r["price"], "size": r["size"],
                     "ts": r["ts"]} for r in good]
    print("consumer VWAP over correct records : {:.4f}".format(_vwap(good)))
    print("consumer VWAP after the rename     : {}".format(_vwap(renamed_rows)))
    print("exceptions raised                  : 0")
    print()
    print("Nothing failed. The JSON still parses, `price` is simply absent, and")
    print("the consumer computes a number from a field that is not there. This")
    print("is the whole argument for a registry: the pipeline does not break, it")
    print("keeps running and publishes a wrong figure.")

    print("\n" + "=" * 78)
    print("2. WITH A REGISTRY -- the same change is refused at REGISTRATION")
    print("-" * 78)
    reg = Registry()
    reg.register(V1)
    print("registered trade v1 (compatibility BACKWARD)")
    try:
        reg.register(RENAMED)
        print("   rename ACCEPTED -- which would be a bug in this registry")
    except IncompatibleSchema as exc:
        print("   rename REFUSED:")
        for part in str(exc).split(": ", 1)[1].split("; "):
            print("      - {}".format(part))
    print()
    print("The refusal happens before a single bad record is written, which is")
    print("the only place it is cheap. Once the record is in the log every")
    print("consumer has to cope with it forever, including replays.")

    print("\n" + "=" * 78)
    print("3. THE ASYMMETRY THAT CATCHES PEOPLE")
    print("-" * 78)
    print("{:<34}{:>12}{:>12}{:>10}".format(
        "change", "BACKWARD", "FORWARD", "FULL"))
    for label, candidate in [
            ("add an OPTIONAL field", ADD_OPTIONAL),
            ("add a REQUIRED field", ADD_REQUIRED),
            ("rename a field", RENAMED),
            ("narrow float -> int", NARROWED)]:
        cells = []
        for mode in ("BACKWARD", "FORWARD", "FULL"):
            problems = check_compatibility(V1, candidate, mode)
            cells.append("ok" if not problems else "REFUSED")
        print("{:<34}{:>12}{:>12}{:>10}".format(label, *cells))

    print()
    print("Read row two. Adding a REQUIRED field is forward compatible and not")
    print("backward compatible -- an old reader ignores it, a new reader cannot")
    print("find it in the archive. So 'add a field' is safe or unsafe depending")
    print("entirely on which direction you need, and a registry set to the wrong")
    print("mode is worse than no registry, because it issues an approval.")
    print()
    print("Row four is the limit of what any compatibility check can do, and")
    print("it came out against what I expected to write. Narrowing float -> int")
    print("PASSES the forward check, and the check is right: an old reader")
    print("expecting a float accepts an integer without complaint. Nothing about")
    print("the representation breaks.")
    print()
    print("What breaks is the value. Every price is now truncated at the")
    print("producer and the registry has no way to know that, because")
    print("compatibility is an algebra over TYPES and truncation is a statement")
    print("about MEANING. A registry tells you whether a change will break a")
    print("reader; it cannot tell you whether the data is still true. Treating")
    print("a green compatibility check as a review is the mistake this row")
    print("exists to make visible.")

    print("\n" + "=" * 78)
    print("4. READING HISTORY")
    print("-" * 78)
    v2 = reg.register(ADD_OPTIONAL)
    print("registered trade v{} (added optional `venue`)".format(v2))
    old_env = {"__subject": "trade", "__version": 1, **good[0]}
    rec, schema = reg.read(old_env)
    print("a v1 record read back under v{} : {}".format(schema.version, rec))
    print()
    print("The version travels WITH the record. Without the stamp a consumer has")
    print("to guess which schema a record was written under, and the usual guess")
    print("is 'the latest' -- wrong for every record in the archive written")
    print("before the last change. A replay is when that guess costs something.")

    print("\n" + "=" * 78)
    print("5. WHAT THIS IS NOT")
    print("-" * 78)
    print("Confluent Schema Registry is a SERVICE: producers and consumers")
    print("resolve schemas over HTTP at runtime, ids are embedded in the wire")
    print("format, and Avro or Protobuf does the encoding. This is the")
    print("compatibility ALGEBRA and the version stamp, in process, over JSON.")
    print("It answers 'would this change break someone', which is the part that")
    print("is reasoning rather than infrastructure -- and it does not give you")
    print("a shared registry that two independently deployed services agree on,")
    print("which is the part that is infrastructure.")
    print("=" * 78)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
