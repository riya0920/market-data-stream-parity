"""Capture a live Kraken session to disk for replay.

Usage: python record_session.py --seconds 45
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from src.feed import load_recording, record

DEFAULT = ROOT / "data" / "kraken_session.jsonl"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=float, default=45.0)
    ap.add_argument("--out", type=Path, default=DEFAULT)
    ap.add_argument("--symbols", nargs="*", default=["BTC/USD", "ETH/USD"])
    args = ap.parse_args()

    print("recording {} for {:.0f}s -> {}".format(
        ", ".join(args.symbols), args.seconds, args.out))
    stats = asyncio.run(record(args.out, args.symbols, args.seconds))

    print("\ncaptured {:,} ticks, {} heartbeat/timeout events".format(
        stats["ticks"], stats["heartbeats"]))
    if stats["ingest_lag_p50_ms"] is not None:
        print("ingest lag p50 / p99 : {}ms / {}ms".format(
            stats["ingest_lag_p50_ms"], stats["ingest_lag_p99_ms"]))
        print("\nThat lag is exchange timestamp to our receive time: network path,")
        print("venue-side batching, and clock offset between us and Kraken all")
        print("fold into it. It CANNOT be reconstructed after the fact, which is")
        print("why it is captured at record time rather than derived later.")

    ticks = load_recording(args.out)
    by_symbol = {}
    for t in ticks:
        by_symbol[t.symbol] = by_symbol.get(t.symbol, 0) + 1
    print("\nper symbol: {}".format(by_symbol))
    if ticks:
        span = (ticks[-1].event_time_ms - ticks[0].event_time_ms) / 1000
        print("event-time span: {:.1f}s".format(span))
    print("\nThe live connection's job is to CAPTURE. Every downstream test runs")
    print("against this file, because a live feed is unrepeatable and a result")
    print("that cannot be re-derived is not a correctness claim.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
