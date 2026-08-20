"""Live market-data feed with a recorder.

Kraken's public WebSocket, chosen for reasons that are part of the design rather
than convenience: it is free, needs no key, runs 24/7 (so a test at 3am on a
Sunday still has data), and carries real microstructure -- genuine out-of-order
arrivals, genuine bursts, genuine quiet periods. Binance was tried first and
returns HTTP 451 from this location, which is itself a useful reminder that a
feed's availability is part of its risk profile.

**The recorder is the point, not the live connection.** A live feed cannot
support a correctness claim: it is unrepeatable, so a result computed from it can
never be re-derived and a regression can never be reproduced. So the connection's
job is to CAPTURE a session to disk, after which every downstream test runs
against a fixed file. That is what makes `run_parity.py` a proof rather than an
anecdote.

Recorded fields, and why each:
  event_time_ms   the exchange's own timestamp. Bars are built on this.
  recv_time_ms    when WE received it. The difference is ingest lag, and it
                  cannot be reconstructed later if it is not captured now.
  seq             exchange trade id, used for duplicate suppression and for
                  ordering within a millisecond.
  price/qty       integer minor units. Kraken sends decimal strings, and they are
                  parsed with Decimal rather than float -- binary floating point
                  cannot represent 0.1, and a price that fails to round-trip is a
                  reconciliation break waiting to happen.
"""
from __future__ import annotations

import asyncio
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path

KRAKEN_WS = "wss://ws.kraken.com/v2"
PRICE_SCALE = 100          # USD cents
QTY_SCALE = 100_000_000    # satoshi-like, 8dp


@dataclass(frozen=True)
class LiveTick:
    seq: int
    event_time_ms: int
    recv_time_ms: int
    price_minor: int
    size: int
    symbol: str
    side: str

    @property
    def ingest_lag_ms(self) -> int:
        return self.recv_time_ms - self.event_time_ms


def _parse_ts(iso: str) -> int:
    # Kraken sends RFC3339 with microseconds and a Z suffix.
    return int(datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp() * 1000)


def parse_trade(msg: dict, recv_time_ms: int) -> list[LiveTick]:
    if msg.get("channel") != "trade" or msg.get("type") not in ("update", "snapshot"):
        return []
    out = []
    for d in msg.get("data", []):
        out.append(LiveTick(
            seq=int(d["trade_id"]),
            event_time_ms=_parse_ts(d["timestamp"]),
            recv_time_ms=recv_time_ms,
            # Decimal, never float: 0.1 has no exact binary representation and a
            # price that does not round-trip becomes a break nobody can explain.
            price_minor=int((Decimal(str(d["price"])) * PRICE_SCALE)
                            .quantize(Decimal(1))),
            size=int((Decimal(str(d["qty"])) * QTY_SCALE).quantize(Decimal(1))),
            symbol=d["symbol"],
            side=d["side"],
        ))
    return out


async def record(path: Path, symbols: list[str] | None = None,
                 duration_s: float = 60.0, max_ticks: int | None = None) -> dict:
    """Capture a live session to JSONL. Returns capture statistics."""
    import websockets

    symbols = symbols or ["BTC/USD", "ETH/USD"]
    path.parent.mkdir(parents=True, exist_ok=True)
    ticks = 0
    heartbeats = 0
    started = asyncio.get_event_loop().time()
    lags: list[int] = []

    async with websockets.connect(KRAKEN_WS, open_timeout=15) as ws:
        await ws.send(json.dumps({
            "method": "subscribe",
            "params": {"channel": "trade", "symbol": symbols},
        }))
        with path.open("w", encoding="utf-8") as fh:
            while asyncio.get_event_loop().time() - started < duration_s:
                if max_ticks and ticks >= max_ticks:
                    break
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=20)
                except asyncio.TimeoutError:
                    # Silence beyond the timeout is exactly the feed-loss case
                    # ops cares about; it is recorded, not swallowed.
                    heartbeats += 1
                    continue
                recv_ms = int(asyncio.get_event_loop().time() * 1000)
                wall_ms = int(datetime.now().timestamp() * 1000)
                msg = json.loads(raw)
                if msg.get("channel") == "heartbeat":
                    heartbeats += 1
                    continue
                for t in parse_trade(msg, wall_ms):
                    fh.write(json.dumps(asdict(t)) + "\n")
                    lags.append(t.ingest_lag_ms)
                    ticks += 1
                del recv_ms

    lags_sorted = sorted(lags)
    return {
        "path": str(path),
        "ticks": ticks,
        "heartbeats": heartbeats,
        "symbols": symbols,
        "duration_s": duration_s,
        "ingest_lag_p50_ms": lags_sorted[len(lags_sorted) // 2] if lags else None,
        "ingest_lag_p99_ms": (lags_sorted[int(len(lags_sorted) * 0.99)]
                              if lags else None),
    }


def load_recording(path: Path) -> list[LiveTick]:
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            out.append(LiveTick(**json.loads(line)))
    return out


def to_engine_ticks(live: list[LiveTick], symbol: str | None = None):
    """Adapt recorded ticks to the Tick shape the bar builder consumes.

    The adapter exists so the correctness layer never depends on one venue's
    message format. Swapping Kraken for Coinbase is a parser change here and
    nothing downstream moves.
    """
    from .replay import Tick

    rows = [t for t in live if symbol is None or t.symbol == symbol]
    rows.sort(key=lambda t: (t.event_time_ms, t.seq))
    return [Tick(seq=t.seq, event_time_ms=t.event_time_ms,
                 price_minor=t.price_minor, size=t.size) for t in rows]
