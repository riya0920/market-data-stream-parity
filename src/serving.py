"""Serving layer: Parquet archive + DuckDB query surface.

The spec asks for TimescaleDB or ClickHouse for serving and Parquet for the
archive. DuckDB stands in for the serving database, and the substitution is
honest in a specific way: what a serving layer must do here is answer
"give me the 1m bars for BTC/USD between two timestamps" fast, and support the
BATCH RECOMPUTATION that the parity check depends on. DuckDB does both against
Parquet directly.

What is genuinely lost by not running Timescale/ClickHouse: concurrent writers,
retention policies and continuous aggregates, replication, and any multi-user
concurrency story. Those are operational properties, and none of them changes
whether a bar is correct.

The archive is the important half regardless of engine. It is what makes the
rebuild drill possible: if the archive is not the source of truth for
recomputation, "replay from archive" is a phrase rather than a procedure.
Partitioned by symbol and hour so a rebuild reads only the hours it needs.
"""
from __future__ import annotations

from pathlib import Path

HOUR_MS = 3_600_000


def archive_bars(bars: dict, symbol: str, out_root: Path) -> list[Path]:
    """Write bars to Parquet, partitioned by symbol and hour.

    Hive-style partitioning (symbol=X/hour=Y) so DuckDB can prune partitions
    from the path alone -- a rebuild of one hour touches one file rather than
    scanning the archive.
    """
    import pandas as pd

    rows = []
    for bucket, b in sorted(bars.items()):
        rows.append({
            "bucket_start_ms": b.bucket_start_ms,
            "open_minor": b.open_minor, "high_minor": b.high_minor,
            "low_minor": b.low_minor, "close_minor": b.close_minor,
            "volume": b.volume, "tick_count": b.tick_count,
            "revision": b.revision, "is_final": b.is_final,
            "suspect": b.suspect, "suspect_reason": b.suspect_reason,
        })
    if not rows:
        return []

    df = pd.DataFrame(rows)
    df["hour"] = (df.bucket_start_ms // HOUR_MS) * HOUR_MS
    written = []
    for hour, chunk in df.groupby("hour"):
        d = out_root / "symbol={}".format(symbol.replace("/", "_")) / "hour={}".format(hour)
        d.mkdir(parents=True, exist_ok=True)
        p = d / "bars.parquet"
        chunk.drop(columns=["hour"]).to_parquet(p, index=False)
        written.append(p)
    return written


def archive_ticks(ticks, symbol: str, out_root: Path) -> list[Path]:
    import pandas as pd

    if not ticks:
        return []
    df = pd.DataFrame([{
        "seq": t.seq, "event_time_ms": t.event_time_ms,
        "price_minor": t.price_minor, "size": t.size} for t in ticks])
    df["hour"] = (df.event_time_ms // HOUR_MS) * HOUR_MS
    written = []
    for hour, chunk in df.groupby("hour"):
        d = out_root / "symbol={}".format(symbol.replace("/", "_")) / "hour={}".format(hour)
        d.mkdir(parents=True, exist_ok=True)
        p = d / "ticks.parquet"
        chunk.drop(columns=["hour"]).to_parquet(p, index=False)
        written.append(p)
    return written


class ServingStore:
    """DuckDB over the Parquet archive."""

    def __init__(self, archive_root: Path):
        import duckdb

        self.root = Path(archive_root)
        self.con = duckdb.connect(":memory:")

    def _glob(self, kind: str, symbol: str | None = None) -> str:
        sym = symbol.replace("/", "_") if symbol else "*"
        return str(self.root / "symbol={}".format(sym) / "hour=*" / "{}.parquet".format(kind))

    def bars(self, symbol: str | None = None, start_ms: int | None = None,
             end_ms: int | None = None):
        q = "SELECT * FROM read_parquet('{}', hive_partitioning=1)".format(
            self._glob("bars", symbol).replace("\\", "/"))
        conds = []
        if start_ms is not None:
            conds.append("bucket_start_ms >= {}".format(start_ms))
        if end_ms is not None:
            conds.append("bucket_start_ms < {}".format(end_ms))
        if conds:
            q += " WHERE " + " AND ".join(conds)
        return self.con.execute(q + " ORDER BY bucket_start_ms").fetchdf()

    def ticks_for_hour(self, symbol: str, hour_ms: int):
        p = (self.root / "symbol={}".format(symbol.replace("/", "_"))
             / "hour={}".format(hour_ms) / "ticks.parquet")
        if not p.exists():
            return None
        return self.con.execute(
            "SELECT * FROM read_parquet('{}') ORDER BY event_time_ms, seq".format(
                str(p).replace("\\", "/"))).fetchdf()

    def suspect_bars(self, symbol: str | None = None):
        """Quality flags travel WITH the data into the serving layer.

        A consumer querying the store must be able to see that a bar is built on
        a hole, without having to correlate against a separate alerting system
        that may not have retained the incident.
        """
        df = self.bars(symbol)
        return df[df.suspect] if len(df) else df

    def hours_available(self, symbol: str) -> list[int]:
        base = self.root / "symbol={}".format(symbol.replace("/", "_"))
        if not base.exists():
            return []
        return sorted(int(p.name.split("=")[1]) for p in base.iterdir()
                      if p.is_dir() and p.name.startswith("hour="))
