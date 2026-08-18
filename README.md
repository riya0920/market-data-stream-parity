# DATA-3 — Real-Time Market Data Pipeline

**Status: ~20% slice.** The correctness core — replay harness, event-time bars
with watermarks, emit-and-revise, quality flags, and the streaming-vs-batch
parity proof — is built and passing. **There is no Kafka, no Flink, no
TimescaleDB and no Grafana**; this is the algorithmic layer those systems would
host, running in-process so it can be verified.

```bash
python run_parity.py
```

## Why in-process, and what that costs

The parity proof, the watermark semantics, and the revise-vs-drop decision are
where market-data pipelines are actually right or wrong, and none of them need a
broker to be demonstrated. What *is* lost by not having one: real backpressure,
partition ordering, checkpoint/restart semantics, consumer-lag metrics, and any
credible latency number. So no p95 bar-emit latency is quoted here — a
single-process number would be measuring Python, not the system.

## What is built

**Replay harness with injected disorder** (`src/replay.py`) — out-of-order ticks
(≤12s), duplicates, feed gaps. Ground truth is the clean tick list, so everything
downstream is scored rather than eyeballed. A live WebSocket is unrepeatable and
therefore cannot support a correctness claim.

**Event-time bars with watermarks** (`src/bars.py`). The watermark bound (5s) is
stated as a design decision, not a constant: too small drops real data, too large
means no bar is ever trustworthy.

**Emit-and-revise with consumer-visible finality.** Bars carry `is_final`,
`revision`, and `suspect`. Late ticks inside the bound revise the bar; beyond the
bound they go to a late-events table and mark the bar suspect — they do **not**
retroactively change a number consumers have already acted on.

**Quality flags on the data**, not beside it. Bars adjacent to a detected gap are
flagged, so a consumer can distinguish "quiet market" from "we were blind."

## Results (current run: 100k ticks, 4.15 hours, 247 bars)

```
duplicates suppressed     : 967
late, within bound        : 124  -> bar revised
late, beyond bound        : 68   -> late-events table, bar marked suspect
gaps detected             : 3
bars flagged suspect      : 61
throughput                : ~95,000 ticks/sec (single-threaded CPython, Win11 laptop)
```

**Parity A — streaming vs batch over the same ticks:**

```
mismatched bars                                  : 54
mismatched, explained by beyond-bound late ticks : 54
mismatched, UNEXPLAINED                          : 0
VERDICT: EXACT except 54 documented late-arrival exceptions, all flagged suspect
```

The claim is deliberately not "zero mismatches". Batch sees every tick at once,
so it *will* disagree on bars where streaming refused a beyond-bound revision —
and that refusal is the correct behaviour. The provable claim is: **every
mismatch is one of the documented exceptions, and every one is flagged suspect on
the data.** Unexplained mismatches would mean the streaming path is wrong, and
that count is 0.

**Parity B — streaming vs batch over the *original* ticks** (including what the
feed gaps swallowed) must *not* be exact: 61 affected bars, 58 present and
flagged, 3 absent entirely so consumers see a hole rather than a wrong number.
Reporting "100% parity" by quietly choosing the surviving-ticks baseline would be
picking the convenient comparison.

**Rebuild drill:** 59 bars recomputed from the archive in 0.030s, 0 differing.
The window must be bar-aligned — rebuilding an arbitrary timespan re-derives the
edge bars from a partial tick population and produces two wrong bars that look
exactly like a parity bug. That's the real trap in a reprocessing runbook, and it
bit this build before the alignment was made explicit.

## What is NOT built (the other 80%)

1. **The entire infrastructure**: Kafka, Flink/Spark Structured Streaming,
   TimescaleDB/ClickHouse serving, Parquet archive, Grafana. In-process only.
2. **A real feed.** No crypto WebSocket client; ticks are generated.
3. **Latency metrics** — no ingest lag, no bar-emit p95, no histogram export. The
   spec asks for these and they are absent rather than faked.
4. **Multi-symbol.** One instrument, one watermark. Per-key watermarks and the
   slow-partition problem are untouched.
5. **Derived analytics**: rolling VWAP, realised volatility, σ-jump anomaly flags
   — none implemented, so the in-stream vs batch spot-check of those is missing.
6. **Alerting and the runbook**: gap alerts fire nowhere; the rebuild is a script
   in `run_parity.py`, not an operational tool with a documented procedure.
7. **Replay speed control** (1x/10x/100x) — replay is as-fast-as-possible only,
   so the "sustained ticks/sec at 100x" figure the spec asks for is not
   measurable here.
8. Heartbeat-loss detection distinct from data gaps.
