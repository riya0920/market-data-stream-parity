# DATA-3 — Real-Time Market Data Pipeline

**Status: ~45%.** The correctness core — replay harness, event-time bars with
watermarks, emit-and-revise, quality flags, the streaming-vs-batch parity proof,
derived analytics with a reorder buffer, and per-key watermarks — is built and
tested (8 tests). **There is still no Kafka, no Flink, no TimescaleDB and no
Grafana**; this is the algorithmic layer those systems would host, running
in-process so it can be verified.

```bash
python run_parity.py      # bar parity, gap flags, rebuild drill
python run_analytics.py   # VWAP/vol/jump parity, per-key watermarks, emit latency
python -m pytest tests -q
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

## Derived analytics, and the bug they exposed

`python run_analytics.py` computes VWAP, realised volatility and σ-jump flags
in-stream and against batch. The first version folded arrivals in **arrival
order**, and the parity table said this:

| metric | naive append | batch |
|---|---|---|
| VWAP | 504141.6478 | 504141.6478 |
| realised vol | 0.017419 | 0.010954 |
| jump flags | **812** | **2** |

VWAP ties because it is a ratio of two sums and does not care about order.
Realised vol and jump flags are functions of the *sequence* of returns, so a tick
folded in three seconds out of place fabricates two spurious returns — one
jumping back to it and one jumping forward again. **812 jump flags against 2 is
not a tolerance to document; it is a wrong number**, and an earlier draft of this
file called it "expected streaming/batch divergence" and moved on.

The fix is a reorder buffer (`stream_with_reorder`): hold ticks, release them in
event-time order once the watermark passes. Realised vol now lands within 0.03%
of batch and jump flags match exactly. The cost is exactly the watermark bound in
added latency — which is why that bound is a published parameter, not an
implementation detail.

## Per-key watermarks

One global watermark fails in both directions: `min()` lets one quiet symbol
freeze the whole board, `max()` lets one busy symbol finalise a quiet symbol's
bars before its ticks arrive. Measured here, an illiquid symbol that stops
trading would drift **166 minutes** behind; per-key watermarks with an idle
timeout hold it to exactly the configured 120s. The tradeoff is stated too: the
idle advance trades correctness for liveness, and any tick from that symbol more
than 120s late is now dropped.

Bar-emit latency: p50 2.5ms / p95 8.1ms, in-process on a laptop.

## What is NOT built

1. **The entire infrastructure**: Kafka, Flink/Spark Structured Streaming,
   TimescaleDB/ClickHouse serving, Parquet archive, Grafana. In-process only, so
   there is no backpressure, no checkpointing, no consumer-lag metric.
2. **A real feed.** No crypto WebSocket client; ticks are generated.
3. **Replay speed control** (1x/10x/100x) — replay runs as fast as possible, so
   the "sustained ticks/sec at 100x" figure the spec asks for is not measurable
   here and is not quoted.
4. **Alerting and the runbook**: gap detection produces flags and counts, but
   nothing pages anyone, and the rebuild is a script rather than a documented
   operational procedure.
5. **Heartbeat-loss detection** distinct from data gaps — a feed that is
   connected but silent is currently indistinguishable from a quiet market.
6. **True multi-symbol bar storage.** `MultiSymbolProcessor` tracks per-key
   watermarks and buffers, but bars are still built per symbol in memory with no
   partitioned serving layer.
7. **Ingest-lag metrics** (event time vs arrival time distribution), which is the
   number an ops team actually watches.
