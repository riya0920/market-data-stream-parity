# DATA-3 — Real-Time Market Data Pipeline

**Status: ~97%.** Runs on a **real Kraken feed**, through a durable partitioned
log with consumer groups and committed offsets, into a Parquet archive with a
DuckDB serving layer, with a rebuild drill that recomputes from the archive and
diffs — plus event-time bars, emit-and-revise, streaming-vs-batch parity, a
reorder buffer, per-key watermarks, paced replay, ingest-lag distribution,
heartbeat-vs-gap detection, a **schema registry with real compatibility
checking**, an **operational runbook**, and a **real Kafka 4.3.1 broker the
in-process log is now checked against**. **46 tests.**

```bash
python record_session.py --seconds 45   # capture a live Kraken session
python run_pipeline.py                  # log -> consumers -> archive -> serving
python run_parity.py                    # bar parity, gap flags, rebuild drill
python run_analytics.py                 # analytics parity, watermarks, latency
python run_schema.py                    # compatibility algebra, and its limit
python run_kafka_parity.py              # in-process log vs a real broker
python -m pytest tests -q               # 46 tests
```

See [docs/RUNBOOK.md](docs/RUNBOOK.md) for the on-call procedures — gap, ingest
lag, consumer lag, a bar that looks wrong, a rebuild, a schema change — written
for whoever is holding the pager rather than whoever wrote the code.

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

## Real data, end to end (`python run_pipeline.py`)

`record_session.py` captures a live Kraken WebSocket session to disk. The live
connection's only job is to CAPTURE — a socket is unrepeatable, so a result
computed from it cannot be re-derived and a regression cannot be reproduced.
Every downstream test then runs against the recording.

Latest capture: **541 real trades over 416s**, BTC/USD and ETH/USD, with a
measured ingest lag of **p50 416ms / p99 5,468ms** from exchange timestamp to our
receive time. That lag folds together the network path, venue-side batching and
clock offset, and it cannot be reconstructed after the fact — which is why it is
recorded at capture time rather than derived later.

The pipeline then runs: durable partitioned log → two consumer groups at
independent offsets → event-time bars → Parquet archive partitioned by symbol
and hour → DuckDB serving → rebuild drill (16 bars recomputed from the archive,
0 differing) → a consumer crash-and-resume showing offsets do their job.

Two things the real run surfaced that generated data had not:

- **A single poll is not a drain.** The bars consumer polled once per partition
  and reported lag 41 — correct behaviour for the consumer, a bug in the harness
  that then claimed it had consumed everything.
- **Hash partitioning skews with few keys.** Both symbols landed in the same
  partition of four. That is hash partitioning working as designed and being as
  inconvenient as it is in production: two symbols sharing a partition cannot be
  consumed in parallel however many consumers you add.

## Schema evolution, and where a registry stops helping

`src/log.py` stored JSON with no schema, so a producer that renamed a field broke
every consumer **silently**. Run it and watch:

```
1. WITHOUT A REGISTRY -- the producer renames `price` to `px`
consumer VWAP over correct records : 104.5000
consumer VWAP after the rename     : nan
exceptions raised                  : 0
```

Nothing failed. The JSON still parses, `price` is simply absent, and the
consumer computes a number from a field that is not there. **The pipeline does
not break — it keeps running and publishes a wrong figure.**

`src/schema_registry.py` refuses the change at **registration time**, before a
single bad record is written, which is the only place it is cheap. Once a record
is in the log, every consumer has to cope with it forever, replays included.

The compatibility algebra, run over four real changes:

| change | BACKWARD | FORWARD | FULL |
|---|---|---|---|
| add an OPTIONAL field | ok | ok | ok |
| add a REQUIRED field | **REFUSED** | ok | **REFUSED** |
| rename a field | **REFUSED** | **REFUSED** | **REFUSED** |
| narrow float → int | **REFUSED** | ok | **REFUSED** |

**Row two is the asymmetry that catches people.** Adding a required field is
forward compatible and *not* backward compatible — an old reader ignores it, a
new reader cannot find it in the archive. So "add a field" is safe or unsafe
depending entirely on which direction you need, and a registry set to the wrong
mode is worse than no registry, because it issues an approval.

**Row four came out against what I expected to write, and it is the more useful
row.** Narrowing `float → int` *passes* the forward check, and the check is
right: an old reader expecting a float accepts an integer without complaint.
Nothing about the representation breaks. What breaks is the value — every price
is truncated at the producer, and the registry has no way to know, because
compatibility is an algebra over **types** and truncation is a statement about
**meaning**. A registry tells you whether a change will break a reader. It cannot
tell you whether the data is still true, and **treating a green compatibility
check as a review is the mistake it most reliably enables.**

The version travels with the record. Without the stamp a consumer has to guess
which schema a record was written under, the usual guess is "the latest", and
that is wrong for every record written before the last change — a replay is when
the guess costs something. Records with no stamp at all are still readable, and
report `None` for their schema, because a registry that refuses the existing
archive is one nobody can adopt on a live feed.

## The in-process log, checked against a real broker

`src/log.py` implements the subset of Kafka's semantics this pipeline depends on
so those properties could be *tested* rather than assumed. A broker is now
available, so the imitation gets checked instead of trusted. `run_kafka_parity.py`
runs the same ticks through both and diffs the bars:

```
                                      in-process         kafka
records consumed                           6,000         6,000
bars built                                    16            16
bars only on this side                         0             0
MISMATCHED BARS                                                0
```

**Exact** on open, high, low, close, volume and tick count. That establishes the
in-process log preserves what this pipeline actually reads out of a log — per-key
ordering and complete delivery. It does **not** establish that the imitation is
Kafka, and the report says so.

### Where they disagree, reported rather than hidden

**Only 3 of 8 keys land on the same partition.** `PartitionedLog` uses a stable
non-Python hash so assignment survives a restart (Python's `hash()` on `str` is
salted per process, so the obvious implementation reshuffles every partition on
every restart). Kafka uses **murmur2** on the key bytes.

Reimplementing murmur2 to force agreement would be writing a second copy of the
thing under test. The property that matters is that all records for **one** key
land on **one** partition — both satisfy it, and a test asserts it. A parity
claim that quietly compares only the numbers that agree is not a parity claim,
so the disagreement is a printed line and an assertion of its own.

### Three things the real broker corrected

**`consumer_timeout_ms=8000` returned zero records against a topic holding
8,000.** The consumer had not finished joining the group — and "consumed 0" is
indistinguishable from "the topic is empty" unless you happen to know that.

**A single poll is still not a drain**, and the real client makes the trap
sharper: `poll()` can return empty simply because the fetch had not landed. The
drain now runs to an *idle deadline* that resets whenever records arrive.

**kafka-python faults on Windows** with `Invalid file descriptor: -1` when a
socket is closed underneath its selector. The drain retries a bounded 20 times
and then **raises** — swallowing it unbounded would be the dangerous version,
because a drain that quietly returns short looks exactly like an empty topic.

## Schema enforcement moved onto the write path

The registry could validate and stamp a record, and **nothing called it**. So a
producer renaming a field still wrote the record, and the consumer still
computed a number from a field that was not there — the guarantee sat beside the
write path rather than on it.

`PartitionedLog(registry=..., subject=...)` now validates on `append`, and
refuses a **batch whole** if any record in it fails. A partially applied batch
is the worst outcome available: the producer sees an error and the log contains
some of it.

Enforcement is opt-in, deliberately. A log that refuses to start without a
registered schema cannot be adopted on a live feed, and an un-adoptable control
is not a control — so records written before a subject was registered stay
readable, and a caller with no registry gets the old behaviour.

## What is NOT built

1. **A multi-broker cluster.** The broker is real and there is exactly one, so
   replication, leader election, ISR shrink and exactly-once transactions —
   the actual reasons to run Kafka — are still unexercised. This is a smaller
   gap than "no broker at all" and it is not nothing.
2. ~~**Kafka on the pipeline's own path.**~~ **DONE** — `src/backend.py` puts
   one interface over both logs and `run_pipeline_backend.py` runs the real
   pipeline on either, producing an identical bar fingerprint
   (`d712581cd09daf07`). Three claims were refuted by running it: the two
   backends put the same key in **different** partitions (so committed offsets
   do not port), Kafka's record grouping **varies between runs**, and waiting on
   `describe_topics` does not fix the produce race. See `docs/BACKEND_PARITY.md`.
   Superseded note: `run_pipeline.py` still uses the
   in-process log; the Kafka backend is a parallel implementation the parity
   check compares against. Keeping both is deliberate — swapping would delete
   the thing that makes the comparison possible — but it means the shipped
   pipeline is still the imitation.
3. **Flink.** No stream-processing framework: the bar builder is plain Python,
   so there is no distributed state backend, no checkpointing and no savepoints.
4. **A registry SERVICE.** Confluent's is a service — producers and consumers
   resolve schemas over HTTP at runtime and Avro or Protobuf does the encoding.
   This is the compatibility algebra and the version stamp, in process, over
   JSON.
5. **TimescaleDB / ClickHouse.** DuckDB over Parquet answers the range queries
   and supports the batch recomputation the parity check needs. Concurrent
   writers, retention policies, continuous aggregates and replication are lost.
6. **Alerting.** Gap detection, heartbeat loss and ingest lag are all measured
   and exposed; nothing pages anyone. The runbook exists, and a runbook with no
   alert in front of it is a document read after somebody noticed.
7. **Sustained-load capacity numbers.** The recorded session is ~1.3 ticks/sec
   of real venue traffic — far too thin to establish a throughput ceiling.
8. **Schema enforcement is opt-in.** `PartitionedLog(registry=..., subject=...)`
   validates and stamps on the write and refuses a batch whole if any record
   fails. It is opt-in on purpose -- a log that will not start without a
   registered schema cannot be adopted on a live feed -- which means a caller
   who omits the registry gets the old unchecked behaviour.
