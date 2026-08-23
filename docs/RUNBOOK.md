# Runbook — market data pipeline

Written for whoever is holding the pager, not for whoever wrote the code. Every
procedure here is a script that exists in this repo, so the runbook and the code
cannot drift apart the way a hand-maintained document does.

**What this covers:** gap detection, ingest lag, consumer lag, a bar that looks
wrong, a rebuild, and a schema change. **What it does not:** anything requiring a
broker, because there is no broker — see the *Escalation* section for what that
means when the answer would normally be "fail over".

---

## 0. Before anything else: is the data wrong, or is the feed down?

They look identical on a dashboard and need opposite responses.

| symptom | feed is down | market is quiet |
|---|---|---|
| ticks/sec | 0 | low but non-zero |
| heartbeat / last message age | growing without bound | normal |
| bars emitted | none | emitted, thin |
| gap flags | set on the boundary bar | clear |

`run_pipeline.py` reports the last message age per symbol. **A quiet market
produces bars; a dead feed produces silence.** If you cannot tell, assume the
feed is down — the cost of investigating a quiet market is a few minutes, and the
cost of publishing bars from a dead feed is a wrong number that consumers trade
on.

---

## 1. Gap detected

**Signal:** bars adjacent to the gap carry `suspect = true` and the gap appears
in the gap table.

1. Confirm the gap is real and not a reconnect: `python run_parity.py` prints
   detected gaps with their boundaries.
2. Decide whether the gap is *inside* the watermark bound (5s). Inside, late
   ticks revise the bar. Outside, they go to the late-events table and the bar
   stays suspect. **This is a decision, not a bug**: a bar consumers have
   already acted on is not retroactively changed.
3. If the gap spans more than one bar, the affected bars are flagged and the
   correct action is to **leave them flagged**, not to backfill them into
   looking complete. A consumer that can see the hole can decide what to do; one
   that cannot, cannot.

**Do not** rebuild to "fill" a gap. Rebuilding recomputes from the archive, and
the archive is missing exactly the ticks the gap is made of. You will get the
same bars back, with the flags cleared, which is strictly worse.

---

## 2. Ingest lag has grown

**Signal:** the ingest-lag percentiles recorded at capture time (p50 / p99 from
exchange timestamp to our receive time) have moved.

That lag folds together three things and **they cannot be separated after the
fact** — this is why it is recorded at capture time rather than derived later:

- network path
- venue-side batching
- clock offset between us and the exchange

1. Check whether p50 moved or only p99. p50 moving is a path or batching change;
   p99 alone is usually a garbage-collection or scheduling artefact on our side.
2. Compare against another symbol on the same venue. Both moving points at the
   venue or the path; one moving points at us.
3. **Clock offset is the trap.** If our clock has drifted, ingest lag moves with
   no underlying change at all, and every event-time bar boundary moves with it.
   Check NTP sync before concluding anything about the venue.

---

## 3. Consumer lag is growing

**Signal:** a consumer group's committed offset falls further behind the
partition high-water mark on each poll.

1. `run_pipeline.py` reports lag per partition per group.
2. **Check whether the lag is on one partition or all of them.** All partitions
   means the consumer is too slow. One partition means a hot key — and with few
   symbols this pipeline has already hit that: both symbols hashed to the same
   partition of four. Two symbols sharing a partition cannot be consumed in
   parallel however many consumers you add.
3. Adding consumers past the partition count does nothing. That is not a tuning
   subtlety, it is the partition model.

---

## 4. A bar looks wrong

Work in this order, because each step rules out the one after it.

1. **Is it flagged suspect?** Then it is a known late arrival or gap boundary and
   the flag is the answer.
2. **Does it disagree with batch?** Run `run_parity.py`. Every mismatch must be
   one of the documented late-arrival exceptions, and the *unexplained* count
   must be 0. A non-zero unexplained count means the streaming path is wrong and
   this stops being a data question.
3. **Is the analytics stream folding in arrival order?** This bit once: naive
   append produced **812 jump flags against batch's 2** and realised vol 59% too
   high, because a tick folded three seconds out of place fabricates two spurious
   returns. If jump flags are wildly high, check the reorder buffer first.
4. **Is the window bar-aligned?** See §5.

---

## 5. Rebuild from the archive

`run_parity.py` performs the rebuild drill. **The window must be bar-aligned.**

Rebuilding an arbitrary timespan re-derives the edge bars from a partial tick
population and produces two wrong bars that look exactly like a parity bug. That
is the real trap in a reprocessing runbook and it bit this build before the
alignment was made explicit.

1. Choose a window whose boundaries fall on bar boundaries.
2. Rebuild and diff against the serving layer. The expected result is **0
   differing**.
3. If bars differ only at the window edges, the window was not aligned. Fix the
   window, not the pipeline.

---

## 6. A producer wants to change the schema

**Do not** let the change reach the log first and reason about it afterwards.

1. Run `python run_schema.py` and register the proposed schema against the
   current one.
2. The registry refuses breaking changes at **registration time**, before a
   single bad record is written, which is the only place it is cheap.
3. Pick the compatibility mode by asking *who you cannot redeploy*: BACKWARD if
   consumers upgrade first (the usual case — a consumer that cannot read history
   cannot replay), FORWARD if producers do.
4. **A green compatibility check is not a review.** Narrowing `float → int`
   passes the forward check and truncates every price on the feed. The registry
   reasons about types; truncation is about meaning.

---

## 7. Escalation, and the honest limits

| situation | action here | what a real deployment would do |
|---|---|---|
| broker node down | **not applicable** — there is no broker | fail over to a replica, ISR handles it |
| partition leader election | **not applicable** | automatic |
| exactly-once needed | **not available** | Kafka transactions |
| serving layer down | restart; DuckDB is a file | fail over the cluster |
| feed down | reconnect; ticks in the outage are lost | same, plus venue replay if offered |

The first three rows are the reasons to run Kafka, and `src/log.py` does not
replace it. It implements keyed partitions, durable offsets, consumer groups and
at-least-once delivery — enough to *test* the properties this pipeline depends
on. Replication, leader election, ISR and exactly-once are absent, so any page
whose answer is "fail over" has no answer here.

**Nothing in this runbook pages anyone.** Gap detection, heartbeat loss and
ingest lag are all measured and exposed; no alert rule fires and no dashboard
renders them. A runbook without an alert on the front of it is a document that
gets read *after* somebody noticed, which is the wrong end of the incident.
