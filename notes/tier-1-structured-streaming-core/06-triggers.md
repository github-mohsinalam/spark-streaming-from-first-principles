# Triggers

> **Tier 1 · Concept 6 of 8**
> The control that decides *when* a streaming query fires a new
> micro-batch. Most tutorials race past this with a one-line
> "set processingTime to control batch cadence." That misses the point.
> Triggers are how you translate a business SLA into engine configuration,
> and the choice has real consequences for latency, throughput, cost,
> and operational behaviour.

---

## The one-sentence idea

Triggers are a four-way knob — latency, throughput, cluster cost, and
commit-frequency — that you turn to match the pipeline's actual product
purpose. The right trigger is rarely the default, and "lowest latency"
is rarely the right optimisation target.

---

## What a trigger actually does

Recall the trigger loop from Concept 1:

```
trigger fires → read new offsets → run Catalyst plan → write sink → commit
       ↑                                                                │
       └────────────────────────────────────────────────────────────────┘
                       (loop forever)
```

The trigger is what causes that loop to advance from one iteration to
the next. Without specifying a trigger, Spark uses the **default**: as
soon as the previous batch finishes, immediately start the next one.
This sounds reasonable until you trace what it means in practice — the
streaming query runs flat-out, consuming all available CPU, regardless
of whether new input has arrived.

So the first thing to know about triggers is: **the default is rarely
what you want for production.**

The four trigger types correspond to four different relationships
between *clock time* and *batch boundaries*:

| Trigger                   | Batch boundaries are driven by...                              |
| ------------------------- | -------------------------------------------------------------- |
| Default (unspecified)     | Previous batch finishing                                       |
| `ProcessingTime("Ns")`    | A wall-clock interval                                          |
| `AvailableNow`            | The set of records currently available — process all, then stop|
| `Continuous("Nms")`       | (Experimental) Per-record, no batches at all                   |

---

## Default trigger — "as fast as possible"

What it does: as soon as batch N finishes, immediately starts batch N+1.

What this means in practice:

- If new data has arrived, the next batch processes it immediately.
- If no new data has arrived, the next batch is empty but still fires
  — creating an empty Delta commit, an empty offset log entry, and an
  empty `_delta_log/00000000000000000N.json` file.

### Why this is almost never what you want

Two costs compound:

1. **CPU spin on empty batches.** The streaming query is *always* busy
   processing empty DataFrames. The cluster is at 100% utilisation
   doing nothing useful.
2. **Delta log bloat.** `_delta_log` writes a new versioned JSON file
   per commit, even when its `add` action list is empty. After a week
   of running, you have hundreds of thousands of empty Delta versions.
   This slows down checkpoint-based table listings, forces premature
   compaction work, makes time-travel queries navigate millions of
   mostly-empty versions, and inflates cloud-storage object counts
   (real money on S3/ADLS at scale).

The senior pattern recognition here: **knowing what looks like a
performance optimisation but is actually a cost amplifier is a
senior-DE skill.** The default trigger is the canonical example — it
appears to optimise latency but actually wastes compute and inflates
storage with empty bookkeeping.

The default trigger exists because Spark needs *some* behaviour to
fall back to. Treat it as a "no commitment" placeholder, not a
deliberate choice.

---

## `ProcessingTime("N seconds")` — "fire on a wall-clock cadence"

What it does: fires the next batch every N seconds, starting from when
the query begins. If the previous batch is still running when the
trigger fires, the new trigger is *delayed* until the previous finishes
(Spark logs *"Current batch is falling behind"* — a warning worth
treating as a signal that the trigger interval is too tight).

### What this means in practice

- Every N seconds, regardless of how much input has arrived.
- A batch may be empty (no new input) or large (a burst arrived). The
  trigger interval bounds the *minimum* latency, not the batch size.

### When to use it

This is the workhorse trigger and the default choice for steady-state
pipelines. Three product questions it answers:

1. **"Dashboard ≤ 10 seconds stale."** Set `ProcessingTime("5 seconds")`
   — every 5 seconds the aggregation refreshes, so worst-case staleness
   is one trigger interval plus processing time.
2. **"Batch into reasonable file sizes."** Trigger interval determines
   how much data accumulates per batch, which determines output file
   sizes. A 10-second trigger on a Kafka topic at 1000 rec/sec produces
   10,000-record batches → reasonable-sized files. A 1-second trigger
   produces 1,000-record batches → small-files problem.
3. **"Throttle commits on a slow downstream sink."** If your sink can't
   keep up with empty-batch-per-second commits, a longer trigger gives
   it breathing room.

The trigger interval is **the most important latency knob in streaming.**
It's the explicit translation of "how fresh does the data need to be"
into engine config.

### Smoothing bursts: trigger interval vs `maxOffsetsPerTrigger`

Two knobs exist for two slightly different problems:

- **Smoothing over time** (handling bursts in steady-state): set a
  longer trigger interval. A 30-second trigger naturally consolidates
  a one-second burst of 10,000 records into a 10,000-record batch
  every 30 seconds, with no extra config.
- **Capping batch size** (handling absolutely-too-big batches): set
  `maxOffsetsPerTrigger`. This caps the number of records the engine
  reads per batch even if more are available. Useful when bursts are
  unpredictable and could exceed safe memory limits.

They compose: in a 30-second trigger window, Spark reads *up to*
`maxOffsetsPerTrigger` records, whichever limit comes first. For
steady-state pipelines where bursts are the only concern, the trigger
interval alone is the right smoothing knob. Reach for
`maxOffsetsPerTrigger` only when burst size is genuinely
unpredictable.

### When you wouldn't use ProcessingTime

- For one-off backfills where the source already has all the data and
  you just want to drain it.
- For sub-100ms latency requirements (use `Continuous`, with caveats).

---

## `AvailableNow` — "drain the source, then stop"

What it does: processes all data currently available in the source in
a series of micro-batches, then **terminates the query**. The query
is no longer a long-running stream; it's a *bounded* job using the
streaming machinery.

### What this means in practice

- On query start, Spark inspects the source to determine "what's
  available right now" — for Kafka, the highest offset per partition;
  for files, the current file list; for Delta, the current table
  version.
- Then it processes that data, possibly across multiple internal
  micro-batches (governed by `maxOffsetsPerTrigger` /
  `maxFilesPerTrigger`).
- When done, the query stops cleanly.

### The senior-grade insight

**`AvailableNow` lets you run streaming queries in batch-job patterns.**
You get the streaming machinery (replayable source, checkpoint,
exactly-once into Delta) without the always-on cluster. Pay-per-job
rather than pay-per-second. This is a major cost lever for streaming
pipelines that don't actually need continuous low-latency processing.

---

## When `AvailableNow` is genuinely needed: three patterns

### Pattern 1: Scheduled batch ingest of streaming sources

The dashboard is checked twice a day, but the pipeline currently runs
`ProcessingTime("10s")` 24/7. The cost math:

- Current: cluster up 168 hours/week to serve a dashboard checked twice.
- Proposed: `AvailableNow` twice a day, each run ~20 minutes
  (depending on volume) → ~4.7 hours/week of compute.
- **97% reduction in cluster spend, same freshness at the moments of
  actual use.**

The argument that survives an executive conversation: *"The current
pipeline optimises freshness 24/7 for a dashboard that's looked at
twice. We're paying for freshness nobody experiences. The proposed
schedule provides identical freshness at the moments of actual use,
while eliminating compute spend during the 99% of hours nobody's
looking."*

This preempts the obvious objection ("but won't the dashboard be
stale?"). Yes, technically, but only when nobody's looking — which
is the whole point.

### Pattern 2: Catchup after an outage

Your pipeline runs `ProcessingTime("15 seconds")` steady-state. Then
it's down for 12 hours. Running it back up normally would try to fire
the first batch with 12 hours of accumulated data — potentially
overwhelming the cluster or breaching SLAs on first restart.

The pattern:

1. **Catchup phase.** Run with `AvailableNow` + `maxOffsetsPerTrigger`
   set to a value your cluster can handle (say, 1M records). The job
   drains the 12-hour backlog across many internal batches, each
   bounded by `maxOffsetsPerTrigger`. Cluster stays within safe
   memory limits throughout. Job terminates when caught up.
2. **Steady-state phase.** Switch back to
   `ProcessingTime("15 seconds")`. Same checkpoint location, same
   logic, same SLA as before the outage.

### The production pattern: parameterise the trigger

The naïve version of pattern 2 requires manually editing the code,
deploying, then editing back. Two deploys, error-prone. The
production-grade version makes the trigger a deploy-time configuration:

```scala
val trigger = sys.env.get("RUN_MODE") match {
  case Some("catchup")  => Trigger.AvailableNow
  case Some("steady")   => Trigger.ProcessingTime("15 seconds")
  case _ => throw new IllegalArgumentException("Set RUN_MODE")
}

events.writeStream
  .option("checkpointLocation", "/lake/events/_checkpoint")
  .option("maxOffsetsPerTrigger", "1000000")  // safety cap
  .trigger(trigger)
  .start()
```

After an outage, the runbook is:

1. Run with `RUN_MODE=catchup`. Picks up at the last committed
   offset, drains everything available, terminates cleanly.
2. Once it terminates, run with `RUN_MODE=steady`. Same checkpoint,
   same query — now ticking on a 15-second cadence with no backlog.

### Why same-checkpoint works across this swap

The trigger is **not** a structural part of the query plan. It's a
runtime-loop-cadence setting. The checkpoint identifies the query by
its physical contents (source identities, state schema, output mode),
none of which change when you swap triggers. This is why `RUN_MODE=catchup`
and `RUN_MODE=steady` can share the same checkpoint location safely.

(Caveat: structural changes like topic name, output mode, or
aggregation shape *would* invalidate the checkpoint. Covered in
Concept 8.)

### The "wall-clock-stable available now" property

There's a subtle reason catchup-then-steady doesn't double-process or
skip anything: **the source's notion of "available now" is wall-clock-
stable.** When `AvailableNow` starts at time T, it asks Kafka "what's
the highest offset right now?" and locks that in as the target. The
job runs until offsets reach that target, then terminates — even if
new data continues flowing in during the catchup.

So if new data arrives during the 20 minutes of catchup, those new
records sit in Kafka. When the catchup job ends and you switch to
steady-state, the first steady-state batch picks them up from where
catchup ended. No gap, no overlap.

### Pattern 3: Testing the streaming logic as a bounded job

Develop the streaming logic against a fresh checkpoint, run it once
with `AvailableNow` against a known input, inspect the output,
iterate. Same code path as production, no always-on cluster needed
during development.

---

## `Continuous("N ms")` — "no batches, per-record"

What it does: abandons the micro-batch model entirely. Records are
processed individually as they arrive, with checkpoint commits
happening every N milliseconds (the "checkpoint interval").

### Constraints

- **Only a small subset of operations are supported.** No aggregations.
  No joins (with rare exceptions). Only stateless map/filter/project
  and limited deduplication.
- **At-least-once delivery only.** Exactly-once is not supported.
- **Reduced fault tolerance.** Recovery semantics are weaker than
  micro-batch.
- **Still experimental in Spark 4.** Has been "experimental" for
  several major versions; the API has not converged.

### Why so limited?

Micro-batch is *the* mechanism by which Spark achieves exactly-once:
the batch boundary is the transaction boundary. Continuous mode
dissolves that boundary, which loses the property. Tier 0's
processing-models concept covered this — Spark traded latency floor
for easy exactly-once, and continuous mode is the reverse trade.

### Honest take

**Continuous trigger exists, but you almost certainly won't use it.**
Most "sub-second latency" requirements turn out, on examination, to
be "sub-5-second latency" requirements that micro-batch handles fine.
Genuine sub-100ms requirements usually push teams to Flink or Kafka
Streams rather than Spark's experimental continuous mode.

Knowing it exists is enough.

---

## The latency / throughput / cost / commit-frequency knob

Triggers expose a four-way trade-off:

|                       | `ProcessingTime`        | `AvailableNow`            |
| --------------------- | ----------------------- | ------------------------- |
| Latency               | ~N sec                  | bounded job time          |
| Throughput            | steady (rec / N sec)    | batched, paceable         |
| Cluster cost          | always-on               | pay-per-run               |
| Commit frequency      | every N sec             | per internal batch        |
| Output file sizes     | ~rate × N sec           | paceable via `maxOffsets…`|

Picking a trigger is picking which point on this trade-off space
you want:

- **"Real-time dashboard, looked at constantly"** → `ProcessingTime("5s")`
  → low latency, always-on cluster, small batches.
- **"Hourly batch ETL of a streaming source"** → `AvailableNow` on cron
  → high throughput per run, no idle cost, large batches.
- **"Bursty pipeline, latency tolerant"** → `ProcessingTime("30s")` →
  smooths bursts into reasonable batch sizes, balances latency and
  cost.

The mistake to avoid: thinking the trigger is purely a latency setting.
It's a four-way knob, and aggressive latency settings have real costs
in throughput, cluster spend, and commit-history bloat.

---

## Connecting back

**To Concept 1 (`readStream` / `writeStream`).** The trigger is
configured on `writeStream`, not `readStream`. Sources don't trigger;
sinks (and the engine driving the sink) do. This is consistent with
the framing: `writeStream` is where you commit to the runtime
behaviour of the query, including how often it loops.

**To Concept 3 (sources and replayability).** `AvailableNow` works
because the source is *replayable*: the engine can ask "what's
available right now" and trust that the source will hand back the
same data on a future query. A non-replayable source can't support
`AvailableNow` — there's no "what's available" to enumerate.

---

## Spark 3.x → 4.x note

`AvailableNow` was added in Spark 3.3 and is stable through Spark 4.
`Once` is deprecated in Spark 3.3+ in favour of `AvailableNow`.
`Continuous` has been experimental since Spark 2.3 and remains
experimental in Spark 4 — knowing it exists is still enough, the API
has not converged. `ProcessingTime` is stable across all versions
you'll touch.

---

## Prove you got it

1. **Trigger to product-question.** For each of these product
   requirements, name the right trigger (and any options needed
   alongside) and justify in one sentence:
    - (a) "Dashboard data should be at most 30 seconds stale."
    - (b) "Run nightly: pick up everything that arrived in Kafka today,
      process to Delta, terminate."
    - (c) "Recover from a 12-hour outage without overwhelming the
      cluster with one massive batch."
    - (d) "Run the streaming query continuously, but smooth out
      producer bursts into reasonably-sized batches."
2. **The default trigger trap.** A teammate launches a streaming
   Kafka query with no trigger specified. Initially it works fine. A
   week later, the Delta table has 12 million versions in `_delta_log/`
   and the cluster is at 100% CPU even when no data is flowing.
   Explain what happened and what they should have done instead.
3. **The `AvailableNow` cost argument.** Your company has a streaming
   pipeline currently running 24/7 with `ProcessingTime("10s")`. The
   dashboard it feeds is checked twice a day by humans. You want to
   argue for switching to `AvailableNow` on a schedule. Make the
   argument: what changes, what stays the same, and what cluster cost
   reduction can you (approximately) claim?

<details>
<summary>Answers</summary>

1. (a) `ProcessingTime("15 seconds")` — fits comfortably in the
   30-second budget with headroom for occasional slow batches.
   (b) `AvailableNow` on a cron — picks up everything available,
   processes to Delta, terminates; no always-on cluster.
   (c) `AvailableNow` with `maxOffsetsPerTrigger` set to a
   cluster-safe cap (e.g. 1M records). Drains the 12-hour backlog
   across many internal bounded batches, then terminates; switch
   back to `ProcessingTime` for steady-state.
   (d) `ProcessingTime` with a trigger interval long enough to
   absorb the burst window (e.g. 30 seconds). The trigger interval
   itself is the smoothing knob; `maxOffsetsPerTrigger` is needed
   only if bursts could exceed safe memory.
2. The default trigger fires a new batch immediately after the
   previous one finishes — including when no new data has arrived.
   Each empty trigger still writes a new offset log entry, a new
   commit log entry, and (for Delta) a new versioned commit file in
   `_delta_log`. Over a week this produces millions of empty
   versions, slowing all subsequent table reads, forcing premature
   compaction, and inflating cloud-storage object counts. The
   cluster is at 100% CPU because the streaming loop is busy doing
   bookkeeping for empty DataFrames. The fix is to specify a
   `ProcessingTime` trigger matched to the actual SLA — even one
   second is dramatically better than the default, because it gives
   real input time to accumulate between batches.
3. **What changes:** the cluster is no longer always-on. Two
   scheduled invocations per day, each running `AvailableNow`,
   process all data accumulated since the last run and terminate.
   The cluster spins up for each run and spins down on termination.
   **What stays the same:** checkpoint location, query structure,
   source/sink, all output semantics. Each invocation's offset log
   continues exactly where the previous left off. The data on disk
   is identical to what the always-on pipeline would have produced.
   **Cost reduction:** the always-on version uses ~168 cluster hours
   per week. The scheduled version uses roughly 2 × 7 × 20 min
   ≈ 4.7 hours per week. That's a ~97% reduction in compute spend.
   The argument that survives executive scrutiny: the original
   pipeline optimised freshness 24/7 for a dashboard checked twice;
   we're paying for freshness nobody experiences. The scheduled
   version provides identical freshness at the moments of actual
   use, while eliminating compute spend during the 99% of hours
   nobody's looking.

</details>

---

[← Tier 1 index](./README.md) · [Previous: Output Modes ←](./05-output-modes.md) · [Next: Stateless Transformations →](./07-stateless-transformations.md)