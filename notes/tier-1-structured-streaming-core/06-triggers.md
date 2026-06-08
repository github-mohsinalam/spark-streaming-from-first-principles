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
commit frequency — that you turn to match the pipeline's actual product
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
the next.

The four trigger types correspond to four different relationships
between *clock time* and *batch boundaries*:

| Trigger                   | Batch boundaries are driven by...                              |
| ------------------------- | -------------------------------------------------------------- |
| Default (unspecified)     | Previous batch finishing                                       |
| `ProcessingTime("Ns")`    | A wall-clock interval                                          |
| `AvailableNow`            | The set of records currently available — process all, then stop|
| `Continuous("Nms")`       | (Experimental) Per-record, no batches at all                   |

---

## Default trigger — the actual behaviour

The Apache Spark documentation, quoted in the Databricks knowledge base,
defines the default trigger as:

> *"If no trigger setting is explicitly specified, then by default, the
> query will be executed in micro-batch mode, where micro-batches will
> be generated as soon as the previous micro-batch has completed
> processing."* — [Databricks KB](https://kb.databricks.com/streaming/optimize-streaming-transactions-with-trigger)

Databricks characterises this as equivalent to a `processingTime` trigger with 0 ms intervals. The "0 ms" doesn't mean "0 ms between
empty batches." It means "as soon as the previous batch finishes,
fire the next one — *if* there's new data to process."

### The mechanism, step by step

After each batch, Spark's micro-batch executor does the following loop
([Spark internals reference](https://books.japila.pl/spark-structured-streaming-internals/micro-batch-execution/MicroBatchExecution/)):

1. Ask each source: "what's your latest offset?" (`getOffset()`).
2. Compare against the last-committed offset. Is there new data?
3. **If yes** → construct the next micro-batch immediately, run it, commit.
4. **If no** → either sleep briefly (governed by `spark.sql.streaming.pollingDelay`) and re-check, or construct an empty
   batch for state-maintenance purposes (see below).

### The empty-batch nuance

Spark exposes a configuration property
`spark.sql.streaming.noDataMicroBatches.enabled` that defaults to
`true`. The official internals documentation defines it as:

> "Controls whether the streaming micro-batch engine should execute batches with no data to process for eager state management for stateful streaming queries (true) or not (false)."

So empty batches *do* fire occasionally — but for a deliberate reason:
**stateful operators (watermarks, session windows, `transformWithState`)
need periodic empty batches to advance their internal clocks and emit
results that depend on time progression**. Databricks documents this
explicitly:

> "Setting this configuration to false might also result in stateful operations that use watermarks or processing time timeouts not getting data output until new data arrives instead of immediately."

For a *stateless* query (just filter / map / project / passthrough
sinks), these empty batches typically produce no observable output and
no Delta commit. For a *stateful* query, they are functionally necessary.

### Why this matters for what you observe in a demo

If you run a stateless streaming query against a socket source with
the default trigger and no typing, you'll see:

- The first batch fires (often to initialize the offset log and
  metadata).
- Subsequent batches fire only when you actually type something into
  the socket.
- The console stays quiet in between — the engine is sleeping,
  waiting for the source to advance.

That empirical behaviour is correct and consistent with the
mechanism above.

---

## The real problems with the default trigger

The default trigger does not "spin CPU on empty batches." But it does
have real failure modes, and they're worth knowing precisely.

### 1. No commitment to a cadence

You cannot tell the team *"the dashboard is at most N seconds stale."*
The cadence is "whatever the engine decides, given how fast the
previous batch finished and how fast the source produces data."
Predictability and SLA control are lost.

This is the deepest reason to specify a trigger explicitly: not for
performance, but for **operational determinism**.

### 2. Excessive small commits under bursty input

This is the real Delta-log-bloat scenario, and it's about real data,
not empty batches:

If your data arrives in many small frequent bursts — say, a Kafka topic
receiving sporadic individual records — the default trigger creates
*one micro-batch per burst*. Each burst becomes its own:

- Offset log entry
- Commit log entry
- Delta `_delta_log` JSON commit file
- Set of small Parquet files

Over a week of sparse-but-bursty traffic, this can produce hundreds of
thousands of tiny Delta versions and millions of tiny output files. The
downstream consequences:

- Slow table reads (Delta has to traverse a long version chain).
- Premature compaction work.
- Time-travel queries (`VERSION AS OF`) navigate excessive history.
- Cloud-storage object counts grow large — real money on S3/ADLS at
  scale.

### 3. Storage transaction costs

For cloud-storage-backed pipelines, polling has direct cost. From a
[Databricks knowledge base article](https://kb.databricks.com/streaming/optimize-streaming-transactions-with-trigger):

> "When running a structured streaming application that uses cloud storage buckets (S3, ADLS Gen2, etc.) it is easy to incur excessive transactions as you access the storage bucket. Failing to specify a .trigger option in your streaming code is one common reason for a high number of storage transactions. When a .trigger option is not specified, the storage can be polled frequently."

Cloud storage typically bills per request. A streaming query that polls
every few milliseconds can rack up surprising bills even when processing
little data.

### 4. No backpressure or batch-size control

`ProcessingTime("30s")` implicitly throttles by accumulating 30 seconds
of data per batch. Default trigger has no such throttle — each batch
is "whatever has arrived since the last one finished."

### The pattern recognition

Knowing what *looks* like a performance optimisation but is actually
a cost amplifier is a senior-DE skill. The default trigger is the
canonical example: it appears to optimise latency, but in practice
the lack of cadence commitment produces commit storms, file-count
bloat, and storage transaction cost — all without any matching
business benefit because no SLA actually requires "fire as soon as
possible."

Treat the default trigger as a "no commitment" placeholder, not a
deliberate choice. Specifying any explicit `ProcessingTime` interval
— even a small one — fixes most of the problems above.

---

## `ProcessingTime("N seconds")` — "fire on a wall-clock cadence"

What it does: fires the next batch every N seconds, starting from when
the query begins. If the previous batch is still running when the
trigger fires, the new trigger is *delayed* until the previous
finishes. Spark logs *"Current batch is falling behind"* in that case
— a warning worth treating as a signal that the trigger interval is
too tight.

### What this means in practice

- Every N seconds, regardless of how much input has arrived.
- A batch may be empty (no new input) or large (a burst arrived). The
  trigger interval bounds the *minimum* latency, not the batch size.

### When to use it

This is the workhorse trigger and the default choice for steady-state
pipelines. Three product questions it answers:

1. **"Dashboard ≤ 10 seconds stale."** Set `ProcessingTime("5 seconds")`
   — worst-case staleness is one trigger interval plus processing time.
2. **"Batch into reasonable file sizes."** Trigger interval determines
   how much data accumulates per batch, which determines output file
   sizes.
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
  every 30 seconds.
- **Capping batch size** (handling absolutely-too-big batches): set
  `maxOffsetsPerTrigger`. Caps the number of records the engine reads
  per batch even if more are available.

They compose: in a 30-second trigger window, Spark reads *up to*
`maxOffsetsPerTrigger` records, whichever comes first. For
steady-state pipelines where bursts are the only concern, the trigger
interval alone is the right smoothing knob. Reach for
`maxOffsetsPerTrigger` only when burst size is genuinely unpredictable.

---

## `AvailableNow` — "drain the source, then stop"

What it does: processes all data currently available in the source in
a series of micro-batches, then **terminates the query**. The query
is no longer a long-running stream; it's a *bounded* job using the
streaming machinery.

The Apache Iceberg connector documentation summarises the key insight
clearly:

> "Rate limiting options can be applied to queries that use Trigger.AvailableNow to split one-time processing of all available source data into multiple micro-batches for better query scalability."

### What this means in practice

- On query start, Spark inspects the source to determine "what's
  available right now."
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
overwhelming the cluster.

The pattern:

1. **Catchup phase.** Run with `AvailableNow` + `maxOffsetsPerTrigger`
   set to a value your cluster can handle (say, 1M records). The job
   drains the 12-hour backlog across many internal batches, each
   bounded by `maxOffsetsPerTrigger`. Cluster stays within safe
   memory limits. Job terminates when caught up.
2. **Steady-state phase.** Switch back to
   `ProcessingTime("15 seconds")`. Same checkpoint location, same
   logic, same SLA as before the outage.

### The production pattern: parameterise the trigger

The naïve version requires editing the code twice during the outage.
The production-grade version makes the trigger a deploy-time config:

```scala
val trigger = sys.env.get("RUN_MODE") match {
  case Some("catchup")  => Trigger.AvailableNow
  case Some("steady")   => Trigger.ProcessingTime("15 seconds")
  case _ => throw new IllegalArgumentException("Set RUN_MODE")
}

events.writeStream
  .option("checkpointLocation", "/lake/events/_checkpoint")
  .option("maxOffsetsPerTrigger", "1000000")
  .trigger(trigger)
  .start()
```

After an outage, the runbook is:

1. Run with `RUN_MODE=catchup`. Picks up at the last committed offset,
   drains everything available, terminates cleanly.
2. Once it terminates, run with `RUN_MODE=steady`. Same checkpoint,
   same query — now ticking on a 15-second cadence with no backlog.

### Why same-checkpoint works across this swap

The trigger is **not** a structural part of the query plan. It's a
runtime-loop-cadence setting. The checkpoint identifies the query by
its physical contents (source identities, state schema, output mode),
none of which change when you swap triggers.

Databricks documents this explicitly:

> "You can change the trigger interval between runs while using the same checkpoint."

The same docs also describe expected transition behavior:

> "If a Structured Streaming job stops while a micro-batch is being processed, that micro-batch must complete before the new trigger interval applies. As a result, you might observe a micro-batch processing with the previously specified settings after changing the trigger interval."

(Structural changes — topic name, output mode, aggregation shape —
*would* invalidate the checkpoint. Covered in Concept 8.)

### The "wall-clock-stable available now" property

Catchup-then-steady doesn't double-process or skip anything because
the source's notion of "available now" is wall-clock-stable. When
`AvailableNow` starts at time T, it asks Kafka "what's the highest
offset right now?" and locks that in as the target. The job runs
until offsets reach that target, then terminates — even if new data
continues flowing in during the catchup.

So if new data arrives during the 20 minutes of catchup, those new
records sit in Kafka. When the catchup job ends and you switch to
steady-state, the first steady-state batch picks them up from where
catchup ended. No gap, no overlap.

### Pattern 3: Testing the streaming logic as a bounded job

Develop the streaming logic against a fresh checkpoint, run it once
with `AvailableNow` against a known input, inspect the output,
iterate. Same code path as production, no always-on cluster needed.

---

## `Continuous("N ms")` — "no batches, per-record"

What it does: abandons the micro-batch model entirely. Records are
processed individually as they arrive, with checkpoint commits
happening every N milliseconds.

### Constraints

- **Only a small subset of operations are supported.** No aggregations.
  No joins (with rare exceptions). Only stateless map/filter/project
  and limited deduplication.
- **At-least-once delivery only.** Exactly-once is not supported.
- **Reduced fault tolerance.** Recovery semantics are weaker than
  micro-batch.
- **Still experimental in Spark 4.**

Databricks is explicit about this:

> "Apache Spark has an additional trigger interval known as Continuous Processing. This mode has been classified as experimental since Spark 2.3. Azure Databricks doesn't support or recommend this mode."

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
  smooths bursts into reasonable batch sizes.

The mistake to avoid: thinking the trigger is purely a latency setting.
It's a four-way knob, and aggressive latency settings have real costs
in throughput, cluster spend, and commit-history bloat.

---

## Connecting back

**To Concept 1 (`readStream` / `writeStream`).** The trigger is
configured on `writeStream`, not `readStream`. Sources don't trigger;
sinks (and the engine driving the sink) do.

**To Concept 3 (sources and replayability).** `AvailableNow` works
because the source is *replayable*: the engine can ask "what's
available right now" and trust that the source will hand back the
same data on a future query. A non-replayable source can't support
`AvailableNow`.

---

## Spark 3.x → 4.x note

`AvailableNow` was added in Spark 3.3 and is stable through Spark 4.
`Once` is deprecated in Spark 3.3+ in favour of `AvailableNow`.
`Continuous` has been experimental since Spark 2.3 and remains
experimental in Spark 4 — the API has not converged.
`ProcessingTime` is stable across all versions you'll touch.

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
   Kafka query with no trigger specified. The traffic is bursty —
   single records arriving sporadically throughout the day. A week
   later, the Delta table has hundreds of thousands of versions in
   `_delta_log/` and the cloud storage bill is unexpectedly high.
   Explain what happened (don't say "CPU is at 100% spinning on
   empty batches" — that's a misconception) and what they should
   have done instead.
3. **The `AvailableNow` cost argument.** Your company has a streaming
   pipeline currently running 24/7 with `ProcessingTime("10s")`. The
   dashboard it feeds is checked twice a day by humans. You want to
   argue for switching to `AvailableNow` on a schedule. Make the
   argument: what changes, what stays the same, and what cluster cost
   reduction can you (approximately) claim?

<details>
<summary>Answers</summary>

1. (a) `ProcessingTime("15 seconds")` — fits comfortably in the
   30-second budget with headroom.
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
2. The default trigger fires a new batch as soon as the previous one
   finishes *and* the source has new data. With bursty input, every
   sporadic burst becomes its own tiny micro-batch — each with its
   own offset commit, commit log entry, Delta `_delta_log` file, and
   set of small Parquet files. Over a week of sparse-but-bursty
   traffic this produces hundreds of thousands of tiny Delta
   versions, slowing all subsequent reads, forcing premature
   compaction, and inflating cloud-storage object counts and
   transaction costs (cloud storage bills per request, and rapid
   polling-then-committing produces many requests). The CPU is not
   "spinning on empty batches" — the engine sleeps between bursts —
   but the *real data* arriving in many small pieces is what bloats
   the commit history. The fix: specify a `ProcessingTime` interval
   matched to the SLA (say, 30 seconds). Each batch now consolidates
   ~30 seconds of bursts into one larger commit, dramatically
   reducing version count and storage transactions while staying
   well within typical SLA budgets.
3. **What changes:** the cluster is no longer always-on. Two
   scheduled invocations per day, each running `AvailableNow`,
   process all data accumulated since the last run and terminate.
   The cluster spins up for each run and spins down on termination.
   **What stays the same:** checkpoint location, query structure,
   source/sink, output semantics. Each invocation's offset log
   continues exactly where the previous left off. Data on disk is
   identical to what the always-on pipeline would have produced.
   **Cost reduction:** the always-on version uses ~168 cluster hours
   per week. The scheduled version uses roughly 2 × 7 × 20 min
   ≈ 4.7 hours per week — a ~97% reduction in compute spend. The
   argument that survives executive scrutiny: the original pipeline
   optimised freshness 24/7 for a dashboard checked twice; we're
   paying for freshness nobody experiences.

</details>

---

[← Tier 1 index](./README.md) · [Previous: Output Modes ←](./05-output-modes.md) · [Next: Stateless Transformations →](./07-stateless-transformations.md)