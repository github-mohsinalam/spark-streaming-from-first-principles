# Streaming Aggregations

> **Tier 2 · Concept 1 of 9**
> Where "stateful" stops being a label and becomes a concrete execution model.
> The root cause of the two rules left unresolved in Tier 1.

---

## Two threads from Tier 1

Two facts were stated in Tier 1 without full explanation:

- **Concept 7 (Stateless Transformations):** `dropDuplicates` is an exception to
  the stateless rule — it is stateful with *unbounded* state, because the engine
  must remember every key it has ever seen to detect a duplicate.
- **Concept 5 (Output Modes):** `append` mode is illegal for aggregations without
  a watermark.

Both have the same root cause. This concept is where you dig it up.

---

## From batch aggregation to streaming aggregation

Start from something familiar: a batch aggregation.

```scala
df.groupBy("sensorId").agg(sum("reading").as("total"))
```

Spark knows the full dataset. It shuffles all rows for each `sensorId` onto one
partition, computes `sum`, emits one output row per group, and exits. The whole
computation is a **single bounded pass**. There is no state to carry between passes
because there is only one pass.

Now apply the identical query to a streaming DataFrame:

```scala
streamingDF.groupBy("sensorId").agg(sum("reading").as("total"))
```

Data arrives in micro-batches. Batch 1 brings sensors A, B, C. Batch 2 brings A
and D. Batch 3 brings A and B again. The correct answer to `"total reading for
sensor A"` requires accumulating across **all micro-batches ever processed** — not
just the current one.

That cross-batch accumulation is **state**. It lives not in the micro-batch
DataFrame, which is ephemeral, but in the **state store**: a durable, per-partition
key-value structure that persists between triggers. The checkpoint's `state/`
directory (Tier 1, Concept 8) is where the state store is written to disk.

---

## The execution model, concretely

For each trigger:
1. Read new micro-batch from source
2. For each key in the batch:
    *  Load prior accumulated value for that key from the state store
    *  Apply the aggregation update (e.g., add new reading to running sum)
    * Write the updated value back to the state store
3. Emit output according to output mode

This is the `foldLeft` from Tier 0 — `stream.foldLeft(emptyState)(f)` — running
one micro-batch at a time, with the running state persisted between steps.

**What the state store holds** depends on the aggregation function. For `count`,
it is `key → running_count`. For `avg`, Spark cannot store just a running average
(averages don't merge incrementally without extra information), so it stores
`key → (running_sum, running_count)` — whatever is needed to correctly merge the
next incremental update. The state store entry size is a function of the
aggregation, not just the key cardinality.

---

## The memory problem: unbounded state growth

Every key ever grouped by creates an entry in the state store. In a bounded batch,
the key set is finite and known. In an unbounded stream, new keys arrive at any
time, and **old keys are never evicted unless you explicitly bound them**.

A stream of user click events grouped by `userId`, running for 30 days:

- Active users today: ~50,000 state entries, all receiving updates
- Users who clicked once on Day 1 and never returned: potentially hundreds of
  thousands of entries, none receiving updates, all still occupying state store
  memory and checkpoint disk space

This is **unbounded state growth**. It fails on two surfaces simultaneously:

1. **Executor heap:** state store partitions are kept in memory; as key count
   grows, JVM heap pressure eventually causes OOM.
2. **Checkpoint storage:** the state store is checkpointed to disk on every
   trigger. Unbounded key growth means unbounded disk growth.

This is the most common cause of OOM errors in production stateful streaming
pipelines. It is not a theoretical concern.

**Watermarks are the mechanism that solves this problem.** A watermark tells Spark
"no event older than time T will arrive, so state for windows or groups that ended
before T can safely be evicted." That is Concept 4 (Watermarks Part 1). For now,
hold the problem clearly: **stateful aggregation without watermarks = unbounded
state growth.**

---

## Why `append` is illegal without a watermark — derived

`append` mode promises: emit a row **only when it is finalized** — it will never
change again.

For a streaming aggregation, when is a group's aggregate ever final? Only when the
engine is certain that no future micro-batch will bring more data for that group.
Without a watermark, the engine has no such information — the next batch could
always contain another event for any key. So the engine correctly refuses to emit
anything in `append` mode: it has nothing it can legally call final.

With a watermark, the engine knows "no event with a timestamp older than T will
arrive." Groups whose window or time range falls entirely before T are finalized
and can be emitted.

**`append` without a watermark is a plan-time error.** Spark detects it at query
start and refuses to run the query — the correctness failure is caught before any
data is processed.

`update` without a watermark is different: the query runs and produces correct
output (it makes no finality claim — it only says "here is what changed this
batch"). But the state store still grows without bound. The resource failure bites
you in production, not at query start.

Watermarks fix both simultaneously: they bound state growth (fixing `update`'s
resource problem) and enable finality (unlocking `append`).

---

## Output modes for aggregations, with depth

**`complete`**: after each micro-batch, emit the **entire current result table** —
all groups, all current aggregate values. Safe without watermarks (no finality
claim). Scales badly: 10 million groups means writing 10 million rows to the sink
on every trigger, even if only a handful changed.

**`update`**: after each micro-batch, emit only the **rows whose aggregate value
changed** in this batch. This is the change-data-capture direction of the duality
(Tier 0): the delta of the result table, not the full snapshot. Legal without
watermarks; correct but unbounded state without them. The right pairing for
upsert-capable sinks (Delta `MERGE`, Postgres `ON CONFLICT`) — the sink must
apply the delta, not append it.

**`append`**: emit only **finalized** rows — groups whose aggregate will never
change. Requires watermarks. The right choice when downstream consumers should
only see complete, immutable results.

---

## A concrete example

```scala
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

val sensorStream = spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("subscribe", "sensors")
  .load()
  .select(
    split(col("value").cast("string"), ",").getItem(0).as("sensorId"),
    split(col("value").cast("string"), ",").getItem(1).cast("double").as("reading")
  )

val aggregated = sensorStream
  .groupBy("sensorId")
  .agg(
    sum("reading").as("totalReading"),
    count("*").as("eventCount")
  )

aggregated
  .writeStream
  .outputMode("complete")          // full result table on every trigger
  .format("console")
  .option("truncate", false)
  .trigger(Trigger.ProcessingTime("10 seconds"))
  .option("checkpointLocation", "/tmp/agg-demo/_checkpoint")
  .start()
```

What happens on trigger N (sensors A, A, B arrive):

1. Engine reads micro-batch: two records for A, one for B.
2. State store loads current totals for A and B.
3. Engine merges: A's total += two new readings; B's total += one new reading.
4. State store writes back updated totals for A and B.
5. Because output mode is `complete`, engine reads the **full** state store and
   emits every group — not just A and B — to the console.

On trigger N+1 with no new data, in `complete` mode, the full result table is
re-emitted unchanged. This is what makes `complete` expensive at scale.

---

## Operational note: `spark.sql.shuffle.partitions` is pinned at query start

The number of shuffle partitions determines the number of state store partitions —
physically separate files in the checkpoint's `state/` directory. This value is
**fixed when the query first commits and cannot be changed without breaking the
checkpoint** (a structural change, per Tier 1, Concept 8). The default is 200.

Picking this wrong at query design time means either over-partitioning (200 state
store partition files for a stream with 5 keys) or under-partitioning (200
partitions cannot hold hundreds of millions of keys without unacceptable state
store file sizes). The correct value is chosen before the first commit, not tuned
later. Full treatment in Tier 4.

---

## Spark 3.x → 4.x note

No gap here. The semantics of `groupBy().agg()` on a streaming DataFrame, the
state store execution model, and the output mode rules are unchanged between Spark
3.x and 4.x. Version differences appear at the state store implementation level
(RocksDB, changelog checkpointing — Tier 4) and the custom stateful API level
(`transformWithState` — Concept 9 of this tier). Learn this once; it is stable.

---

## Prove you got it

1. **The state store question.** A streaming query runs
   `groupBy("userId").agg(count("*").as("clicks"))` in `complete` mode. It has
   been running for 30 days. A user who clicked once on Day 1 and never returned:
   is their entry still in the state store? Why? What are the two concrete
   production surfaces that fail as a result?

2. **The derivation question.** Without consulting the output mode rules: using
   only what `update` and `append` each promise about the rows they emit, explain
   in one or two sentences why `update` is legal for an unwatermarked aggregation
   while `append` is not — and why `update` still carries a production risk even
   though it is legal.

<details>
<summary>Answers</summary>

1. Yes, their entry is still in the state store. The engine has no mechanism to
   evict state for a key without a watermark — it cannot know whether that user
   will click again in a future batch. The two failure surfaces are executor heap
   (state store partitions kept in memory; OOM as key count grows) and checkpoint
   storage (state store is written to disk on every trigger; unbounded key growth
   means unbounded disk growth).

2. `update` promises only "here is what changed this batch" — it makes no claim
   about finality, so it can emit correctly at any time without knowing whether
   more data for a group will arrive. `append` promises finality — "this row will
   never change" — which requires knowing that no future event can update the
   group; without a watermark the engine has no such knowledge and has nothing
   legal to emit. `update` is still risky without a watermark because the state
   store — even though output is correct — grows without bound as new keys
   accumulate and old ones are never evicted.

</details>

---

[← Tier 2 index](./README.md) · [Next: Event-Time Windows →](./02-event-time-windows.md)