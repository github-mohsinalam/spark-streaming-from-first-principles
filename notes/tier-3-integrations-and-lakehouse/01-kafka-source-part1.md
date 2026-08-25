# Kafka as a Source — Part 1: Position, Ownership and Rate Limiting

> **Tier 3 · Concept 1 of N — Part 1 of 2**
> Tier 1 proved Kafka satisfies replayability. That was a property of *a log*.
> A topic is N logs, and everything hard here follows from that one fact.
>
> **The tier's shift:** Tiers 0–2 were engine semantics, provable in one JVM with
> a `MemoryStream`. The failure mode was *wrong output*. From here the failure
> mode is *wrong after a restart*, so the demos span process lifetimes and run
> against real infrastructure.

---

## The gap Tier 1 left

Tier 1 established that Kafka is addressable, deterministically re-readable and
monotonic, that offsets live in the checkpoint rather than in a consumer group,
and that the offset log stores the *next* offset to read. Sufficient for
exactly-once *effect* — in principle.

Five things a pipeline built on exactly that still gets wrong:

1. Query built with `startingOffsets = "earliest"`, down over a weekend. Before
   restarting you change it to `"latest"` to skip the backlog. What happens?
2. Ops asks "how far behind are we?" and points at a Kafka lag dashboard reading
   consumer-group offsets. It shows nothing.
3. Retention is 7 days; the pipeline was down 9. Loss, exception, or silence?
4. Someone grows a 6-partition topic to 12 mid-flight. Which records arrive?
5. After 9 hours down the first batch plans 40M records, runs 50 minutes, and an
   executor dies at minute 49. What was made durable?

None follow from "Kafka is replayable". Each is a question about **what shape the
position has, which system holds it, and what happens when the two systems
disagree.**

---

## The position is a vector

"Offset" was scalar in Tier 1 because we reasoned about *a* log. A topic is N
logs with no total order across them — there is no fact about whether p0's record
500 precedes p3's record 500. So the position is a map from `(topic, partition)`
to a number, and three things follow immediately:

- **Parallelism unit = partition.** The vector's key set is the set of
  independent cursors, so it is also the natural task count.
- **Ordering is per-partition only.** Nothing else was ever promised.
- **The key set is dynamic.** Partitions can appear. A vector whose *domain*
  changes is a different thing to checkpoint than a number — §*Domain change*.

### The cross-tier consequence

The watermark is `max(eventTime seen) − threshold`, computed over the whole
micro-batch, which spans every partition. So a partition whose event times run
*ahead* raises the watermark for rows from a partition running *behind*, and
those rows arrive already below it and are dropped on admission.

**Partition skew is an event-time correctness problem, not only a throughput
one.** Tier 2 could not surface this: `MemoryStream` has one cursor.

The sharpest instance is catch-up. One partition holds a 9-hour backlog while
the others are live; the batch mixes 9-hour-old rows with current ones, the
watermark is set by the current ones, and the backlog is admitted straight into
the drop predicate. **The outage's data survives in Kafka and is destroyed by the
watermark on the way in.**

Neither Tier 2's semantics nor Tier 3's topology is at fault. The mitigation is
at this layer — producer partitioning, or a watermark threshold sized to observed
skew, paying state for it — never by changing watermark semantics.

---

## Who owns the position

Kafka has a built-in place to store a consumer's position. Two reasons it cannot
be the authority.

**It records the wrong fact.** Restart must *reproduce batch N exactly*, which
requires the boundary `[start, end)` to be known before batch N runs and to
survive a crash mid-batch. A group commit is written *after* processing and says
"done up to here". Reconstructing the batch as "from committed to whatever is
latest now" yields a *different* batch — different aggregation increments,
different watermark, different state. Deterministic re-read is worthless if you
do not re-read the same range.

Put precisely: **a group offset is one scalar per partition, and one scalar
cannot distinguish "I planned to read to Y" from "I finished up to Y".** Recovery
needs both facts. Spark needs two logs because it needs two facts; Kafka stores
only the second, because a plain consumer has no notion of a batch to replan.

**It is a competing planner.** The group protocol assigns partitions to members
and rebalances them — a second authority deciding who reads what, while Spark's
driver must plan the whole batch itself. Spark's own error path knows this: when
partitions disappear, the message changes to blame a user-set `group.id` if one
is present. [3]

So position lives where the plan lives, and is written **before** execution:

```
offsets/N   the PLAN for batch N   — written ahead
commits/N   batch N COMPLETED      — written after
```

`CommitLog`'s class doc states the ordering outright: obtain batch offsets and
write to the offset log · process · write to the completion log · trigger the
next batch. [6]

### Consequence: Spark commits nothing to Kafka

```scala
override def commit(end: Offset): Unit = {}
```
[1]

Executors *do* get a generated group id —
`spark-kafka-source-<uuid>-<checkpointPath.hashCode>-executor` [2] — and three
things are legible in that string: a **fresh UUID per query start**, so any
committed offset would go to a group that never recurs; the **checkpoint path's
hash**, confirming the direction of ownership; and a `-driver`/`-executor` split.

But the group **never forms**. Executors call `assign()`, not `subscribe()`,
because the driver already fixed the ranges — and under `assign()` the group id is
inert unless something calls `commitSync`. Measured: no group with that prefix
appears in the cluster at all, and no group *anywhere* holds committed offsets
for the topic.

**Operational consequence.** The group id is visible in broker logs, client
metrics and quota config — everywhere a group id normally appears — so people
assume group-based lag monitoring should work. It silently reports nothing. Not
because Spark forgot to commit, but because there is no group to monitor by
design.

### So lag comes from the driver

The driver already fetches latest-available every trigger **in order to plan**,
so it reports the difference for free: `minOffsetsBehindLatest`,
`maxOffsetsBehindLatest`, `avgOffsetsBehindLatest`, on each source's entry in
`StreamingQueryProgress`. [1] That is the Tier 4 lag signal, and it is Spark-side
only.

### End-exclusive, corroborated

The connector's class doc gives the reason Tier 1 inferred: the stored offset is
one past the last available record, to stay consistent with
`KafkaConsumer.position()`. [1] So batch N's end is batch N+1's start — one
number, and `[X, Y)` composes.

---

## Birth: the one decision the checkpoint cannot inherit

The checkpoint owns the position, but on the first run there is none. So
initialization must exist, and must itself be **durably recorded** — otherwise
"start from latest" resolves to a different instant every time the process
restarts before batch 0 commits.

```scala
metadataLog.get(0).getOrElse {
  val offsets = startingOffsets match { ... }
  metadataLog.add(0, offsets)
  offsets
}
```
[1]

It is a `getOrElse`, not a policy. **Editing `startingOffsets` before a restart
does nothing whatsoever.**

That log is the source's own, at `<checkpoint>/sources/<sourceIdx>/0`:

- **First `0` = source index** — position in the query plan. Two `readStream`
  calls give `sources/0/` and `sources/1/`, which is also why reordering sources
  breaks a checkpoint.
- **Second `0` = batch id 0** in an `HDFSMetadataLog`, one entry ever written.
  Not a partition. Partitions live inside the JSON.

The file looks binary because byte 0 is a NUL, written purely so a Spark 2.1.0
reader could be told apart from later ones (SPARK-19517). [4] After it:
`v1\n{"topic":{"0":0,"1":0,"2":0}}`.

> **The folk remedy that doesn't work.** "Delete the offset log to re-read from
> earliest." Delete `offsets/` and `commits/` but leave `sources/`, and
> `startingOffsets` is *still* never consulted. Only a genuinely new checkpoint
> path re-runs the resolution.

---

## When the two systems disagree

The checkpoint says next read is 12,300,000; retention has moved Kafka's earliest
to 40,000,000. The engine faces a choice it has no basis to make — **skip
forward** (silent data loss) or **fail** (outage). Neither is correct in general,
so it must be declared, and the safe default is the loud one:
`failOnDataLoss`, default `true`. [7]

```scala
private def reportDataLoss(message: String, getException: () => Throwable): Unit = {
  if (failOnDataLoss) throw getException()
  else logWarning(...)
}
```
[1] Detection is split: the driver catches structural loss (partitions gone,
offsets moved) [3]; the executor catches an out-of-range seek and, when the flag
is `false`, recovers from earliest with a warning. [5]

Setting it to `false` repairs nothing. What that costs:

**Observable:** the batch succeeds; output resumes from the current earliest; a
`WARN` on the driver and on affected executors. That is the entire signal.

**Not observable:**

- Two days of records are absent and **nothing counts them**. `numInputRows`
  reports what was read, not what was skipped. No metric, no dead-letter.
- The gap is **per-partition and unequal** — each partition's earliest depends on
  its own segment rolling — so the hole is ragged and unreconstructable.
- Downstream it is **indistinguishable from "nothing happened"**. A windowed count
  emits low numbers, not missing rows; a `transformWithState` processor sees
  devices that appear to have gone silent.
- The **event-time jump** advances the watermark by days in one step, so genuinely
  late data still inside the retained window is dropped as collateral.

> `failOnDataLoss = false` converts a loud, correct failure into a silent,
> permanent one. Defensible only where the gap is already an accepted decision
> and an independent backfill path exists.

---

## Domain change: partitions appearing

New partitions have no checkpoint entry, and any starting point other than
*earliest available* silently skips whatever was written between partition
creation and the next plan — invisibly, unlike ordinary late data. So: earliest,
unconditionally, regardless of `startingOffsets`.

```scala
val newPartitions = untilPartitionOffsets.keySet.diff(fromPartitionOffsets.keySet)
val newPartitionInitialOffsets = fetchEarliestOffsets(newPartitions.toSeq)
...
newPartitionInitialOffsets.filter(_._2 != 0).foreach { case (p, o) =>
  reportDataLoss(s"Added partition $p starts from $o instead of 0. ...", ...) }
```
[3]

Three things worth carrying:

- "New" means **absent from the previous batch's offset map**, discovered at
  **planning time**, not mid-batch.
- A non-zero earliest means retention already ate the head, so it routes through
  `reportDataLoss` — with the default, it **throws**.
- The commoner real trigger is not a genuinely new partition but a **topic newly
  matching a `subscribePattern`**: its partitions are new to this checkpoint but
  old in Kafka, so their earliest is rarely 0 and the query dies on discovery.

---

## Rate limiting: how big is a batch?

### Three quantities, not one

Backlog **B**, arrival **R**/sec, capacity **P**/sec.

| | what it is | set by |
|---|---|---|
| **Catch-up time** | ≈ `B / (P − R)` — how long until live again | arithmetic; **the cap does not reduce it** |
| **Peak batch footprint** | shuffle volume, executor memory, rows into a stateful operator | **B** if uncapped — i.e. by how long you were down |
| **Redo cost on failure** | the batch is atomic; a batch that dies redoes whole | **B** if uncapped |

The cap buys the second and third: it makes per-batch cost and per-failure redo
functions of a number *you* chose rather than of an outage's duration. There is
no fix for the first.

### Why the redo problem has no escape

On restart Spark does **not** replan the failed batch:

```scala
offsetLog.getLatest() match {
  case Some((latestBatchId, nextOffsets)) =>
    /* First assume that we are re-executing the latest known batch
     * in the offset log */
    execCtx.batchId = latestBatchId
    execCtx.isCurrentBatchConstructed = true
    execCtx.endOffsets = nextOffsets.toStreamProgress(sources)
```
[8] The commit log is consulted only to decide whether to *advance past* it.

So the recorded range is binding. Setting `maxOffsetsPerTrigger` and restarting
**does not shrink a batch already in the offset log** — it applies from the next
*planned* batch. If the batch failed because it was too big, the restart fails
identically. **A crash loop no config change escapes**; the exits are more
resources or a new checkpoint.

### What the config is

`maxOffsetsPerTrigger` caps **how many Kafka records one micro-batch may plan to
read, summed across all partitions.** Three things it is not, because the name
misleads on all three: not per partition, not a rate (no time unit), not bytes.
Unset, there is no ceiling.

The last point matters more than it sounds. A 1000-record batch could be 50 KB or
5 GB. With variable payloads — a topic mixing 1 KB events with 500 KB blobs — the
cap is a weak proxy for the thing you actually care about. There is no byte-based
limit on the Kafka source, so you size for the worst-case record, or keep large
payloads out of the topic and publish a pointer. A schema decision forced by a
rate-limiting constraint.

### How a batch is planned

Every trigger, before a record moves:

1. **Find the ceiling** — ask Kafka each partition's latest offset. Call it `until`.
2. **Pull it down to the budget** — if the cap is set, reduce each partition's end
   so the total fits. Unset: skip; `until` is the plan.
3. **Write it down** — the map goes to `offsets/N`. *Now* execution starts.

The batch's identity is that map.

### Step 2, worked

Budget 1000 against backlogs 9000 / 900 / 10 (total 9,910) — so this batch takes
about 10% of what is waiting, and each partition gets 10% **of its own backlog**:

| | backlog | share(of backlog) | 1000 (cap) × share | rounded | this batch |
|---|---|-------------------|--------------------|---|---|
| p0 | 9,000 | 90.8%             | 908.17             | 908 | `begin₀ … +908` |
| p1 | 900 | 9.08%             | 90.82              | 90 | `begin₁ … +90` |
| p2 | 10 | 0.10%             | 1.009              | 1 | `begin₂ … +1` |

**999 records** — not 1000. Measured on a fresh topic, where begins are 0 and the
planned ends *are* the counts: `offsets/0 == {p0:908, p1:90, p2:1}`. [11]

### Why proportional

**Equal split** — 333 each — is the obvious alternative and it fails twice here:
p2 takes only its 10 and wastes 323 of the budget, while p0 takes 333 of 9,000
and needs 27 batches. Effective batch: 343 against a budget of 1000, full
per-batch overhead for a third of the work.

Proportional avoids both and yields a property worth naming:

> **Every partition drains in the same number of batches.**

A partition's backlog *in records* is roughly (how far behind in time) × (its
arrival rate). At similar rates, p0 holding 10× p1's records means it is 10×
further behind in time — so draining proportionally returns them to the present
**together**. Under stable rates, proportional allocation is exactly the policy
that keeps partitions **time-aligned**. Measured: all three drained within one
batch of each other. [11]

**Where it breaks is rate skew.** If p0 is high-volume and p1 low-volume, p0's
9,000 records might span an hour while p1's 900 span nine hours. Clearing 10% of
each puts p0's recent data and p1's nine-hour-old data in one batch, the watermark
is set by p0, and p1's rows are dropped on admission.

**That is the event-time skew failure above, and this is its cause** — not
partition lag in general, but *unequal arrival rates*, which the planner cannot
see because it measures backlog in records while correctness is measured in time.

### The starvation guard

At a backlog of 9 rather than 10, p2's share is `1000 × 9/9909 = 0.908` — floors
to **zero**. And its share stays roughly constant as everything drains
proportionally, so it would advance zero offsets **forever** while the query
reported perfect health.

So the planner rounds *up* whenever the computed share is below 1. The source
comment: *"Don't completely starve small topicpartitions."* [1] Any partition with
at least one unread record advances by at least one per batch.

### The cap is not a hard cap

The guard only rounds up. If enough partitions each compute a share below 1, they
all round to 1 and the batch exceeds the cap. Since balanced partitions give
`sizeᵢ/total ≈ 1/N`, the condition is roughly:

> **(partitions holding unread data) > `maxOffsetsPerTrigger`**

Measured: 20 partitions holding 3 records each, cap 5 → every share is
`5 × 3/60 = 0.25` → every partition rounds to 1 → **every batch read 20 records,
4× the cap**, with `offsets/0 == 1` on all twenty. [11]

Two consequences:

- **The effective floor on batch size is the number of partitions holding unread
  data.** No value of the cap goes below it. Size executor memory for
  `max(cap, numPartitions)`.
- **It is invisible in steady state.** Few partitions hold a backlog at any
  instant, so the condition fails and the cap looks tight. It appears only while
  draining a backlog — *when the pipeline is already degraded and least able to
  absorb it.* A capacity problem that only manifests during recovery is the worst
  shape a capacity problem can have. In particular, a topic repartitioned from 12
  to 400 for producer throughput changes the streaming job's memory profile
  without anyone touching the streaming job.

### Choosing a value

Steady state, the cap almost never binds — you read what arrives, which is under
the cap or you are permanently behind. It engages only against a backlog. So the
question is not "how fast do I want to go" but:

> **What is the largest batch this cluster can process without dying, and how
> long am I willing to redo if one fails?**

**`maxOffsetsPerTrigger` is a recovery-time decision, not a throughput control.**

### The companion limits

- `minOffsetsPerTrigger` + `maxTriggerDelay` — the dual problem, batches so small
  that fixed per-batch cost dominates. Composed as a `CompositeReadLimit`: the min
  decides *whether* to run a batch (a skip decision), the max decides *how big*.
  `ReadAllAvailable` outranks both. [1]
- `maxRecordsPerPartition` is **not** a rate limit despite the name — it is
  consumed by `KafkaOffsetRangeCalculator` to split a partition's range across
  more Spark tasks. A parallelism knob. Part 2.
- **`Trigger.Once` discards your rate limit**; `Trigger.AvailableNow` keeps it.
  That is the whole reason the latter superseded the former:

```scala
case _: SingleBatchExecutor =>   // Once
  ... logWarning("The read limit ... is ignored when Trigger.Once is used.")
      s -> ReadLimit.allAvailable()
case _: MultiBatchExecutor =>    // AvailableNow
  ... s.prepareForTriggerAvailableNow(); s -> s.getDefaultReadLimit
```
[8]

`AvailableNow` adds two things beyond the cap: a **frozen ceiling** (latest
offsets fetched once and cached, so the work is bounded at start [1]) and
**termination**. It does **not** rescue a batch already in the offset log —
prevention, not recovery. And mid-run partition changes are fatal:
`verifyEndOffsetForTriggerAvailableNow` throws if the partition set differs from
the prefetched one. [1]

So the pattern splits: **continuously-running pipeline** → set the cap
permanently, sized to steady-state capacity with headroom, and outage recovery
needs no intervention. **`AvailableNow` + cap** → the backfill / scheduled-batch
shape, and the "reprocess into a new checkpoint" escape from a crash loop.

Changing the trigger across restarts is safe: the offset-log metadata carries
`batchWatermarkMs`, `batchTimestampMs` and SQL conf — not the trigger. [8]

---

## Found while demoing: an NPE on the restart path

The demo crashed reproducibly on the batch immediately after restart, *after* the
batch succeeded:

```
java.lang.NullPointerException: Cannot invoke "scala.collection.IterableOps.map(...)"
  because the return value of "scala.Option.get()" is null
  at KafkaMicroBatchStream$.metrics(KafkaMicroBatchStream.scala:520)
  at ProgressContext.extractSourceProgress(ProgressReporter.scala:379)
  at ProgressContext.finishTrigger(ProgressReporter.scala:312)
```

`latestPartitionOffsets` is assigned only inside `latestOffset(start, readLimit)`
— the **planning** call. A restart onto an uncommitted batch sets
`isCurrentBatchConstructed = true` and **skips planning**, so the field is still
`null`. `metrics` then builds `Some(latestPartitionOffsets)`, the `isDefined`
guard passes because `Some(null).isDefined` is `true`, and the next line
dereferences it. [1] There is no escape hatch: `extractSourceProgress` calls
`metrics` unconditionally for any `ReportsSourceMetrics` source — no config, no
`try`. [9]

The fix is `Option(latestPartitionOffsets)`, which collapses to `None`.

Two things make this worth recording rather than working around:

- **The batch's commit is already durable when it fires.** `commitLog.add` runs
  inside `runBatch`, which returns before `finishTrigger`. [8] So the query dies
  but does not loop — a second restart proceeds normally, and every later batch is
  planned, so metrics work again.
- **It is a defect that exists only on the restart path** — the tier's thesis in
  miniature, and precisely the class of bug a `MemoryStream` test cannot reach.

> **Unverified:** whether this is a filed JIRA. Worth searching SPARK for
> `latestPartitionOffsets` before treating it as new.

---

## The demo and the probe

[`demos/tier3/KafkaOffsetOwnershipDemo.scala`](../../src/main/scala/demos/tier3/KafkaOffsetOwnershipDemo.scala)
— four stages, **four separate JVM runs**, one topic and one checkpoint. What
survives a restart cannot be shown inside one process.

The crash is simulated by throwing from `foreachBatch` on a chosen `batchId`,
not by `kill -9`. A hard kill is realistic but non-deterministic — you cannot
choose the batch nor reliably land before the commit-log write. The checkpoint
state left behind is identical at the only boundary that matters: `offsets/N`
written, `commits/N` absent.

| stage | proves |
|---|---|
| 1 | `offsets/1` exists, `commits/1` does not · `sources/0/0` holds resolved zeros · batch 0 planned **48 records, 16 per partition**, not 50 |
| 2a | batch 1 **re-executed the recorded range** with 300 more records available and ignored · `offsets/1` unchanged · `startingOffsets=latest` ignored · `sources/0/0` unchanged · commit durable before progress reporting |
| 2b | no group with Spark's prefix has **formed** · **no group anywhere** holds offsets for the topic · driver reports `offsetsBehindLatest` |
| 3 | 20 records from each newly added partition arrived under `startingOffsets=latest` |

[`demos/tier3/KafkaRateLimitProbe.scala`](../../src/main/scala/demos/tier3/KafkaRateLimitProbe.scala)
— `Trigger.AvailableNow`, fixed backlogs, offset log read back after the run.
Case A: `{908, 90, 1}`, 999 records, all partitions drained within one batch of
each other. Case B: 20 partitions × 3 records at cap 5 → every batch 20 records,
`offsets/0 == 1` everywhere.

Supporting: [`KafkaTestOps.scala`](../../src/main/scala/demos/tier3/KafkaTestOps.scala)
(exact per-partition production, admin ops, group inspection) and
[`CheckpointInspector.scala`](../../src/main/scala/demos/tier3/CheckpointInspector.scala)
(offset log, commit log, source initial offsets — distinct from Tier 1's
Delta-oriented inspector).

### Deliberately not proven here

- **`failOnDataLoss` / retention loss.** Reproducing it locally means fighting
  `retention.ms` and `segment.ms` for a flaky result. Better as a Tier 5 runbook
  item than a demo.
- **Event-time skew dropping rows.** Real and demonstrable, but needs a stateful
  query; own demo.
- **OOM from the wide-topic overshoot.** The record-count overshoot is measured;
  the memory consequence is a sizing argument, not a local experiment.

---

## Spark 3.x → 4.x

- Internals moved: `MicroBatchExecution`, `ProgressReporter`, `TriggerExecutor`,
  `OffsetSeqLog`, `CommitLog` now under
  `org.apache.spark.sql.execution.streaming.{runtime,checkpointing}`.
- `KafkaMicroBatchStream` mixes in `SupportsRealTimeMode`, and real-time mode is
  **mutually exclusive with rate limiting**: `maxOffsetsPerTrigger`,
  `minOffsetsPerTrigger`, `minPartitions`, `endingTimestamp` and `maxTriggerDelay`
  all throw under it. [1]
- `maxRecordsPerPartition` exists in 4.1.2. [2] **Unverified:** which release
  introduced it, and likewise `minOffsetsPerTrigger` / `maxTriggerDelay`.
- **Caveat:** `docs/latest` currently renders 4.2.0, so the option table consulted
  is 4.2.0's [7]. Everything relied on from it is independently confirmed against
  v4.1.2 source.

---

## Prove you got it

1. A query runs for a month on `startingOffsets = "earliest"`. You stop it, delete
   nothing, change the option to `"latest"`, restart. Trace what the engine reads,
   in order, and say what the query does.
2. Give a crash point where `offsets/N` and `commits/N` differ, and say precisely
   what a consumer-group commit could not have recorded.
3. `failOnDataLoss = false` on a 7-day-retention topic after 9 days down. What is
   observable in the first successful batch, and what is not?
4. p0 carries events an hour ahead of p3; downstream has a 10-minute watermark.
   What happens to p3's rows, and whose bug is it?
5. Cap 1000, backlogs `{p0: 9000, p1: 900, p2: 10}`. What ends are planned, and
   why isn't p2 zero?
6. A partition added mid-query reports earliest `4,000` rather than `0`. What does
   Spark conclude, and is it right to?
7. Your executors can hold a 200 MB batch. Records average 2 MB. You set the cap
   to 100 and the topic has 400 partitions. When does this break, and why not
   before?

<details>
<summary>Answers</summary>

1. `getOrCreateInitialPartitionOffsets` finds `sources/0/0` present and returns it
   — `startingOffsets` is never evaluated. `populateStartOffsets` then reads the
   latest offset-log entry and compares against the commit log: committed →
   advance to `batchId + 1`; not committed → re-execute that batch verbatim. The
   edit has no effect. Deleting `offsets/` and `commits/` would not change this
   either; only a new checkpoint path would.
2. Crash after the offset log write and before completion: `offsets/N` present,
   `commits/N` absent, so N is re-executed with the recorded range. A group offset
   is one scalar conflating "planned to" and "finished at" — recovery needs both
   facts, and one number carries one. Reconstructing "from committed to current
   latest" gives a different batch each attempt.
3. **Observable:** the batch succeeds, output resumes from the current earliest, a
   `WARN` on driver and executors. **Not observable:** the ~2-day gap, uncounted by
   any metric; its ragged per-partition shape; the fact that downstream it is
   indistinguishable from "nothing happened"; and the watermark leaping forward,
   dropping still-retained late data as collateral.
4. The watermark is `max(eventTime) − threshold` over the whole batch, so p0 sets
   it and p3's rows are admitted already below it and dropped. Nobody's bug —
   Tier 2 semantics behaving as specified, exposed by the Tier 3 fact that one
   query's input is N logs with independent event-time frontiers. Fix at this
   layer: producer partitioning, or a threshold sized to the skew, paying state.
5. Total 9,910; each partition gets its own 10%: 908 / 90 / 1, so **999** records.
   p2's `1.009` floors to 1 here — but at a backlog of 9 its share would be 0.908
   and only the round-up guard would keep it moving. Without it, a partition below
   `1/cap` of the backlog would advance zero offsets forever while the query looked
   healthy.
6. That the head of the partition is already gone to retention, so records were
   missed. It routes through `reportDataLoss`, so with the default it throws.
   Right to: for a genuinely new partition, earliest should be 0, and anything else
   means data existed and is gone. The common real trigger is a topic newly
   matching a `subscribePattern` — old in Kafka, new to this checkpoint.
7. Not in steady state: few partitions hold a backlog at once, so shares stay
   above 1 and the cap binds normally. It breaks on the **first backlog drain** —
   say 1,200 records spread over 400 partitions. Each share is `100 × 3/1200 =
   0.25`, rounds up to 1, and the batch reads 400 records ≈ 800 MB against a 200 MB
   budget. The floor on batch size is the number of partitions holding unread data,
   and no cap value goes below it. Lowering the cap does not help — it makes more
   shares fall below 1, not fewer.

</details>

---

## Sources

All Spark source fetched firsthand at tag **`v4.1.2`**.

1. `connector/kafka-0-10-sql/.../KafkaMicroBatchStream.scala` — empty `commit`;
   end-offset convention; `getOrCreateInitialPartitionOffsets`; `rateLimit` and its
   starvation-guard comment; `getDefaultReadLimit` / `CompositeReadLimit`;
   `reportDataLoss`; `metrics` and the `Some(null)` defect;
   `prepareForTriggerAvailableNow`; `verifyEndOffsetForTriggerAvailableNow`;
   real-time-mode incompatibilities.
2. `.../KafkaSourceProvider.scala` — `streamingUniqueGroupId`;
   `MAX_RECORDS_PER_PARTITION_OPTION_KEY`; `CUSTOM_GROUP_ID_ERROR_MESSAGE`.
3. `.../KafkaOffsetReader.scala` — `getOffsetRangesFromResolvedOffsets`: new
   partitions from earliest, non-zero-earliest data loss, deleted-partition branch.
4. `.../KafkaSourceInitialOffsetWriter.scala` — leading NUL byte (SPARK-19517),
   `v1` line, offset JSON.
5. `.../consumer/KafkaDataConsumer.scala` — executor-side out-of-range recovery.
6. `sql/core/.../checkpointing/{OffsetSeqLog,CommitLog}.scala` — on-disk formats;
   `CommitLog`'s execution-order doc.
7. Structured Streaming + Kafka Integration Guide — option semantics and
   `failOnDataLoss` default. *Rendered from 4.2.0; cross-checked against v4.1.2
   source.*
8. `sql/core/.../runtime/MicroBatchExecution.scala` — `populateStartOffsets`;
   `commitLog.add` inside `runBatch`, before `finishTrigger`; trigger→read-limit
   wiring; `OffsetSeqMetadata` contents.
9. `sql/core/.../runtime/ProgressReporter.scala` — `extractSourceProgress` calling
   `metrics` unconditionally.
10. `sql/core/.../runtime/TriggerExecutor.scala` — `SingleBatchExecutor` vs
    `MultiBatchExecutor`.
11. Demos, this repo — `KafkaOffsetOwnershipDemo`, `KafkaRateLimitProbe`.

---

[Tier 3 index](./README.md) · [Next: Part 2 — parallelism, consumers, offset strategies →](01-kafka-source-part2.md)