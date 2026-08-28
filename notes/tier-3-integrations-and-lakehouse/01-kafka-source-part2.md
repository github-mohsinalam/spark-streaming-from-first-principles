# Kafka as a Source — Part 2: From Plan to Tasks

> **Tier 3 · Concept 1 — Part 2 of 3**
> Part 1 answered *how big is a batch* and stopped at `offsets/N`: a map of
> `[begin, end)` ranges, one per partition. That map says **what** to read. It
> says nothing about how it becomes work on a cluster. This part is that mapping,
> and the cost of changing it. Part 3 is consumers, memory and offset variants.

---

## What the plan left unspecified

For a 4-partition topic the plan is four offset ranges:

```
p0 [0, 1000)   p1 [0, 10)   p2 [0, 10)   p3 [0, 10)
```

An **offset range** is the unit throughout: which partition, from where, to where,
`size = until − from`. The question Part 1 never touched: **how many Spark tasks
do these become?**

By default, four — one task per range. Breaking that coupling is the whole of
this part.

---

## The default mapping, and the ceiling it imposes

A Kafka partition is one log, read sequentially from an offset, so the obvious
execution is one task per range: open a consumer, seek to `begin`, read to `end`.
That gives the first failure immediately:

> **Read parallelism is capped by the topic's partition count.**

Four partitions → four tasks → at most four cores busy, on a cluster of any size.
And partition count is a **producer-side decision** — set by whoever provisioned
the topic, for write-throughput and consumer-group reasons, possibly years ago and
certainly not with your job in mind.

This is the first thing in the tier you **cannot fix from inside your Spark
application** — your maximum parallelism was configured by another team, in
another system.

**Confirmed empirically** (no configs, backlog 9000/900/10):

```
p0 read=9000  p1 read=900  p2 read=10      → 3 tasks, one per partition
```

---

## Except the coupling isn't necessary

Tier 1's first property: Kafka is **addressable**. A consumer can `assign` a
partition and `seek` to any offset. Reading `[100, 200)` does not require having
read `[0, 100)` — not in the same task, not in the same process, not at all. So a
single partition's range is splittable: `[0, 1000)` can become five tasks reading
`[0,200) … [800,1000)` concurrently on five executors.

**What you give up:** those tasks run in parallel, so that partition's records are
no longer *processed* in offset order. Which is fine, because Spark never promised
it — a DataFrame is unordered, and anything that cares about order (windowing,
aggregation, stateful ops) works off the event-time column and a shuffle, not
arrival order. Kafka's per-partition ordering is a property of the *log* and
survives; only the ordering of *processing* is surrendered, and nothing
downstream relied on it.

So task count can be decoupled from partition count. Two knobs, because there are
two different questions.

---

## The two knobs and the three stages

**`minPartitions`** — a floor on total tasks for the batch. "Give me at least N."

**`maxRecordsPerPartition`** — a ceiling on records per task. "No task reads more
than M."

Not redundant. `minPartitions` is a *fixed count* against a *variable batch*: in
steady state the batch is small and N tasks each get a trivial range; during
catch-up the batch is huge and those same N tasks each get a huge range — the
moment you needed more parallelism, you get the same amount.
`maxRecordsPerPartition` keeps **task size stable across steady state and
recovery**, which is the same recovery-shaped concern as Part 1's rate cap.

> **Terminology trap:** `minPartitions` and the internal names say "partition"
> meaning **Spark task**, not Kafka partition. One word, two things.

Every batch, the ranges pass through three stages, in this fixed order. Both
configs are optional; unset, only stage 0 runs and you get one task per partition.

```
plan ranges
  │
 stage 0 ── drop ranges with size ≤ 0
  │
 stage 1 ── maxRecordsPerPartition: cap each range (skip if unset)
  │
 stage 2 ── minPartitions: split further if still too few tasks (skip if unset / enough)
  ▼
 final task list
```

### Stage 1 — `maxRecordsPerPartition` (local, per range)

Each range on its own: `parts = ceil(size / maxRecordsPerPartition)`, chop into
that many even pieces. No range is compared to any other — no budget, no
proportion. With `maxRecordsPerPartition = 3000` on backlog 9000/900/10:

| range | size | `ceil(size/3000)` | becomes |
|---|---|---|---|
| p0 | 9000 | 3 | three tasks of 3000 |
| p1 | 900 | 1 | unchanged |
| p2 | 10 | 1 | unchanged |

**5 tasks.** `ceil`, so no task exceeds the limit and sizes land *under* it.

### Stage 2 — `minPartitions` (global, proportional-with-exclusion)

Acts only if `minPartitions > current task count`. It must split the current
ranges into more tasks, allocating the extra proportionally to size. Naive
proportional overshoots, for a reason specific to task allocation.

**Why naive fails.** `minPartitions = 10`, four original ranges, `totalSize =
1030`:

| range | `round(size/1030 × 10)` | tasks |
|---|---|---|
| p0 (1000) | `round(9.71)` | 10 |
| p1/p2/p3 (10 each) | `round(0.097)` → floored to 1 | 1 each |

**13 tasks, not 10.** The small ranges each take one task regardless of the math —
a range can't become zero tasks — but their 30 records were still in `totalSize`,
diluting p0's share, and then they added a task on top anyway. They consumed
budget in the denominator and ignored the result.

**The two-phase fix.** Set aside the ranges that resolve to one task, then run the
proportional math on the rest with a reduced budget:

```
phase A (classify):  p0 → split ;  p1,p2,p3 → unsplit (1 task each)
phase B (recompute): unsplitCount = 3
                     splitRangeTotalSize = 1030 − 30 = 1000
                     budget = max(minPartitions − unsplitCount, 1) = 7
phase C (assign):    p0 → round(1000/1000 × 7) = 7 ;  p1,p2,p3 → 1
                     total = 10
```

`KafkaOffsetRangeCalculator.getRanges` (v4.1.2): `unsplitRanges`,
`splitRangeTotalSize`, `splitRangeMinPartitions = max(minPartitions −
unsplitRanges.size, 1)`. "Split" / "unsplit" are the two classes a range lands in.

**Dividing one range into N tasks** uses `remaining / (parts − part)` each step,
so p0 `[0,1000)` into 7 gives `142,143,143,143,143,143,143` — the remainder is
spread, not dumped on the last task.

### Both together — order matters

`maxRecordsPerPartition = 3000` **then** `minPartitions = 6` on 9000/900/10:

- Stage 1 → 5 tasks (p0 ×3 of 3000, p1, p2).
- Stage 2: `6 > 5`, acts. p0's pieces `round(3000/9000 × budget)` stay 1 each →
  **still 5**.

**Measured: 5 tasks. Not 6.** The doc says the count is *approximately*
`max(size/maxRecordsPerPartition, minPartitions)` — *"can be less or more
depending on rounding errors or partitions that didn't receive new data."* On the
same topic, `minPartitions = 7` yields **8**: the rounding boundary sits between 6
and 7, so the count jumps rather than tracking the request. **Treat `minPartitions`
as a hint, not a setting.**

The fixed order means `maxRecordsPerPartition` sets a hard task-size ceiling
first, and `minPartitions` can then only make tasks *smaller*. A hard bound stays
hard.

### The ceiling on `minPartitions`

- **Hard:** total records in the batch. 60 records → at most 60 tasks;
  `filter(_.size > 0)` drops the rest.
- **Soft:** the count is approximate in both directions (see above).
- **Practical:** your cluster. `minPartitions = 200` on 40 cores is 5 waves of
  queueing plus 200 consumer setups, not parallelism. Useful ceiling ≈ total core
  count, or a small multiple for straggler smoothing.

---

## The cost of splitting: locality, and both caches

The **no-config** path does one thing neither split path does:

```scala
offsetRanges.map { range =>
  range.copy(preferredLoc = getLocation(range.topicPartition, executorLocations))
}
```

`getLocation` is `floorMod(hash(topicPartition), numExecutors)` — a stable
assignment, so partition 3 lands on the same executor every batch and the consumer
it opened stays warm. Source comment: *the same topic-partition is preferentially
read from the same executor and the KafkaConsumer can be reused.*

Every range the divider produces is `KafkaOffsetRange(tp, from, until, None)` —
**`preferredLoc = None`**. So setting **either** config drops the whole batch off
the locality path, **including the small ranges that were never split** (p1, p2
come out of stage 2 as one task each, same offsets as before, but with no
preferred location).

Two consequences, and the second is the one to remember:

1. **Scheduling locality is lost.** Tasks land wherever the scheduler puts them.
   *Not verifiable on a single node* — on `local[*]` the driver and executor share
   a JVM, so every task reads `PROCESS_LOCAL` regardless. Confirmed in source;
   needs a multi-node cluster to observe. Noted, not chased.

2. **Consumer reuse breaks — this is the cost that actually bites.** The
   executor-side consumer cache and fetched-data cache (Part 3) are **keyed by
   `(groupId, topicPartition)` and hit only when a partition returns to the
   executor holding its warm consumer.** Locality is the mechanism that makes that
   happen. With `preferredLoc = None`, a partition may be read from a different
   executor each batch, so its cached consumer and pre-fetched buffer are never
   found: every task creates a consumer and issues a cold fetch. **On a wide topic
   with small partitions — the case where splitting buys least parallelism — this
   trades away the most cache benefit.** Also single-node-invisible (one JVM = one
   cache that always hits), so understood from construction, not measured here.

> **Rule for future-me:** `minPartitions` / `maxRecordsPerPartition` are not free
> parallelism. They forfeit locality-based consumer reuse for the entire batch.
> Reach for them when the partition-count ceiling genuinely blocks throughput —
> not by default.

---

## Spark 3.x → 4.x

`KafkaOffsetRangeCalculator` is stable across 3.x/4.x in shape. **Unverified:**
which release introduced `maxRecordsPerPartition` (present in 4.1.2). No behavioural
3→4 difference observed in the stage logic.

---

## Prove you got it (Part 2)

1. 4 partitions, 40 executors, a 4M-record batch taking 20 min. You set
   `minPartitions = 40`. What changes, and what caps how much it can help?
2. Why is `maxRecordsPerPartition` the better knob for an outage-surviving
   pipeline, when `minPartitions` looks like the more direct "use my cluster"?
3. `minPartitions = 8`, backlog `{p0: 800, p1: 5, p2: 5}`. Work out the task count
   and split, and say what you'd get without the unsplit-exclusion phase.
4. Splitting a partition's range processes its records out of offset order. Name
   something that genuinely breaks, or argue nothing does.
5. You set `maxRecordsPerPartition` on a 200-partition topic keeping up in steady
   state, where no partition exceeds the limit. The task numbers don't change.
   What *did* you change about every batch?

<details>
<summary>Answers</summary>

1. p0-style splitting spreads the batch across up to 40 tasks, so wall-clock read
   time drops toward `1/40`. Ceiling: total records (can't exceed one task per
   record) and, practically, core count — and you've now lost locality, so each of
   the 40 tasks pays consumer setup. Net win only because the batch is huge; on a
   small batch the setup would dominate.
2. `minPartitions` is a fixed count against a variable batch: same N tasks whether
   the batch is 60 records or 60M, so during catch-up each task is huge — no help
   when you need it. `maxRecordsPerPartition` bounds task *size*, so task size is
   the same in steady state and recovery; parallelism scales with the backlog
   automatically.
3. Stage 2 only (no `maxRecordsPerPartition`). Phase A: `p0 round(800/810 × 8) =
   round(7.9) = 8 → split`; p1,p2 → 1 → unsplit. Phase B: budget `max(8−2,1)=6`,
   splitTotal 800. Phase C: p0 `round(800/800 × 6)=6`; p1,p2 →1. **8 tasks.**
   Naive: p0 `round(800/810×8)=8` + p1,p2 at 1 each = **10** — overshoot, because
   p1,p2's 10 records diluted p0's share and then added tasks anyway.
4. Nothing breaks. DataFrames are unordered; ordering-sensitive operators use
   event-time + shuffle, not arrival order. Kafka's per-partition *log* order is
   untouched — only concurrent *processing* order changes, which nothing relied on.
5. Every range now carries `preferredLoc = None`, so the whole batch leaves the
   locality path. Same tasks, same offsets — but partitions no longer return to
   their warm consumers, so the consumer and fetched-data caches stop hitting and
   every task does a cold fetch. Invisible on one node; real on a cluster.

</details>

---

## Sources

Spark source fetched firsthand at tag **`v4.1.2`**.

1. `connector/kafka-0-10-sql/.../KafkaOffsetRangeCalculator.scala` — `getRanges`
   (stage 1/2, two-phase split), `getDividedPartition`, `getPartCount`,
   `getLocation`; class doc's "approximately… less or more" caveat;
   `KafkaOffsetRange(..., None)` on split ranges.
2. Executor INFO log, this repo — `log_with_no_configs.txt`,
   `log_with_minPartitions5.txt`.

---

[Tier 3 index](./README.md) ·
[← Part 1](01-kafka-source-part1.md) ·
[Part 3: consumers, memory, offset variants →](01-kafka-source-part3.md)