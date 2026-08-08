# Deduplication

> **Tier 2 · Concept 6 of 9**
> A light concept and a direct payoff of the watermark work. Dedup is a keyed
> state set; the only question is when a key can be forgotten — and that's the
> watermark, again.

---

## Where this connects

Tier 1 Concept 7 flagged `dropDuplicates` as the exception to the stateless rule:
stateful with *unbounded* state, because to know whether an arriving record is a
duplicate the engine must remember every key it has ever seen. Named then, unpacked
now.

## Why dedup is needed

Most streaming sources are **at-least-once** (Tier 1 C3): a Kafka consumer restart
replays records, a producer retry double-emits. Duplicates are the normal case, and
dropping them is how you get exactly-once *effect*.

## The mechanism — a keyed state set

Dedup state is trivial: **the set of dedup keys seen so far.** Per record:

1. compute the dedup key (the subset columns you name)
2. already in state? → **drop the record**
3. else → **store the key, emit the record**

The only interesting question is the recurring one: **when can a key be forgotten?**

---

## `dropDuplicates` — unbounded

```scala
df.dropDuplicates("id")   // no watermark
```

The key can **never** be forgotten: a duplicate could arrive at any future time, so
every key stays in state forever. Same unbounded-state hazard as an unwatermarked
aggregation — a state leak on any long-running query.

## `dropDuplicates` + watermark — bounded, with a trap

Bound it by adding a watermark **and putting the event-time column in the subset**:

```scala
df.withWatermark("eventTime", "1 hour")
  .dropDuplicates("id", "eventTime")   // eventTime MUST be in the subset to evict
```

Keys older than the watermark now evict. But the timestamp is now *part of the
dedup key*, so two copies of the same event are recognized as duplicates only if
their `eventTime` is **byte-identical**. In reality a duplicate often arrives
re-stamped (re-ingested through a different path, clock skew) — so:

- put `eventTime` in the key → near-identical duplicates are seen as **distinct**,
  and slip through
- leave it out → state **never evicts**

Stuck between "doesn't dedup" and "doesn't clean up." This is the most-reported
dedup surprise, and the reason a second API exists.

## `dropDuplicatesWithinWatermark` — the fix (Spark 3.5+)

```scala
df.withWatermark("eventTime", "10 hours")
  .dropDuplicatesWithinWatermark("guid")   // no timestamp in the subset
```

Built for exactly that gap (SPARK-42931). De-dup on `guid` alone — the timestamp is
**not** in the key — while the watermark still bounds state. Requirements: streaming
DataFrame, a watermark defined, an event-time column present.

**The guarantee:** *events are deduplicated as long as the time distance between the
earliest and latest copies is smaller than the watermark delay.* So here the
threshold isn't primarily about lateness — it's about **how far apart two copies of
the same event can arrive and still be caught**. One-way guarantee, same shape as
Concept 4: within the threshold, always deduplicated; beyond it, best-effort.

## The eviction timing — one predicate, two uses (Concept 4 again)

For a key at event time `T`, watermark delay `D`:

- **admission:** a newly arriving record is dropped as too-late when `watermark >= T`
- **eviction:** the key is removed from state when `watermark >= T + D`

So a key lives exactly `D` of event-time past its own timestamp — which is *why* a
duplicate arriving within `D` still finds it in state. Same two-predicate split as
the aggregation and join operators.

## Sizing `D` — the double-duty knob

`D` is one value doing two jobs: as a **watermark** it bounds state and drops late
data (Concept 4); as a **de-dup horizon** it sets the max gap between catchable
duplicates. They coincide because eviction is `watermark >= T + D`. Size it to the
larger need — usually the max expected duplicate gap, plus buffer, since that
tends to exceed normal lateness. Docs: *set the delay threshold longer than the max
timestamp difference among duplicated events.*

## The decision, in one line

`dropDuplicatesWithinWatermark` is the production-correct choice when you know the
max time gap between duplicates. Plain `dropDuplicates` without a watermark leaks;
`dropDuplicates` *with* a watermark works only if duplicates carry identical
timestamps — rare in practice.

---

## The demo (empirical)

[See the demo using MemoryStream](../../src/main/scala/demos/tier2/DropDuplicatesWithinWatermarkDemo.scala)
dedup key `id` only, `D = 10s`, one batch per `addData`. Reading by `batchId`:
```
batch  input         wm  total   upd removed dropped  note
  0    A@5, A@5        0   1       1    0       0      same-batch dup: one out, one dropped
  1    B@12, C@13      0   3       2    0       0      two new ids stored
  2    C@15            3   3       0    0       0      DIFFERENT-timestamp dup of C (13 vs 15) -> dropped
  3    B@12            5   3       0    0       0      exact cross-batch dup -> dropped
  4    E@25            5   4       1    0       0      new id stored
  5    A@5            15   3       0    1       1      A's key evicted (W>=5+10); incoming A late-dropped
```

Four things the trace nails:

- **Batch 2 is the headline.** `C@15` is deduplicated against `C@13` despite the
  different timestamp (`upd=0`, sink unchanged) — the case plain
  `dropDuplicates("id","eventTime")` would miss.
- **A dropped duplicate still advances the watermark.** `C@15` is dropped as a
  duplicate (`upd=0`) yet the end-of-batch watermark still moves, because
  advancement is driven by *max event time processed* — independent of whether the
  row survived dedup. Watermark advance and row survival are **decoupled**: a row
  can be discarded by the operator and still push the clock forward.
- **Batch 5 shows the two-predicate split.** Inherited `W = 15`. Eviction condition
  `W >= T + D` fires for A's key (`15 >= 5 + 10`) → `removed=1`, `total` drops. The
  incoming `A@5` is itself not stored (`upd=0`) because its own time `5 < W` — late
  at admission. Same admission-vs-eviction split as Concept 4. Eviction boundary is
  **non-strict `>=`** (`W >= T + D`, per docs and trace) — *different* from the
  strict `<` found for stream–stream joins. Each operator's boundary is its own;
  don't assume one propagates.
- **`numRowsDroppedByWatermark` is unreliable as a drop signal.** Here it was `0`
  for the dedup drops in batches 2–3 (a duplicate is simply *not stored and not
  emitted*, incrementing nothing) but *did* fire (`dropped=1`) in batch 5, where
  the re-arriving `A@5` was rejected as late-at-admission. So it is **sometimes
  populated, not - never** — it counts late-admission drops but not dedup drops. The
  reliable signal for "was this deduplicated" is `upd=0` + the sink not growing, not
  this metric. (Operator name in the trace: `dedupeWithinWatermark`.)

---

## Spark 3.x → 4.x note

`dropDuplicatesWithinWatermark` is **new in Spark 3.5**. On a 2026 portfolio,
knowing the watermarked variant (and *why* it beats putting the timestamp in the
subset) is the current-expertise signal.

---

## Prove you got it

1. The same event arrives as `(id=7, eventTime=10:00:00.000)` and
   `(id=7, eventTime=10:00:00.500)`. You run
   `withWatermark("eventTime","1 hour").dropDuplicates("id","eventTime")`. Caught?
   Why? Which API catches it, configured how?
2. Why does `dropDuplicates("id")` with no watermark leak, while
   `dropDuplicatesWithinWatermark("id")` does not — in terms of when a key leaves
   state?

<details>
<summary>Answers</summary>

1. **Not caught.** `eventTime` is in the dedup key, so the two rows are distinct
   keys (`.000` ≠ `.500`) and the second passes through. Use
   `dropDuplicatesWithinWatermark("id")` (id only, no timestamp), with the watermark
   delay set to at least the max expected gap between duplicates plus buffer.
2. Without a watermark the engine has no rule to forget a key — a duplicate could
   arrive at any future time — so keys accumulate forever. With
   `dropDuplicatesWithinWatermark`, a key at event time `T` is evicted once
   `watermark >= T + D`, bounding state to a `D`-sized event-time horizon.

</details>

---

[← Concept 5: Streaming Joins](./05-stream-joins.md) · [Next: Arbitrary Stateful Processing (legacy) →](./07-arbitrary-stateful-legacy-part1.md)