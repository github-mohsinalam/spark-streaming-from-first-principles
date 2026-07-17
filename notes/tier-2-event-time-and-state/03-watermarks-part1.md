# Watermarks (Part 1)

> **Tier 2 · Concept 3 of 9**
> The rule that decides when a window is finished. Everything here is either
> derived, or read off the Spark source — not inferred from the docs, which are
> imprecise on the one point that matters most.

---

## The problem

Concept 1: a streaming aggregation accumulates state per group, and without an
eviction rule that state grows forever. Concept 2: windowed aggregations make it
worse — new buckets open with the passage of time, not just with new keys.

Both need one question answered: **when is a window finished, so its state can be
dropped?**

The engine cannot answer this alone. It sees records in *arrival* order, not
*event-time* order, so it never knows whether the next batch will carry an old
timestamp. You have to tell it.

---

## The promise

```scala
df.withWatermark("eventTime", "10 seconds")
```

This is **a promise you make about your data**:

> No record will arrive whose event time is more than `threshold` behind the
> maximum event time already seen.

From that promise the engine derives one value:

```
watermark = (max event time seen so far) - threshold
```

- **Initial value is the epoch** (`1970-01-01T00:00:00Z`). Before any data there is
  no max, so nothing can be late. *(Observed directly in the demo trace, batch 0.)*
- **Monotonic.** `WatermarkTracker.updateWatermark` only assigns when
  `newWatermarkMs > prevWatermarkMs`, so it never regresses. [3]
- **Only `max` feeds it.** `StreamingQueryProgress.eventTime` also exposes `min` and
  `avg`, but those are for monitoring (event-skew diagnosis); the mechanism uses
  `max` alone. [5]

The engine cannot verify the promise. Everything below is correct *conditional on
it holding*. If real data is later than you claimed, it is silently dropped.

---

## The rule (from the source)

`WatermarkSupport.watermarkExpression` builds **one** expression: [3]

```scala
val evictionExpression =
  if (watermarkAttribute.dataType.isInstanceOf[StructType]) {
    LessThanOrEqual(
      GetStructField(watermarkAttribute, 1),      // ordinal 1 of {start, end} = END
      Literal(optionalWatermarkMs.get * 1000))    // ms -> microseconds
  } else {
    LessThanOrEqual(
      watermarkAttribute,                          // raw event-time column
      Literal(optionalWatermarkMs.get * 1000))
  }
```

For a windowed aggregation the watermark attribute *is* the `window` struct, and
ordinal 1 is `end`. So the rule is:

```
a row/window is "too old"  iff  window.end <= watermark
```

**Non-strict.** A window `[10, 15)` is evicted when the watermark reaches exactly
`15`.

### Why non-strict is the *correct* design, not an off-by-one

The window is **half-open**: `[10, 15)` holds events up to but *excluding* 15.

- A record can still update `W = [start, end)` iff its event time `e < end`.
- The promise guarantees every future record has `e >= watermark`.
- So a future record can reach `W` iff `watermark < end`.
- Contrapositive: **`watermark >= end` ⟹ nothing can reach `W`** ⟹ safe to evict.

At `watermark == end`, every event that could belong to the window has already
arrived. Requiring a strict `>` would hold the state one tick longer for zero
correctness benefit.

> **Doc discrepancy — worth knowing.** The Spark programming guide says state is
> retained until `(max event time - late threshold) > T` for "a window ending at
> time T". Read literally with `T = window.end`, that predicts *no* eviction at
> `watermark == end` — which contradicts the source and the observed behaviour.
> The guide is imprecise at the boundary. Trust `LessThanOrEqual`. [1] [3]

---

## One expression, two predicates

That single expression is compiled against **two different schemas**: [3]

```scala
lazy val watermarkPredicateForData =            // bound to the INPUT ROW
  watermarkExpression.map(Predicate.create(_, child.output))

lazy val watermarkPredicateForKeys =            // bound to the STATE KEY
  watermarkExpression.flatMap { e => newPredicate(e, keyExpressions) }
```

**"Dropped" and "evicted" are the same test, applied at two points in the plan.**

**The predicate is *not* `eventTime < watermark`.** This is the misconception to
kill. A record is not filtered because its own timestamp is below the watermark —
it is filtered because its **window's end** is `<= watermark`. In the trace below,
record `103` sits *below* the inherited watermark `104` and is still aggregated,
because its window `[100, 105)` has `end 105 > 104`. Timestamp-below-watermark
survives; window-end-past-watermark does not.

Why two predicates? Eviction removes dead windows from state. Without the *input* filter, a
late record for a dead window would create a **fresh state entry** — resurrecting
it, and (in append mode) emitting it a second time, breaking the emit-once
contract. The input filter prevents resurrection; the eviction scan removes what
has just died.

---

## The per-batch pipeline (`StateStoreSaveExec`, append mode)

```scala
case Some(Append) =>
  val filteredIter = iter.filter(row => !watermarkPredicateForData.get.eval(row))  // 2. FILTER
  while (filteredIter.hasNext) {
    stateManager.put(store, filteredIter.next().asInstanceOf[UnsafeRow])           // 3. WRITE
    numUpdatedStateRows += 1
  }
  val rangeIter = stateManager.iterator(store)                                     // 4. SCAN state
  new NextIterator[InternalRow] {
    override protected def getNext(): InternalRow = {
      ...
      if (watermarkPredicateForKeys.get.eval(rowPair.key)) {
        stateManager.remove(store, rowPair.key)                                    //    EVICT
        numRemovedStateRows += 1
        removedValueRow = rowPair.value
      }
      ... removedValueRow                                                          // 5. EMIT evicted row
    }
  }
```

Per trigger N:

1. **Inherit** the watermark computed at the end of batch N−1. Fixed for the batch.
2. **Filter input** — records are routed to windows and *pre-aggregated*, then rows
   with `window.end <= wm` are dropped → `numRowsDroppedByWatermark`.
3. **Write** survivors to state → `numRowsUpdated`.
4. **Scan the whole state store**, evict rows with `window.end <= wm` →
   `numRowsRemoved`.
5. **Emit** the evicted rows. *In append mode the output rows **are** the evicted
   rows.*
6. **Compute** the new watermark for batch N+1.

**Two invariants fall out of the code:**

- **Survivors are immune to their own batch's eviction.** A row that passed step 2
  has `window.end > wm`; the scan in step 4 uses the *same* `wm`, so it cannot
  match. Eviction only ever removes state written in *earlier* batches.
- **`numRowsDroppedByWatermark` is not a late-record count.** The filter runs on
  *pre-aggregated* rows, so N late records landing in one dead window register as
  **one** dropped row. Read it as zero vs non-zero. [1]

---

## The trace

`WatermarkMemoryStreamDemo.demoNotAFilter`, 5s tumbling windows, 10s threshold,
`noDataMicroBatches.enabled=false` (one batch per `addData` — see below).

```
batch 0 | in=2 | wm=  0 | total=2 upd=2 evicted=0 dropped=0   addData(103, 107)
batch 1 | in=2 | wm= 97 | total=3 upd=2 evicted=0 dropped=0   addData(114, 102)
batch 2 | in=4 | wm=104 | total=4 upd=2 evicted=0 dropped=1   addData(121, 103, 90, 92)
batch 3 | in=1 | wm=111 | total=2 upd=1 evicted=2 dropped=0   addData(122)
batch 4 | in=3 | wm=112 | total=3 upd=2 evicted=0 dropped=0   addData(121, 123, 126)
batch 5 | in=4 | wm=116 | total=3 upd=2 evicted=1 dropped=0   addData(127..130)
```

**The `wm` column is the ordering.** Each value is `(max seen through the previous
batch) − 10`. Batch 0 saw max 107 but ran at `wm=0`; batch 1 ran at `wm=97`. **A
batch never evicts against the watermark it computes** — it inherits the previous
one.

**Batch 2 is the not-a-filter proof.** Inherited `wm=104`:

| record | window | `window.end <= 104`? | fate |
|---|---|---|---|
| `103` | `[100,105)` | `105 <= 104` → false | **kept** — below the watermark, still aggregated |
| `90`, `92` | `[90,95)` | `95 <= 104` → true | dropped |
| `121` | `[120,125)` | `125 <= 104` → false | kept |

`upd=2` (the two surviving rows). `dropped=1` — **two records, one row**, because
`90` and `92` pre-aggregated into a single `[90,95)` row before the filter saw
them. The test is on the *window's end*, never on the record's timestamp.

**Batch 3 is the pipeline in one line.** `upd=1` **and** `evicted=2` in the same
batch: `122` was filtered, written to state, and *then* the scan removed
`[100,105)` and `[105,110)` — while the just-written `[120,125)` was untouched.
Filter → write → scan → evict, with the immunity invariant visible.

`[100,105)` emits with **count 3** — including the `103` that arrived below the
watermark in batch 2.

---

## No-data batches

By default Spark inserts batches with **zero input rows**. The decision: [7]

```
shouldConstructNextBatch = isNewDataAvailable ||
                           (lastExecutionRequiresAnotherBatch && noDataBatchesEnabled)
```

`lastExecutionRequiresAnotherBatch` comes from the stateful operators
(`StateStoreWriter.shouldRunAnotherBatch`, default `false`). The gate is
`spark.sql.streaming.noDataMicroBatches.enabled` (default `true`), documented as
enabling *"batches with no data to process for **eager state management**."* [7]

Run the demo with the default and every data batch is followed by an empty one
that applies the newly-advanced watermark:

```
batch 4 | in=4 | wm=104 | evicted=0 dropped=1     <- data
batch 5 | in=0 | wm=111 | evicted=2               <- no-data batch does the eviction
```

**It is a latency optimization, not a correctness mechanism.** The final sink
contents are *identical* with the flag on or off. Without no-data batches, a
finalized window simply waits for the next arriving record to trigger the scan —
which, on a stream that goes quiet, means append output stalls indefinitely. That
is why the default is on.

**The cost:** roughly 2× the batches, and most of them evict nothing — Spark
re-checks on *every* watermark advance, not only when there is something to remove.
A Tier 4 tuning lever.

---

## Output modes, from the same operator

The three modes are three branches of `StateStoreSaveExec`: [3]

| mode | input filter | eviction | emits |
|---|---|---|---|
| `complete` | **none** | **none** | the whole state store |
| `update` | optional (`case None => iter`) | in `close()`, rows **discarded** | the updated rows |
| `append` | required | in the main iterator | **the evicted rows** |

This settles three rules we previously took on faith:

- **`complete` cannot bound state** — the operator physically never removes
  anything and never filters input.
- **`update` works without a watermark** — the filter is `Option`al. Output stays
  correct; state grows unbounded.
- **`append` requires a watermark** — without one there is nothing it can legally
  emit, since its output *is* the eviction stream.

---

## Version notes (Spark 4.x)

- `MemoryStream` moved: `org.apache.spark.sql.execution.streaming.MemoryStream`
  (≤3.5) → `...execution.streaming.runtime.MemoryStream` (4.1). Bites anyone
  following a 3.5-era tutorial.
- Watermark semantics themselves are unchanged 2.1 → 4.x. The 4.0 advance in this
  area is `transformWithState` with per-key TTL (Concept 9) — a *different*
  eviction mechanism.

---

## Prove you got it

1. A 5s tumbling window `[20, 25)` sits in state. The inherited watermark for the
   current batch is exactly `25`. Is the window evicted? Cite the predicate.
2. A batch arrives with inherited `wm = 60`. It contains one record with event time
   `58` whose window is `[55, 60)`, and one with event time `52` whose window is
   `[50, 55)`. What happens to each, and what does `numRowsDroppedByWatermark`
   report?
3. Why does the input filter exist at all, given that eviction already removes dead
   windows from state?

<details>
<summary>Answers</summary>

1. **Yes.** The predicate is `LessThanOrEqual(window.end, watermark)` → `25 <= 25`
   is true. Non-strict. The window is half-open, so at `wm == end` every event that
   could belong to it has already arrived.
2. `[55,60)`: `60 <= 60` → **true** → the row is **dropped**. `[50,55)`:
   `55 <= 60` → true → also **dropped**. Both windows are dead.
   `numRowsDroppedByWatermark` reports **2** — two distinct pre-aggregated rows
   (different windows). Had both records fallen in the *same* window it would
   report 1.
3. Without it, a late record for an already-evicted window would create a **fresh
   state entry**, resurrecting the window — and in append mode it would be emitted
   a second time, breaking the emit-once-and-final contract. The filter prevents
   resurrection; eviction removes what just died. Same predicate, two binding
   points.

</details>

---

## Sources

1. Apache Spark Structured Streaming Programming Guide — "Handling Late Data and
   Watermarking"; `numRowsDroppedByWatermark` caveat. *(Note: its `> T` boundary
   phrasing is imprecise — see above.)*
2. `DataFrame.withWatermark` API docs — `delayThreshold` definition.
3. `statefulOperators.scala` (Spark master) — `WatermarkSupport.watermarkExpression`,
   `watermarkPredicateForData` / `watermarkPredicateForKeys`,
   `StateStoreSaveExec.doExecute` (Append/Update/Complete branches),
   `WatermarkTracker.updateWatermark`.
4. Demo: `demos/tier2/WatermarkMemoryStreamDemo.scala` — traces reproduced above.
5. `StreamingQueryProgress` ScalaDoc — `eventTime` keys `max`/`min`/`avg`/`watermark`;
   `StateOperatorProgress` fields.
6. Databricks — "Feature Deep Dive: Watermarking in Apache Spark Structured Streaming".
7. `MicroBatchExecution.constructNextBatch` — `shouldConstructNextBatch`;
   `StateStoreWriter.shouldRunAnotherBatch`;
   `spark.sql.streaming.noDataMicroBatches.enabled`.

---

[← Concept 2: Event-Time Windows](./02-event-time-windows.md) · [Next: Watermarks Part 2 →](./04-watermarks-part2.md)