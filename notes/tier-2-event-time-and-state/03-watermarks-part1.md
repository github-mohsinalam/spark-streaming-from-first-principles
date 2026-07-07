# Watermarks (Part 1)

> **Tier 2 · Concept 3 of 9**
> The most misunderstood concept in Structured Streaming. Built here from first
> principles: what a watermark *is*, what it *is not* (an input filter), and a
> derivation — not a memorized rule — of when state is evicted.

---

## What a watermark is — the primary purpose

From the Apache Spark Structured Streaming Programming Guide: watermarking
"lets the engine automatically track the current event time in the data and
attempt to clean up old state accordingly." [1]

Read that carefully. The primary job is **state cleanup**. Nothing in the
definition mentions filtering input records. The other things watermarks are
associated with — emitting finalized rows in `append` mode, dropping late
records — are *implications* of state eviction, not independent features. Hold
this framing; most confusion about watermarks comes from treating "drop late
data" as the primary mechanism when it is actually a downstream consequence.

Why is state cleanup needed at all? From Concept 2: a windowed aggregation keeps
every window open so late data can still update it. The guide confirms this is
automatic — "Structured Streaming can maintain the intermediate state for partial
aggregates for a long period of time such that late data can update aggregates of
old windows correctly." [1] The cost is unbounded state. The guide states the
resulting need directly: "the system needs to know when an old aggregate can be
dropped from the in-memory state because the application is not going to receive
late data for that aggregate any more." [1] The watermark is the rule that answers
*when*.

---

## The declaration and the promise

```scala
df.withWatermark("eventTime", "10 minutes")
```

Two inputs: the event-time column, and a **delay threshold**. The PySpark API doc
defines the threshold as "the minimum delay to wait for data to arrive late,
relative to the latest record that has been processed." [2]

Conceptually, this is **a promise you make to the engine about your data**:

> No record will arrive whose event time is behind the maximum event time already
> processed by more than the threshold.

The engine cannot verify this promise — it is a claim about the external world
(your source's actual lateness behavior). Everything the watermark does is correct
*conditional on this promise holding*. If real data violates it, records are
silently dropped (derived below).

---

## The watermark value and how it advances

The engine computes, per micro-batch:

```
watermark = (max event time seen so far) − threshold
```

Only the **maximum** event time feeds this calculation. This is verifiable from
the engine source: `WatermarkTracker.updateWatermark` computes
`newWatermarkMs = eventTimeStats.max - delayMs`. [3]

**Monotonic advance (verified from source).** The same code updates the watermark
only when it would move forward:

```scala
val newWatermarkMs = e.eventTimeStats.value.max - e.delayMs
val prevWatermarkMs = operatorToWatermarkMap.get(index)
if (prevWatermarkMs.isEmpty || newWatermarkMs > prevWatermarkMs.get) {
  operatorToWatermarkMap.put(index, newWatermarkMs)
}
```

The watermark never regresses; if a batch brings a lower max, the watermark holds
its previous value. [3]

**Batch-boundary timing.** The watermark is recomputed at the end of a trigger and
used by the *next* trigger — the guide's illustration shows the watermark being
set for the *following* trigger based on the max event time observed in the current
one. [1] So the watermark in effect *during* batch N is the value computed at the
end of batch N−1. It does not change mid-batch.

**Initial value.** Before any data has been processed (batch 0), there is no max
event time, so the watermark sits at its minimum possible value — effectively the
epoch (1970-01-01). Nothing is "late" in the first batch; every record is above
the initial watermark. *(Conceptual consequence of `max − threshold` with no data;
consistent with observed `1970-01-01` watermark timestamps in engine test
traces. [4])*

**Monitoring.** The `StreamingQueryProgress.eventTime` map exposes four keys for
observability: `max`, `min`, `avg` (event-time statistics for the trigger) and
`watermark` (the watermark used in the trigger). [5] Only `max` drives the
mechanism; `min` and `avg` are reported for monitoring and event-skew diagnosis,
not consumed by eviction.

---

## The per-trigger ordering — four steps, precisely sequenced

The single biggest source of error  is
collapsing the trigger's internal steps into "the watermark filters late records."
Worse, it is easy to get the *ordering* of watermark-advance and eviction wrong —
to assume a trigger evicts against the watermark it just computed from its own
data. It does not.

The Apache Spark programming guide states the ordering directly, for the micro-batch
engine: watermarks are advanced at the **end** of a micro-batch, and the **next**
micro-batch uses the updated watermark to clean up state and output results. [1]
So the watermark a trigger *computes* does not evict anything in that same trigger —
it governs the *next* one.

A single trigger N performs these steps in order:

**Step 1 — inherit the watermark.** Trigger N begins with the watermark computed at
the end of trigger N−1. Call it `W_in`. This value is fixed for the whole trigger.
(For trigger 0, `W_in` is the epoch — no data has been seen.)

**Step 2 — read the batch and update state; the watermark is NOT an input filter.**
Every record in the batch is routed to its window(s) and updates the corresponding
state **provided that window still exists in the state store**. A record whose event
time is below `W_in`, but whose window is *still open*, **is still aggregated**. The
guide confirms the behavior: an out-of-order, late record whose window is still
active falls into that window and the engine correctly updates the maintained
counts. [1]

**Step 3 — evict and finalize against `W_in`.** Windows whose `end` is behind
`W_in` — the *inherited* watermark, not one computed from this batch's data — are
finalized (emitted, in `append` mode) and evicted from the state store. This
eviction is what causes *future* late records for those windows to be dropped.

**Step 4 — compute the outgoing watermark.** After processing, the engine computes
`W_out = (max event time seen so far) − threshold` and stores it. Trigger N+1 will
inherit `W_out` as *its* `W_in`. The max event time observed in trigger N thus
affects only *future* evictions — never trigger N's own.

The correct causal chain:

```
watermark advances  →  old window's state is evicted  →
later records for that now-gone window have nothing to update  →  they are dropped
```

Dropping is **downstream of eviction**, not a filter applied on record arrival.
The programming guide describes exactly this sequence: once the watermark advances
past a window's end, that window's intermediate state is cleared, and any
subsequent data belonging to it is then considered too late and ignored. [1] A late
record is dropped because its bucket is gone — not because its timestamp is a small
number.

"Late" does not mean "dropped." "Window already evicted" means "dropped." The two
simulations below make this concrete, ending with the case where a below-watermark
record is *still aggregated* because its window remains open.

---

## Simulation 1 — the simple, in-order case

Before the subtle case, walk the straightforward one: data arrives roughly in
order, and windows evict cleanly. This establishes the four-step ordering with
nothing tricky happening. The critical discipline: **each batch evicts against the
watermark it *inherited* (`W_in`), then computes `W_out` for the next batch.**

**Configuration.** Threshold = `10 min`. Tumbling window = `5 min`. Output mode =
`append`. Aggregation = `count` per window.

---

**Batch 0 — query starts, no data.** `W_in` = epoch (`1970-01-01`); nothing can be
late, nothing to evict. `W_out = epoch` (no max event time yet). Empty output.

```
State store: (empty)
W_out (→ inherited by batch 1): epoch
```

**Batch 1 arrives:** `[P, 10:02]`, `[Q, 10:04]`
- *Inherit:* `W_in` = epoch.
- *Update state:* both accepted (above epoch). Both fall in `[10:00, 10:05)`.
- *Evict against `W_in` = epoch:* nothing to evict.
- *Compute `W_out`:* max event time = `10:04`; `W_out = 10:04 − 10min = 09:54`.

```
State store:
  [10:00, 10:05) → count 2   (P, Q)   OPEN
W_out (→ inherited by batch 2): 09:54
```

**Batch 2 arrives:** `[R, 10:07]`, `[S, 10:09]`
- *Inherit:* `W_in` = `09:54`.
- *Update state:* both accepted. Both fall in `[10:05, 10:10)`.
- *Evict against `W_in` = 09:54:* windows with `end` behind `09:54` → none
  (`[10:00,10:05)` ends at `10:05 > 09:54`). Nothing evicted.
- *Compute `W_out`:* max = `10:09`; `W_out = 10:09 − 10min = 09:59`.

```
State store:
  [10:00, 10:05) → count 2   (P, Q)   OPEN
  [10:05, 10:10) → count 2   (R, S)   OPEN
W_out (→ inherited by batch 3): 09:59
```

**Batch 3 arrives:** `[T, 10:16]`
- *Inherit:* `W_in` = `09:59`.
- *Update state:* accepted. Falls in `[10:15, 10:20)`.
- *Evict against `W_in` = 09:59:* windows with `end` behind `09:59` → none
  (`[10:00,10:05)` ends at `10:05 > 09:59`). **Nothing evicted yet** — even though
  this batch's data pushes the max to `10:16`, that only affects `W_out`.
- *Compute `W_out`:* max = `10:16`; `W_out = 10:16 − 10min = 10:06`.

```
State store:
  [10:00, 10:05) → count 2   (P, Q)   OPEN
  [10:05, 10:10) → count 2   (R, S)   OPEN
  [10:15, 10:20) → count 1   (T)      OPEN
W_out (→ inherited by batch 4): 10:06
```

**Batch 4 arrives:** `[U, 10:17]`
- *Inherit:* `W_in` = **`10:06`** (computed at end of batch 3).
- *Update state:* accepted. Falls in `[10:15, 10:20)` → count 2.
- *Evict against `W_in` = 10:06:* windows with `end` behind `10:06` (`W_in > end`):
  `[10:00, 10:05)` (`10:06 > 10:05`). **Evicted and emitted as final.** `[10:05,
  10:10)` ends at `10:10 > 10:06` → still open.
- *Compute `W_out`:* max = `10:17`; `W_out = 10:17 − 10min = 10:07`.

```
State store BEFORE eviction:
  [10:00, 10:05) → count 2   → evict + emit final
  [10:05, 10:10) → count 2   OPEN
  [10:15, 10:20) → count 2   OPEN

State store AFTER eviction:
  [10:05, 10:10) → count 2   OPEN
  [10:15, 10:20) → count 2   OPEN

Appended to sink (final): [10:00,10:05)=2
W_out (→ inherited by batch 5): 10:07
```

**The ordering point.** `[10:00, 10:05)` was complete in data terms after batch 1.
The watermark first passed its end (`10:05`) when `W_out = 10:06` was computed at
the **end of batch 3** — but that watermark did not evict anything in batch 3. It
was **inherited by batch 4** and drove the eviction there. So the window emitted in
**batch 4**, one trigger after the watermark value crossed its end. This one-trigger
lag is the observable fingerprint of the "advance at end, next batch uses it"
ordering [1] — and it stacks on top of the append-mode delay (the window already
could not emit until the watermark passed its end at all).

---

## Simulation 2 — the below-watermark-but-still-open case

Simulation 1 had every record above the inherited watermark. The subtler, more
commonly misunderstood case is a record whose event time is **below `W_in`** that is
*still* aggregated — because its window has not yet been evicted. This section builds
that case from scratch, keeping the same four-step discipline: **evict against the
inherited `W_in`, then compute `W_out`.**

**Configuration.** Threshold = `10 min`. Tumbling window = `5 min`. Output mode =
`append`. Aggregation = `count` per window.

---

**Batch 0 — query starts, no data.** `W_in` = epoch (`1970-01-01`); nothing to
evict. Empty output.

```
State store: (empty)
W_out (→ inherited by batch 1): epoch
```

**Batch 1 arrives:** `[A, 09:03]`, `[B, 09:07]`
- *Inherit:* `W_in` = epoch. Everything is above epoch.
- *Update state:* both accepted. `09:03 → [09:00, 09:05)`; `09:07 → [09:05, 09:10)`.
- *Evict against `W_in` = epoch:* nothing.
- *Compute `W_out`:* max = `09:07`; `W_out = 09:07 − 10min = 08:57`.

```
State store:
  [09:00, 09:05) → count 1   (A)     OPEN
  [09:05, 09:10) → count 1   (B)     OPEN
W_out (→ inherited by batch 2): 08:57
```

**Batch 2 arrives:** `[C, 09:14]`, `[D, 09:02]`
- *Inherit:* `W_in` = `08:57`.
- *Update state:* `D` (`09:02`) → window `[09:00, 09:05)` is open, so `D` is
  accepted and increments it. `C` opens `[09:10, 09:15)`.
- *Evict against `W_in` = 08:57:* nothing (`[09:00,09:05)` ends at `09:05 > 08:57`).
- *Compute `W_out`:* max = `max(09:07, 09:14, 09:02) = 09:14`;
  `W_out = 09:14 − 10min = 09:04`.

```
State store:
  [09:00, 09:05) → count 2   (A, D)  OPEN   ← D folded in correctly
  [09:05, 09:10) → count 1   (B)     OPEN
  [09:10, 09:15) → count 1   (C)     OPEN
W_out (→ inherited by batch 3): 09:04
```

**Batch 3 arrives:** `[E, 09:21]`, `[F, 09:03]`, `[G, 08:50]` — the key batch.
- *Inherit:* `W_in` = **`09:04`**.
- *Update state, record by record:*
    - `F, 09:03`: **`09:03` is below `W_in = 09:04`.** A naive "filter records below
      the watermark" model would drop `F`. That model is wrong. `F`'s window
      `[09:00, 09:05)` is **still in the state store**, so `F` is **accepted** and
      increments that window to count 3. *This is the observation's case: an event
      earlier than the watermark value, still aggregated, because its window is open.*
    - `G, 08:50`: window `[08:50, 08:55)` was never opened and is far below `W_in`.
      No bucket exists. **Dropped** — nothing to update.
    - `E, 09:21`: opens `[09:20, 09:25)`, count 1.
- *Evict against `W_in` = 09:04:* windows with `end` behind `09:04` → none
  (`[09:00,09:05)` ends at `09:05 > 09:04`). **Nothing evicted in this batch** —
  even though this batch's max is `09:21`, that only sets `W_out`.
- *Compute `W_out`:* max = `09:21`; `W_out = 09:21 − 10min = 09:11`.

```
State store (no eviction this batch):
  [09:00, 09:05) → count 3   (A, D, F)   OPEN   ← F accepted below W_in
  [09:05, 09:10) → count 1   (B)         OPEN
  [09:10, 09:15) → count 1   (C)         OPEN
  [09:20, 09:25) → count 1   (E)         OPEN
W_out (→ inherited by batch 4): 09:11
```

**Batch 4 arrives:** `[H, 09:22]`
- *Inherit:* `W_in` = **`09:11`** (computed at end of batch 3).
- *Update state:* accepted → `[09:20, 09:25)` → count 2.
- *Evict against `W_in` = 09:11:* windows with `end` behind `09:11` (`W_in > end`):
  `[09:00, 09:05)` (`09:11 > 09:05`) and `[09:05, 09:10)` (`09:11 > 09:10`).
  **Both evicted and emitted as final.** `[09:10, 09:15)` ends at `09:15 > 09:11` →
  still open.
- *Compute `W_out`:* max = `09:22`; `W_out = 09:22 − 10min = 09:12`.

```
State store BEFORE eviction:
  [09:00, 09:05) → count 3   (A, D, F)   → evict + emit final
  [09:05, 09:10) → count 1   (B)         → evict + emit final
  [09:10, 09:15) → count 1   (C)         OPEN
  [09:20, 09:25) → count 2   (E, H)      OPEN

State store AFTER eviction:
  [09:10, 09:15) → count 1   (C)         OPEN
  [09:20, 09:25) → count 2   (E, H)      OPEN

Appended to sink (final): [09:00,09:05)=3, [09:05,09:10)=1
W_out (→ inherited by batch 5): 09:12
```

---

**The point of the simulation.** Window `[09:00, 09:05)` was emitted with count
**3** — it correctly included `D` (accepted in batch 2) and `F` (accepted in batch
3 **while `F`'s event time `09:03` was below the inherited watermark `09:04`**). `F`
was not dropped, because dropping is a consequence of a window being *evicted*, not
of the record's timestamp being below the watermark.

Note the ordering precisely: the watermark first passed `[09:00, 09:05)`'s end when
`W_out = 09:11` was computed at the **end of batch 3** — but eviction did not happen
in batch 3. That watermark was **inherited by batch 4**, which evicted and emitted
the window. Crucially, `F` arrived in batch 3, *before* the eviction in batch 4, so
it landed while the window was still open. This is exactly why the ordering matters:
if eviction had (wrongly) happened in batch 3 against that batch's own freshly
computed `09:11`, the window would have closed at count 3 in the same batch — the
count would coincidentally match here, but the *emission trigger* would be wrong,
and any record arriving between the two triggers would be mishandled.

Contrast the two records that were below the watermark in batch 3:
- `F, 09:03` — **accepted**, because window `[09:00, 09:05)` was still open.
- `G, 08:50` — **dropped**, because window `[08:50, 08:55)` had never been opened.

Same "below the watermark" property, opposite outcomes — decided entirely by
whether the target window still exists in the state store. And *after* batch 4's
eviction, a `[09:01]` record arriving in batch 5 *would* finally be dropped —
because only now is its bucket gone. That is the true "too late to process"
condition: not "timestamp below the watermark," but "window already evicted."

---

## The eviction rule — derived, not asserted

**Claim.** A window `W = [start, end)` can be safely evicted once `watermark > end`.

**What "safely" means.** Evicting `W` must not discard a record the engine
*promised* to include. So the proof rests entirely on the watermark promise.

**Proof.**

A record with event time `e` updates window `W = [start, end)` only if
`start ≤ e < end`.

Suppose `end ≤ watermark`. Then for any such record:

```
e < end ≤ watermark = maxEventTime − threshold
⟹  e < maxEventTime − threshold
⟹  maxEventTime − e > threshold
```

But the watermark promise states that no record exists whose event time is more
than `threshold` behind `maxEventTime`. Therefore, once the watermark has reached
`end`, no *promised* record can still update `W`. Its state is discarded with no
loss of a promised record.

**Which watermark, and which trigger.** The proof establishes that a watermark
value satisfying `watermark > end` is a *safe threshold* for evicting `W` — it says
nothing about *when* the engine applies it. Those are separate facts. The value
question is answered by the promise (above); the timing question is answered by the
engine's ordering: eviction in trigger N runs against the *inherited* watermark
`W_in` (computed at the end of N−1), not against a watermark recomputed from
trigger N's own data. [1] So the operative statement is: `W` is evicted in the first
trigger whose *inherited* `W_in` satisfies `W_in > end`. The proof guarantees this
loses no promised record; the ordering guarantees it happens one trigger after the
watermark value first crosses `end`.

**The boundary is strict (`>`).** The guide states the retention condition as: the
engine "will maintain state and allow late data to update the state until
`(max event time seen by the engine − late threshold > T)`", where `T` is the
window boundary. [1] State is retained *while* that inequality is false; eviction
occurs when it becomes true. So the operative condition is `watermark > end`, and
the tie `watermark == end` resolves to **still retained**.


---

## The one-way guarantee — and why the asymmetry is load-bearing

The threshold gives an **asymmetric** guarantee:

- Records arriving **within** the threshold of the max event time are
  **guaranteed** to be processed.
- Records arriving **beyond** the threshold are **not guaranteed** to be dropped —
  they *may* still be processed.

This is the guide's "exact guarantees" caveat, and the second half is what makes
the model internally consistent. The derivation above used only the **near side**
("within threshold ⇒ accepted", contrapositively "belongs in an evicted window ⇒
was beyond threshold ⇒ not promised"). The **far side** is deliberately weak: a
beyond-threshold record is dropped *only if* its window has already been evicted.
If the window is still open (watermark hasn't passed its end yet), a "too-late"
record is still accepted — exactly the `F, 09:03` case in Simulation 2.

If the guarantee were two-way (drop *everything* beyond threshold at input), the
engine would have to filter `F, 09:03` even with its window `[09:00, 09:05)` still
open — contradicting observed behavior. The asymmetry is not a wart; it is what
makes the eviction rule and the not-a-filter behavior simultaneously true.

**The premise is your responsibility.** The whole derivation is conditional on the
promise being accurate. If real data arrives more than `threshold` late, its window
may already be evicted and the record is silently dropped — no error, no warning.
Whether the promise matches reality is a domain measurement (your source's actual
lateness distribution), not something the engine verifies.

---

## Output-mode interaction

Watermarks are usable with `append` and `update`, not `complete`. [6]

**Update mode.** The watermark is *optional* for producing output. The engine emits
a window's current value on every update; the watermark's job here is purely to
trim state. Databricks: "for 'update' mode, the aggregate can be produced
repeatedly starting from the first record and on each received record, thus a
watermark is optional. The watermark is only useful for trimming the state." [6]
Without a watermark, `update` output is still correct but state grows without
bound (Concept 1).

**Append mode.** The watermark is *required* for aggregations. A window is emitted
exactly once, only after the watermark passes its end. Databricks: "in append mode
the aggregate is produced only at the closing of the time window plus the watermark
delay." [6] This is the latency cost of `append`: results are delayed by up to
`threshold` beyond the window's end (the watermark must advance `threshold` past the
window end), plus one additional trigger from the "advance at end, next batch
evicts" ordering [1] — the window emits in the trigger *after* the computed watermark
first crosses its end.

**Complete mode.** Cannot use watermarks to trim state — the mode requires the
entire result table every trigger, so no eviction is possible. [6]

**Late-data metric.** `StateOperatorProgress.numRowsDroppedByWatermark` reports how
many rows were considered too late to be included. Note it measures rows dropped
*post-aggregation*, not raw input rows, so it is an indicator rather than a precise
count. [6] This is the observable signal that your threshold may be too small.

---

## Multi-partition / multi-stream subtlety

The watermark tracks the max event time *globally*. With multiple input
partitions or streams, Spark tracks the max per stream and, by default, chooses
the **minimum** across them as the global watermark — so it advances at the pace of
the *slowest* stream, preventing premature eviction when one stream lags. [7]
Tunable via `spark.sql.streaming.multipleWatermarkPolicy` (default `min`, settable
to `max` since Spark 2.4). [7] Event skew across partitions is exactly why the
`min`/`avg` monitoring fields exist.

---

## Spark 3.x → 4.x note

The watermark mechanism — `withWatermark`, `max − threshold`, monotonic advance,
batch-boundary timing, eviction-drives-dropping, output-mode rules — is materially
unchanged from Spark 2.1 through 4.x. The programming-guide text is identical
across 3.5 and 4.x. The documentation *location* changed in 4.0: the single guide
page was split into smaller pages (the watermarking content now lives under
`streaming/apis-on-dataframes-and-datasets.html`), but the semantics did not
change. [1] The Spark 4.0 advance in this area is `transformWithState` with per-key
TTL eviction (Concept 9) — a *different* eviction mechanism from the global
watermark.

---

## Prove you got it

1. **The two-fates question.** A query has a 10-minute watermark and 5-minute
   tumbling windows. The watermark in effect at the start of the current batch is
   `09:04`. Two records arrive in this batch: `[X, eventTime=09:03]` and
   `[Y, eventTime=08:58]`. The window `[09:00, 09:05)` is currently in the state
   store; the window `[08:55, 09:00)` was evicted two batches ago. For each record,
   state whether it is aggregated or dropped, and *why* — referencing the state
   store, not the watermark number.

2. **The derivation question.** Reconstruct the eviction proof in your own words:
   starting from "a record updates `W = [start, end)` only if `e < end`", show why
   `watermark > end` guarantees no *promised* record can still update `W`. Identify
   the exact step where the watermark promise is used.

3. **The ordering question.** A 5-minute tumbling window `[09:00, 09:05)` is open.
   At the end of trigger N, the engine computes a watermark of `09:06` (the first
   time the computed watermark exceeds the window end `09:05`). In `append` mode, is
   `[09:00, 09:05)` emitted at the end of trigger N, or in trigger N+1? Explain the
   rule that decides this.

<details>
<summary>Answers</summary>

1. `[X, 09:03]` is **aggregated**. Its window `[09:00, 09:05)` is still in the
   state store, so despite `09:03` being below the watermark `09:04`, there is an
   open bucket for it to update. The watermark is not an input filter. `[Y, 08:58]`
   is **dropped** — its window `[08:55, 09:00)` was already evicted, so there is no
   bucket to update. The drop is a consequence of prior eviction, not of comparing
   `08:58` to the watermark.

2. A record updates `W = [start, end)` only if its event time `e < end`. If
   `end ≤ watermark`, then `e < watermark = maxEventTime − threshold`, so
   `maxEventTime − e > threshold`. The watermark **promise** — no record arrives
   more than `threshold` behind `maxEventTime` — is used exactly here: it asserts
   no such record exists. Therefore once the watermark reaches `end`, no promised
   record can update `W`, and it is safe to evict. The promise is the single
   premise; everything else is arithmetic.

3. `[09:00, 09:05)` is emitted in **trigger N+1**, not at the end of trigger N. The
   rule: a trigger evicts and finalizes windows against the watermark it *inherited*
   (computed at the end of the previous trigger), never against the watermark it
   computes from its own data. The `09:06` watermark is *computed* at the end of
   trigger N, but it only *governs eviction* when it is inherited by trigger N+1. So
   even though `09:06 > 09:05` became true at the end of trigger N, the window is not
   emitted until trigger N+1 runs its eviction step against the inherited `09:06`.
   This one-trigger gap between "computed watermark crosses window end" and "window
   emitted" is the ordering's fingerprint.

</details>

---

## Sources

1. Apache Spark Structured Streaming Programming Guide — "Handling Late Data and
   Watermarking," and the micro-batch watermark-ordering statement ("watermarks are
   advanced at the end of a micro-batch, and the next micro-batch uses the updated
   watermark to clean up state and output results" — stated in the stream–stream
   join subsection). (Spark 3.5 guide; Spark 4.x:
   `streaming/apis-on-dataframes-and-datasets.html`.)
2. `pyspark.sql.DataFrame.withWatermark` — PySpark API documentation
   (`delayThreshold` parameter definition).
3. `WatermarkTracker.updateWatermark` engine source, as quoted in waitingforcode,
   "Event time skew and global watermark in Apache Spark Structured Streaming."
   (`newWatermarkMs = max − delayMs`; monotonic `newWatermarkMs > prevWatermarkMs`.)
4. waitingforcode, "Apache Spark Structured Streaming and watermarks" (engine test
   traces showing epoch-based initial watermark timestamps).
5. `StreamingQueryProgress` JavaDoc — `eventTime` map keys `max`/`min`/`avg`/
   `watermark`.
6. Databricks Blog, "Feature Deep Dive: Watermarking in Apache Spark Structured
   Streaming" (update vs append vs complete; `numRowsDroppedByWatermark`).
7. Apache Spark Structured Streaming Programming Guide — multiple-watermark policy
   (`spark.sql.streaming.multipleWatermarkPolicy`, default `min`).

---

[← Concept 2: Event-Time Windows](./02-event-time-windows.md) · [Next: Watermarks Part 2 →](./04-watermarks-part2.md)