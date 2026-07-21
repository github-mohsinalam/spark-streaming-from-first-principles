# Watermarks (Part 2)

> **Tier 2 · Concept 5 of 9**
> Part 1 was one operator, one stream. Part 2 is the single new idea: what is
> *the* watermark when the query has more than one source of it? Two faces —
> multiple input streams, and chained operators.

---

## The framing

Part 1 traced a single `groupBy` over a single input. The watermark was one value
with one source. Two situations break that assumption:

- **Horizontally** — multiple input *streams* (a join or a union) feed one
  operator, each with its own event-time and its own candidate watermark.
- **Vertically** — multiple stateful *operators* chained in a pipeline, where the
  watermark must flow through the chain.

Both ask the same question: when there is more than one watermark, what governs
eviction?

---

## Part 2a — multiple input streams: the global watermark

A query that joins or unions streams A and B, each with `withWatermark`, has two
per-stream watermarks. Eviction needs one. Which?

**Derive it from the promise.** State for stream A must be retained as long as
*either* stream could still produce a record that needs it. If B lags — its
watermark well behind A's — and you evicted using A's higher watermark, you would
drop A-state that B's still-valid, not-yet-arrived records need to match against.
Silently wrong joins.

So the global watermark must move at the pace of the **slowest** stream — the
**minimum** of the per-stream watermarks. The docs confirm: Structured Streaming
tracks the max event time per input stream, computes a per-stream watermark, and by
default takes the **minimum** as the global watermark, so no data is dropped as too
late if one stream falls behind. [1]

**The knob:** `spark.sql.streaming.multipleWatermarkPolicy`, default `min`,
settable to `max` since Spark 2.4. [1]

- `min` (default) — advances at the pace of the slowest stream. Never drops valid
  data; **stalls** if a stream dies (its watermark stops advancing, so the global
  one does too).
- `max` — advances at the pace of the fastest stream. Never stalls; **drops**
  data from slower streams. `WatermarkTracker`'s own comment calls it "the most
  aggressive" for late-data dropping. [2]

Same latency-vs-correctness dial as the threshold itself, now *across* streams
instead of within one.

---

## Part 2b — chained stateful operators (shape only; internals backlogged)

Stack two stateful operators — a windowed aggregation, then a second stateful
operation on its result. What watermark does the second operator use?

**The bug (Spark ≤ 3.4).** Spark used *one* global watermark for all operators.
Operator 1 (append-mode aggregation) emits a window exactly when the watermark
passes its end (Part 1), so the emitted row's event-time is essentially *at* the
global watermark. Operator 2 sees that same watermark, and its input-data predicate
(`window.end <= watermark`, Part 1) matches — so it **drops the row at input**,
before it ever enters state. Operator 1 finalizing correctly produces rows
operator 2 discards. Data lost, silently.

**The guardrail.** Spark 3.0 printed a warning (SPARK-28074); since 3.1,
`spark.sql.streaming.statefulOperator.checkCorrectness.enabled` defaults to `true`
and Spark throws an `AnalysisException` for such chains rather than return wrong
results. [3]

**The fix (Spark 3.5, SPARK-42376): watermark propagation.** Each operator gets its
*own* input and output watermark; the **output watermark of one operator becomes
the input watermark of the next**. Because operator 1's output watermark accounts
for the delay it introduces, operator 2's input watermark sits correctly *behind*
it and no longer treats freshly-emitted rows as late. This is what makes
multi-stateful-operator pipelines correct on Spark 4.x. [4]

**One constraint worth remembering now:** chaining stateful operators is supported
in **append mode only** — not update or complete. Follows from Part 1: a downstream
stateful operator can only consume *finalized* rows; a window that might still
change cannot be safely fed onward. [5]

> **Backlogged to Tier 3/4.** The propagation *internals* — the bottom-up
> `PropagateWatermarkSimulator`, `produceOutputWatermark`, and the exact arithmetic
> of each operator's output watermark (does it lag input by the window length?) —
> are deferred until the first multi-stage pipeline (e.g. window → dedup, or a
> chained join) actually needs them. The shape above is enough to recognize the
> `AnalysisException` and know why it exists.

---

## The one idea, both faces

Horizontally, reconcile multiple input-stream watermarks by `min` (safe) or `max`
(fast). Vertically, *propagate* — each operator's output watermark feeds the next.
Part 1 was the single-operator, single-stream mechanism; Part 2 is what happens
when either dimension has more than one.

---

## Prove you got it

1. A stream-stream join has stream A at watermark `12:10` and stream B at `12:03`.
   The engine uses `12:03`. Why can't it use `12:10` — what goes wrong, in terms of
   retained state?
2. Before watermark propagation, why did feeding an append-mode windowed
   aggregation into a second stateful operator cause the second to drop valid rows?
3. Chained stateful operators are legal only in append mode. Tie to Part 1: why
   can't an *update*-mode aggregation be safely fed downstream?

<details>
<summary>Answers</summary>

1. `12:10` would drop legitimate stream-B records that could still match A-state on
   the join key — B is lagging, so records with event-times between `12:03` and
   `12:10` are still valid but would be treated as late. `min = 12:03` retains and
   evicts state at the pace of the slowest stream, preserving join correctness at
   the cost of latency.
2. Operator 1 emits a window when the watermark is at/past its end, so the emitted
   row's event-time is essentially *at* the global watermark. Operator 2, sharing
   that same watermark, applies its input predicate `window.end <= watermark`, which
   matches — the row is **dropped at the input filter** (not evicted from state),
   before entering operator 2. Propagation fixes this by giving operator 2 a lower
   input watermark.
3. Update mode emits rows that may still change (it re-emits a group on every
   update). A downstream stateful operator needs *finalized* inputs to reason about
   lateness; a still-changing row cannot be safely consumed. Append emits each
   window once, finalized — which is exactly the contract chaining requires.

</details>

---

## Sources

1. Databricks — "Apply watermarks to control data processing thresholds"; Spark
   Structured Streaming Programming Guide (multiple-stream global watermark,
   `multipleWatermarkPolicy`, default `min`, `max` since 2.4).
2. `WatermarkTracker.scala` (Spark master) — min/max policy comments.
3. Spark Structured Streaming Migration Guide —
   `spark.sql.streaming.statefulOperator.checkCorrectness.enabled` (warn in 3.0
   per SPARK-28074; `AnalysisException` by default since 3.1).
4. SPARK-42376 "Introduce watermark propagation among operators" (Spark 3.5);
   waitingforcode, "What's new in Apache Spark 3.5.0 — watermark propagation."
5. Spark Structured Streaming Programming Guide — "Chaining multiple stateful
   operations on streaming Datasets is not supported with Update and Complete mode."

---

[← Concept 3: Watermarks Part 1](./03-watermarks-part1.md) · [Next: Streaming Joins →](./05-streaming-joins.md)