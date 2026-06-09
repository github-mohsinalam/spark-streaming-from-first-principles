# Stateless Transformations

> **Tier 1 · Concept 7 of 8**
> The shortest concept in Tier 1, by design. The central fact about
> stateless transformations is precisely that *there's almost nothing
> new to learn* — they are batch transformations applied to a streaming
> DataFrame, and Spark goes to considerable trouble to make sure they
> behave identically. Understanding *why* that "no surprises" property
> holds, and what the small list of genuine exceptions is, is the
> whole concept.

---

## The one-sentence idea

A stateless transformation is one whose output for any record depends
only on that record. The absence of cross-row dependency is what
makes incremental execution trivial — and trivial incremental
execution is what guarantees batch-streaming equivalence.

---

## The defining property

A transformation is **stateless** if its output for any record depends
only on that record. No history, no aggregation across rows, no notion
of "what came before."

```
stateless: f(r) → r'                  (each row processed independently)
stateful:  f(r, state) → (r', state') (each row threads through accumulated state)
```

Concretely, on a Spark DataFrame:

| Operation                                  | Stateless?                        |
| ------------------------------------------ | --------------------------------- |
| `select(...)`                              | Yes                               |
| `filter(...)` / `where(...)`               | Yes                               |
| `withColumn("x", ...)`                     | Yes                               |
| `map(...)` (on a typed `Dataset[T]`)       | Yes                               |
| `flatMap(...)`                             | Yes                               |
| `drop(...)`                                | Yes                               |
| `dropDuplicates(...)` (without watermark)  | **No — stateful, unbounded state**|
| `groupBy(...).agg(...)`                    | **No — stateful**                 |
| `join(...)` with another stream            | **No — stateful**                 |
| `orderBy(...)` over the whole stream       | **Illegal**                       |

The pattern: anything that requires looking at multiple records
together is stateful. Anything that processes each record in
isolation is stateless.

---

## Why statelessness is preserved across the streaming boundary

Recall the streaming-flag-on-the-logical-plan story from Concept 1.
The same Catalyst plan that runs against a bounded `DataFrame` in
batch runs against an unbounded streaming `DataFrame` in streaming —
except the engine loops over micro-batches and threads state through
stateful operators.

For a stateless operator, **there is no state to thread**. The
operator's behaviour on a single record is identical whether that
record is from a 100-record batch table or the 17th micro-batch of
a year-long stream. The operator never "knows" which case it's in.
Catalyst plans the operation the same way; the executor runs it the
same way; the output is the same.

That's why this works without surprise:

```scala
// Batch:
val out = spark.read.json("/data")
  .select("user_id", "amount")
  .filter($"amount" > 100)
  .withColumn("amount_usd", $"amount" * 1.1)

// Streaming — identical transformation chain:
val out = spark.readStream.schema(s).json("/data")
  .select("user_id", "amount")
  .filter($"amount" > 100)
  .withColumn("amount_usd", $"amount" * 1.1)
```

The four lines of transformation are byte-for-byte identical. The
engine handles the streaming-ness around them, not within them.
**This is Spark's deliberate design promise: the API surface for
stateless work is the same in both worlds.**

### The deeper reason batch–streaming equivalence holds here

It's not just that stateless operators *happen* not to need state.
It's that **the absence of cross-row dependency is what makes
incremental execution trivial** — and trivial incremental execution
is what makes batch-streaming equivalence possible.

For an aggregation, you cannot promise "batch and streaming produce
the same output," because:

- Batch sees all data, computes one final value.
- Streaming sees data in pieces, must emit *intermediate* values
  along the way.

The output mode (`append` / `update` / `complete`) is precisely the
mechanism for choosing *which* intermediate values to emit. Streaming
aggregations don't have one canonical answer; they have three
legitimate ones.

Stateless operations have only one answer in both worlds — process
this row, emit the result. That's why no output-mode question even
arises for them in interesting ways: every stateless query is
trivially `append`-compatible.

---

## Two practical consequences

**1. Your batch Scala skills transfer directly.** Filters,
projections, column expressions, `when/otherwise`, string and date
functions, conditional logic, UDFs — everything you already know
about batch DataFrames works on streaming DataFrames with no
syntactic or semantic change.

**2. Spark's streaming-flag correctness gate doesn't fire on
stateless operations.** From Concept 1, the planner rejects
operations whose semantics it can't honour incrementally. Stateless
operations *trivially* have incremental semantics — each row
processed once, output once, done. So the planner accepts them
without complaint, regardless of output mode, trigger, source, or
sink.

---

## The small list of genuine differences

Two operations look stateless but aren't, and one operation is
fundamentally illegal on an unbounded stream.

### `dropDuplicates` without a watermark — stateful, unbounded

```scala
events.dropDuplicates("event_id")
```

This looks like a per-record filter, but it isn't. To detect a
duplicate, the engine must remember every `event_id` it has ever
seen. State grows without bound.

A critical operational distinction: this is *unbounded-state*
stateful, which differs sharply from *bounded-state* stateful (like
a watermarked aggregation). Unbounded state grows forever; bounded
state has a defined eviction point. The streaming planner accepts
`dropDuplicates` but will warn about state growth in
`StreamingQueryProgress` metrics.

The production-correct version is **`dropDuplicatesWithinWatermark`**,
which bounds state by retaining only IDs seen within the watermark
window. Tier 2 covers the mechanics. For now: know that
`dropDuplicates` is *not* what its name suggests — it's a stateful
operator with unbounded state.

### `orderBy` over the whole stream — illegal

```scala
events.orderBy("eventTime")           // rejected at plan time
```

Sorting an unbounded stream by some column has **no incremental
semantics at all**. The very concept of a globally sorted unbounded
stream has no well-defined output — there's no notion of "the sort
so far" that a future record can append to or interleave with. A
record arriving later might belong anywhere in the order.

The planner rejects this at `start()` not because it's hard, but
because it's *meaningless*. This is one of the cleanest cases where
the correctness gate from Concept 1 pays off: refusing meaningless
plans rather than producing wrong output.

You can sort *within* a windowed aggregation (Tier 2), but not over
the whole stream globally.

### Stateful operations are stateless inside `foreachBatch`

This is the loophole that Concept 4 introduced. Inside a
`foreachBatch` body, the per-batch DataFrame is *bounded*, and you
can run any batch operation on it — including `groupBy + sum +
orderBy`. The output of that bounded computation is then written by
you to the sink.

So `groupBy` on the streaming side is stateful (and triggers all the
state-management machinery), but `groupBy` *inside* `foreachBatch`
is just a batch operation on a bounded DataFrame. Same syntax,
completely different semantics. This is the architectural lever
that lets you escape streaming's legality rules when you need batch
expressivity on each micro-batch.

---

## Performance: stateless transformations are the cheap ones

Streaming queries pay a per-batch overhead for *being streaming*:
offset log read, plan execution, sink write, commit log write,
checkpoint state read/write (for stateful operators). Stateless
operations skip the state-store overhead entirely. They contribute
the same cost they would in batch — a Catalyst-optimised pass over
the data — and nothing more.

This is why a Kafka → bronze Delta append pipeline (Demo 01 from
Concept 5) runs cheaply: the entire transformation chain is
stateless. Parse JSON, project, add an ingestion timestamp, write.
No state store engaged.

Once you introduce streaming aggregations or joins (Tier 2), the
cost picture changes considerably.

---

## Connecting back

**To Concept 1 (the streaming flag).** The streaming flag enables
the planner's correctness gate. For stateless operators, the gate
doesn't trip — there's nothing to reject. The flag still does its
other job (mark the plan as incremental, attach the trigger loop),
but it doesn't constrain *what* you write at the per-row level.

**To Concept 2 (Datasets and the typed API).** The typed `Dataset[T]`
API's stateless operators (`map`, `filter`, `flatMap` taking
functions) are exactly as stateless as their untyped DataFrame
counterparts. The typed/untyped distinction is orthogonal to
stateful/stateless.

**To Concept 5 (output modes).** Stateless queries are legal in
`append` and `update` modes (the engine emits each transformed row
once). `complete` is illegal — re-emitting the whole
monotonically-growing result table forever isn't a useful primitive
for stateless work.

---

## Spark 3.x → 4.x note

No gap. Stateless transformations are stable across all Spark
versions you'll encounter. This is the most boring version-
compatibility note in Tier 1, and that's by design.

---

## Prove you got it

1. **Identify what's actually stateful.** For each of the following
   operations on a streaming DataFrame, say whether it's stateless,
   stateful, or illegal, and justify each in one sentence:
    - (a) `events.filter($"amount" > 100).select("user_id", "amount")`
    - (b) `events.withColumn("amount_in_cents", $"amount" * 100)`
    - (c) `events.dropDuplicates("event_id")`
    - (d) `events.groupBy("region").count()`
    - (e) `events.orderBy("eventTime")`
2. **The "no surprises" derivation.** Why is it possible for Spark
   to promise that stateless transformations behave identically in
   batch and streaming, while the same promise can't be made for
   aggregations? Answer in two or three sentences, framing it in
   terms of what state the operator does or doesn't need — and what
   that implies about output modes.
3. **The `foreachBatch` loophole.** You have a streaming Kafka
   source and want to produce a per-batch top-10 ranking of users
   by transaction count. The top-10 ranking requires `groupBy +
   count + orderBy + limit`, which doesn't fit the streaming
   legality rules (`orderBy` is illegal; `groupBy` would create
   unbounded state). Yet there's a clean way to do this in
   Structured Streaming. Describe the approach, explain why it
   sidesteps both restrictions, and call out what the "top 10"
   actually means in this design.

<details>
<summary>Answers</summary>

1. (a) **Stateless.** A row either satisfies the predicate or
   doesn't; the projection picks columns from a single row. Neither
   step depends on other rows.
   (b) **Stateless.** Each row is transformed independently;
   `amount_in_cents` is computed from that row's `amount` alone.
   (c) **Stateful, with unbounded state.** To recognise a duplicate
   `event_id`, the engine must remember every `event_id` it has
   ever seen. State grows forever; the planner accepts this but
   warns. The bounded variant is `dropDuplicatesWithinWatermark`.
   (d) **Stateful, with bounded-or-unbounded state depending on
   watermark.** The running count per region is the state; each
   new record updates it. Without a watermark, the state grows in
   principle (new regions could appear); with a watermark on a
   windowed grouping, state is bounded.
   (e) **Illegal.** A globally sorted unbounded stream has no
   well-defined output — a future record could belong anywhere
   in the order. The planner rejects this at `start()` because
   the semantics are meaningless, not because the operation is
   hard.
2. Stateless operators have no cross-row dependency, so the
   operator's behaviour on a single record is identical whether
   that record comes from a bounded batch table or the 17th
   micro-batch of a year-long stream. There's no "state so far"
   that batch and streaming would compute differently. Aggregations,
   by contrast, must produce intermediate output along the way in
   streaming (the engine cannot wait forever for "all data" before
   emitting), so they have *three* legitimate intermediate-output
   semantics — `append` / `update` / `complete` — none of which
   matches batch's "compute once at the end" semantic exactly. The
   output mode question itself only arises because aggregations
   are stateful.
3. Use `foreachBatch`. The streaming source emits raw records
   through a stateless transformation chain (parse, project,
   filter), and the streaming query writes via `foreachBatch`.
   Inside the `foreachBatch` body, the per-batch DataFrame is
   *bounded* — just this micro-batch's records — and the streaming
   flag is gone. You can now run any batch operation on it,
   including `groupBy + count + orderBy(desc("count")) + limit(10)`.
   The result is written to a sink (Delta, etc.) from inside the
   function.

   This sidesteps both restrictions because:
    - **`orderBy` is legal in batch.** The bounded per-batch
      DataFrame has a well-defined sort.
    - **`groupBy` over a bounded DataFrame doesn't create streaming
      state.** The state-store machinery isn't engaged; the
      aggregation is just a Catalyst-planned shuffle on a finite
      dataset.

   What the "top 10" means: this is **top 10 within this
   micro-batch**, not across all history. If you wanted top 10
   across all history, you'd need either a true streaming
   aggregation with watermarked state (Tier 2), or a `foreachBatch`
   that reads the existing top-10 table, merges this batch's
   counts, recomputes top-10, and writes back — externalising the
   state to a Delta table rather than to the state store.

</details>

---

[← Tier 1 index](./README.md) · [Previous: Triggers ←](./06-triggers.md) · [Next: Checkpointing Basics →](./08-checkpointing-basics.md)