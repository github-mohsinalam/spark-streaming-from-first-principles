# Output Modes

> **Tier 1 · Concept 5 of 8**
> The three ways Structured Streaming converts the evolving result table
> back into records — and the place where Tier 0's stream–table duality
> finally surfaces as an API choice you have to make every time you write
> a query.

---

## The one-sentence idea

Output modes are the *table→stream* direction of the stream–table duality,
exposed as three different commitments about what a row's emission means.
Each mode has a specific semantic contract; a query is legal in a mode
only when the engine can honour that contract on it.

---

## The framing: outputs as the table→stream direction

Recall Concept 1 of Tier 0:

> A stream and a table are two encodings of the same information. You can
> convert in both directions. `foldLeft` integrates a stream into a table;
> CDC differentiates a table into a stream.

Structured Streaming's programming model uses *both* directions:

- **Input side:** the unbounded source is conceptually a *table that grows
  by appends*. Your query runs against this conceptual table. This is the
  `foldLeft` direction: every new record is folded into the conceptual
  input table.
- **Output side:** the result of your query is *also* a conceptual table —
  the **result table** — that evolves as new input arrives. To emit it to a
  sink, Spark must turn this evolving table back into a stream of records.
  That is the table→stream direction. That is what output modes are.

```
Input stream → [conceptual input table] → query → [conceptual result table] → output stream
              (foldLeft direction)                                            (output mode direction)
```

The output mode is your choice of *how* to differentiate the result table
back into records. There are three modes, and each one corresponds to a
specific notion of "what changed since the last trigger."

---

## The three modes, derived

For each mode, the question to answer is: **after this trigger fires, what
records does Spark emit to the sink?**

### `complete` — emit the entire result table

After every trigger, the sink receives the *whole* current result table.

```
Trigger 1: result table = [{A: 5}, {B: 3}]                  → emit both rows
Trigger 2: result table = [{A: 7}, {B: 3}, {C: 1}]          → emit all three rows
Trigger 3: result table = [{A: 7}, {B: 9}, {C: 1}, {D: 2}]  → emit all four rows
```

The sink sees a *snapshot* each time. Every row is re-emitted whether it
changed or not.

**Use this for:** small dashboards where you want a current-state snapshot
each trigger. The console sink in demos. Real-time leaderboards over a
small key space.

**Avoid for:** anything where the result table grows unboundedly — every
trigger re-emits more data than the last, and the sink load grows without
bound. `complete` against a growing aggregation is a memory and network
bomb.

### `update` — emit only the rows that changed

After every trigger, the sink receives only the rows whose values changed
(or were inserted) since the previous trigger.

```
Trigger 1: result table = [{A: 5}, {B: 3}]                  → emit {A: 5}, {B: 3}
Trigger 2: result table = [{A: 7}, {B: 3}, {C: 1}]          → emit {A: 7}, {C: 1}
                                                                (B unchanged, skipped)
Trigger 3: result table = [{A: 7}, {B: 9}, {C: 1}, {D: 2}]  → emit {B: 9}, {D: 2}
```

This is the **changelog view** of the result table. **`update` is CDC on
the result table** — not an approximation, the literal table→stream
direction applied to the result table itself. Each emitted record is a
mutation event: "this row's value is now X." If you replayed the
`update`-mode output through `foldLeft` with an upsert function, you would
reconstruct the result table at any point — the inverse-pair property
that defines the duality.

**Use this for:** maintaining a current-state table downstream. The
upstream emits changelog records; the downstream's `MERGE` or `ON CONFLICT`
absorbs them and stays current. This is the natural pairing with Delta
`MERGE` on a domain key.

**Crucially:** `update` does *not* emit deletions. If a row falls out of
the result table (e.g. a windowed aggregation whose window has expired),
`update` is silent about it. That asymmetry matters for some pipelines.

### `append` — emit only newly-finalised rows

After every trigger, the sink receives only the rows that are *new* and
*will never change again*.

```
Trigger 1: result table = [{A: 5}, {B: 3}]
           Are these final? Spark does not know yet → emit nothing.

Trigger 2: result table = [{A: 7}, {B: 3}, {C: 1}]
           A changed → not final. No row is provably final yet → emit nothing.

...

Trigger N: a watermark fires; the window for the 10:00–10:05 bucket closes.
           {A: 7, window: 10:00–10:05} is now provably final → emit it.
```

`append` only emits rows when Spark can prove **no future input will
change them**. For non-aggregated queries (filter, map, project), every
input row produces an output row that will never change, so `append`
works naturally. For aggregations, Spark needs a *watermark* (Tier 2) to
know when a window is closed and its row can be finalised.

**Use this for:** event ingest (immutable facts, one input → one output,
no aggregation) — the default and natural mode for raw event streams.
Also for aggregations *with watermarks*, when you want the lakehouse's
append-only invariant.

**The defining property:** once a record is in the sink, it never gets a
corrective update later. Append-only sinks (Parquet files, Delta tables
without `MERGE`) work *only* with this mode because they have no way to
express "update the row I emitted three minutes ago."

---

## All three modes are table→stream — the differences

| Mode      | Emits...                                | Form              | Pairs with...                          |
| --------- | --------------------------------------- | ----------------- | -------------------------------------- |
| `complete`| the whole result table                  | full snapshot     | small bounded result tables; dashboards|
| `update`  | only changed/inserted rows              | CDC / changelog   | upsert-by-key sinks (Delta `MERGE`)    |
| `append`  | only rows provably final, exactly once  | finalised stream  | append-only sinks (Parquet, plain Delta)|

All three are the table→stream direction, distinguished by **when** a row
is emitted and **whether** it can be re-emitted later. `complete` snapshots;
`update` differentiates; `append` waits for finalisation.

---

## The legality matrix — derived from the contracts

Most tutorials present this as a flat table. Derive it instead — the rules
fall out cleanly from the modes' contracts.

The question for any query Q in any mode M is: **can Spark produce a
correct stream of records under M's semantics for Q?**

Consider what Q does:

| Query shape                    | What can change in the result table?                                       |
| ------------------------------ | -------------------------------------------------------------------------- |
| Stateless (filter, map, select) | Nothing — each output row is final the moment it is emitted. Rows only insert. |
| Aggregation, no watermark      | *Any* existing aggregation row can change (late data could update any group, forever). |
| Aggregation, with watermark    | Rows in *open* windows can change; rows in *closed* windows (past watermark) are final. |
| Deduplication                  | Once a row is emitted, it is stable; new rows only insert.                  |

Now apply each output mode:

| Query shape                    | `append`              | `update`              | `complete`            |
| ------------------------------ | --------------------- | --------------------- | --------------------- |
| Stateless                      | legal — default fit   | legal — identical to append here | illegal — "emit the whole table" is unbounded |
| Aggregation, no watermark      | illegal — no row is ever provably final | legal — emits the changing group | legal — snapshots the table |
| Aggregation, with watermark    | legal — closed windows emit on watermark advance | legal — emits the changing group | legal — snapshots the table |
| Deduplication                  | legal — each emitted row is final | legal — identical to append here | illegal — unbounded |

The mode is rejected at plan time (analysis error) when the combination is
invalid: *"Complete output mode not supported when there are no streaming
aggregations"* and similar. The planner refuses to plan a query whose
semantics it cannot honour. This is the same correctness-gate behaviour as
the streaming flag from Concept 1: illegal combinations fail at `start()`,
not at runtime after consuming gigabytes.

### A note on `dropDuplicates`: legal but operationally fragile

The legality matrix marks `dropDuplicates` as legal in both `append` and
`update` modes. That is true — its emission is one row per unique key,
final on first occurrence, which satisfies `append`'s "never revised"
contract. But legality is not sufficiency. The engine maintains the
deduplication state keyed by every distinct value ever seen, and that
state has **no eviction boundary**. A duplicate could arrive years later,
so the engine cannot forget any key. On a high-cardinality stream
(per-event-id, per-request-id, per-user-id) the state store grows
linearly with the number of unique keys — gigabytes within days is
realistic at production scale.

>**The takeaway:** the legality matrix tells you what *runs*, not what *stays
>healthy under load*. Several stateful operators are legal but maintain
>unbounded state without a watermark — `dropDuplicates` is just the most
>common one a developer is likely to reach for. Production-grade streaming
>work means choosing watermarked variants whenever possible, which is
>exactly what Tier 2 is about.

### Two derivations worth doing explicitly

**Why `append` is illegal for an aggregation without a watermark.** The
semantic of `append` is "emit only rows that will never change again."
Without a watermark, Spark has no boundary that says "no more data will
arrive for this group." Any future late record could update any group. No
row is ever provably final. The mode's contract cannot be honoured →
plan-time rejection.

**Why `complete` is illegal for a non-aggregating query.** The result
table for `filter`/`select`/`map` is monotonically growing: every input
row produces an output row, all of which persist. Re-emitting the whole
table on every trigger is "send the entire history so far, forever." This
is not "unsupported" in some narrow technical sense — it is **semantically
the wrong primitive** for a streaming query, so the planner refuses it.

This is the deepest point about output modes: they are not an arbitrary
API choice. **Each mode has a specific semantic contract, and a query is
legal in a mode only when the engine can honour that contract.**

---

## The watermark+window unit, briefly

The fix that takes an aggregation from "illegal in `append`" to "legal in
`append`" is not just a watermark — it is **watermark and window
together**, and both pieces are necessary:

- A watermark without a window does not help. `groupBy("userId").count()`
  has no window for the watermark to close.
- A window without a watermark does not help. Spark would have no notion
  of "watermark has passed this window's end" to trigger emission.

The combination is what creates *finalisable* result rows: rows that
belong to a window whose closing time is provably past the watermark, and
which therefore can never change. Both pieces are derived properly in
Tier 2; this is the foreshadow.

---

## The mode/sink pairing

`update` mode emits changes. On retry of batch N after a crash, those same
changed-row records are emitted again (replayable source + checkpoint do
their job). The sink receives the *same row* with the *same value* twice.
Is that a duplicate?

It depends on what the sink does:

- **Delta `MERGE` on key:** the second `MERGE` writes the same value the
  row already has. Idempotent. No problem.
- **Plain append to a table:** two rows now exist for the same logical
  key. **Bug.**
- **JDBC `INSERT ... ON CONFLICT UPDATE`:** same value overwritten.
  Idempotent.

So `update` mode pairs naturally with **upsert-by-key** sinks and pairs
badly with append-only sinks. This is the reason Delta `MERGE` is the
canonical pairing for streaming aggregations: the mode emits changes, the
sink absorbs them by key. Square peg, square hole.

### A diagnosis pattern worth knowing

If a teammate runs an aggregation in `update` mode against a plain
append-only sink and is surprised by "duplicates" (multiple rows per
logical key in the output), the pipeline does not have a bug — it has a
**missing decision**. Two legitimate fixes exist:

- **Fix A: change the sink.** Stay in `update` mode upstream; switch the
  sink to Delta `MERGE` on key. Preserves the *continuously-updating
  current state* semantic — the latest aggregate per key is always
  available, updated every trigger.
- **Fix B: change the mode.** Stay with the append-only sink; switch the
  query to `append` mode by adding a watermark + windowed `groupBy`.
  Preserves the *append-only immutability* semantic — you accept some
  latency, in exchange for an immutable history of finalised
  window-aggregates.

Which fix is right depends on what the pipeline is *for*: current state,
or immutable history. Forcing the choice is what surfaces the real
product question.

---

## Spark 3.x → 4.x note

No gap. The three output modes and their legality rules are identical
across Spark 3.x and 4.x. Error-message wording occasionally changes
between minor releases; the semantics do not.

---

## Prove you got it

1. **Derive the legality.** Without consulting the matrix above, derive
   *from the semantics of each mode* why this query is illegal in `append`
   mode but legal in `update` and `complete`:
   ```scala
   events
     .groupBy("userId")
     .count()
     .writeStream
     .outputMode("???")
     // no watermark
   ```
   Then explain what *single change* (counted as one logical fix, even if
   it involves two API calls) makes `append` legal too.
2. **The duality showing up.** Explain how the three output modes are
   direct expressions of the *table→stream direction* from Tier 0's
   stream–table duality. Which mode corresponds most directly to "CDC over
   the result table," and why?
3. **The mode/sink pairing.** A teammate writes a streaming pipeline that
   runs an aggregation (`groupBy + sum`) in `update` mode and writes to a
   plain Parquet append-only sink. They are surprised that the output
   table contains multiple rows per logical key. Explain what is actually
   happening, why their setup is fundamentally mismatched, and name *both*
   legitimate fixes — including what semantic each fix preserves.

<details>
<summary>Answers</summary>

1. `append` emits only rows that are *provably final* — rows Spark can
   guarantee will never change. The query `groupBy("userId").count()` has
   no watermark and no window, so any group's count could be updated by
   any future record arriving for that user — no row is ever provably
   final. `append` cannot honour its semantic contract here → plan-time
   rejection. `update` is fine because it just emits the current values
   of rows that changed (no finality required). `complete` is fine
   because it snapshots the whole table each trigger (also no finality
   required). The single change that makes `append` legal is
   **watermark + windowed `groupBy`** — both pieces together. The
   watermark provides Spark's commitment that "I will not accept
   event-times earlier than this point"; the window gives the aggregation
   a closing time for the watermark to pass. Each closed window's row is
   then provably final and can be emitted exactly once. Either piece
   alone is insufficient.
2. The duality says a table and a stream are inverse encodings of the
   same information: `foldLeft` integrates a stream into a table; CDC
   differentiates a table into a stream. Structured Streaming uses both
   directions — the input is folded into a conceptual table, the result
   table is differentiated back into records by the output mode. All
   three modes are table→stream, differing only in *when* and *how* a row
   is emitted: `complete` is the snapshot form (emit the whole table),
   `append` is the finalised-stream form (emit each row exactly once once
   stable), `update` is **CDC on the result table** — not an
   approximation but the literal differential form, emitting one record
   per row mutation. Folding the `update`-mode output back through an
   upsert function reconstructs the result table, which is the
   inverse-pair property that *defines* the duality.
3. `update` mode emits a record every time a key's aggregate value
   changes — and on retry of any batch, the same changed-rows are
   re-emitted. A plain append-only sink has no way to express "overwrite
   the row I emitted three minutes ago," so each emission becomes a new
   row in the output. The result is multiple rows per logical key. The
   setup is mismatched because `update` is differential (CDC) but the
   sink is accumulative (append). Two legitimate fixes:
    - **Fix A — change the sink** to Delta `MERGE` on the key. Preserves
      the *continuously-updating current state* semantic: the latest
      aggregate per key is always available, refreshed every trigger.
    - **Fix B — change the mode** by adding watermark + windowed
      `groupBy` and switching to `append`. Preserves the *append-only
      immutability* semantic: each window's aggregate is finalised once
      and never revised; you trade latency for an immutable history. The
      right fix depends on whether the pipeline's product purpose is
      current state or immutable history — choosing one is what surfaces
      the real product decision.

</details>

---

[← Tier 1 index](./README.md) · [Previous: Delta as Streaming Sink ←](./04.5-delta-as-streaming-sink.md) · [Next: Triggers →](./06-triggers.md)