# Streaming Joins

> **Tier 2 · Concept 5 of 9**
> Two kinds, and they could not be more different: stream–static is a stateless
> per-batch join; stream–stream is a stateful buffer-and-match bounded by
> watermarks. Everything hard lives on the stream–stream side.

---

## The split

- **Stream–static** — one streaming side, one batch (lookup/dimension) side.
- **Stream–stream** — both sides streaming.

Not two flavors of one thing. Different execution models, different state,
different correctness rules. Take them separately.

---

## Stream–static: stateless

The static side is fully present, so each micro-batch is joined against it exactly
like a batch join, run once per trigger. A streaming row arrives, matches (or not),
emits, and is done — **the engine never has to remember it** to match something that
arrives later. So:

- No watermark, no windows, no time bound (there is no state to bound).
- Batch–streaming equivalence (Tier 1 C7) holds directly.
- Join-type/side legality still applies: the stream must sit on the correct side of
  an outer join (emitting unmatched *static* rows over an unbounded stream is
  meaningless).

---

## Two hash-join algorithms — the interview contrast

**Batch hash join.** One side is the *build* side: hash all its rows into a table,
then *stream* the other (probe) side through, looking each row up. Asymmetric —
one side builds, one side probes — and both sides are fully available, so the
result is final in one pass.

**Symmetric hash join (stream–stream).** Neither side is fully available, and a
match can arrive on *either* side later. So **both sides build and both sides
probe**. Each arriving row is added to *its own* side's hash state and probes the
*other* side's state for matches. Two build tables instead of one, both persisted
in the state store across batches.

The move from one build side to two is the whole reason stream–stream joins are
stateful: you must retain both sides because the counterpart may still be coming.

---

## Stream–stream: what's in state, and the flow

Both sides buffer in the state store as a **multi-map** (one join key → many rows).
Physically that's two stores per side: `keyToNumValues` (key → count) and
`keyWithIndexToValue` ((key, i) → row) — because a plain KV store can't hold a list.

Per input row, on each side:

1. drop the row if it's older than the watermark (admission filter)
2. **probe** the other side's state → emit matches satisfying the condition
3. **append** the row to its own state
4. evict old state on both sides (see below)

Probe happens *before* append — a row does not match itself.

---

## The three required pieces (stream–stream)

```scala
val matched = impressions.withWatermark("impressionTime", "5 seconds")
  .join(
    clicks.withWatermark("clickTime", "5 seconds"),
    expr("""
      impAdId = clkAdId AND
      clickTime >= impressionTime AND
      clickTime <= impressionTime + interval 10 seconds
    """),
    "inner")
```

1. **watermark on both sides** — how late each stream can be
2. **equality predicate** (`adId`) — the join key
3. **two-directional time-range predicate** — how far apart a matching pair may sit

The time bound must constrain **both** directions (`cT >= iT` *and*
`cT <= iT + interval`). Each direction bounds the *opposite* side; a one-sided
bound leaves one side's state unbounded. Miss it entirely → unbounded state (inner)
or the query is rejected (outer).

---

## State eviction — derived

The two per-side watermarks are reconciled into **one global watermark**
`W = min(leftWm, rightWm)` (min policy, Concept 5). Eviction uses `W` on both sides
— using each stream's own watermark would defeat the purpose of a global one.

Derive the eviction threshold from the watermark promise (every future row has
event time `≥ W`) pushed through the time condition.

**Left (impressions).** A future click matching an impression needs
`cT <= iT + interval` and `cT >= W`. Chain:

```
W <= cT <= iT + interval   ⟹   W <= iT + interval
```

A match is still possible while `iT + interval >= W`. So evict when:

```
iT + interval < W          (strict <)
```

**Right (clicks).** A future impression needs `cT >= iT` and `iT >= W`, so
`cT >= iT >= W`. A match is possible while `cT >= W`. Evict when:

```
cT < W                     (strict <)
```

**The boundary is strict `<`** — not the non-strict `<=` that aggregation uses on
`window.end` (Concept 4). The strictness comes from the join condition's
inequalities interacting with the watermark promise, not from the comparison
operator alone. Confirmed empirically (below); the exact source line producing the
strictness was not traced — everything else here is derived or confirmed from the
physical plan's `state cleanup` predicates.

Eviction uses the **inherited** watermark (Concept 4 timing): the `W` computed at
the end of batch N governs eviction in batch N+1.

---

## The boundary, proven

Probe: target impression `X@10` (so `iT + interval = 20`), watermark threshold 5,
interval 10. A parked impression keeps `W` click-bound; clicks step `W` through
the boundary. Reading by `batchId` and the inherited `wm`:

```
batchId  inherited W   X@10 removed?
  2          19            no
  3          20            no      <- non-strict <= would evict here
  4          21            YES     <- strict < evicts here
```

`X@10` survives `W = 20` and leaves only at `W = 21`. So `iT + interval < W`
(strict), not `<=`. This also explains why an already-matched row like `A@10`
lingers one boundary longer than a naive `<=` predicts.

Full runnable demo (impressions/clicks, all six edge cases + this probe):
`demos/tier2/StreamStreamJoinDemo.scala`.

---

## Inner vs outer

- **Inner** — emit only matches; unmatched rows just vanish at eviction.
- **Outer** — must emit null-padded unmatched rows, and can only know a row is
  unmatched once its match window has closed under the watermark. So the null row
  is emitted **at eviction time**, which is exactly why outer joins *mandate* the
  time-range bound: without it there's no moment at which "unmatched" becomes
  decidable. (Left-outer demo parked for later.)

---

## Instrumentation gotchas (verified in the demo)

- **`numRowsTotal` is both sides combined** (SPARK-35896), not per-side. Per-side
  inspection needs the State Data Source reader (Tier 4).
- **`numRowsDroppedByWatermark` is NOT populated by joins.** A late input row is
  dropped by simply *not appending* it — the admission check increments no counter.
  This is an aggregation-only metric; the Concept 4 assumption does **not** carry.
- **Two independent `MemoryStream`s ≠ one batch per `processAllAvailable`.** Each
  source advances its own offset; the engine may split them. Assert on `batchId`,
  never on `addData` grouping.

---

## Spark 3.x → 4.x note

Stream–stream join semantics (three required pieces, global-min watermark, strict
eviction boundary, outer-join mandate) are stable 2.3 → 4.x. On 4.1.x the join
internals and `MemoryStream` live in `...execution.streaming.runtime`. Supported
outer-join types and real-time-mode join limits are version-dependent — check the
migration notes rather than assuming.

---

## Prove you got it

1. Why is stream–static stateless but stream–stream stateful? Answer in terms of
   what the engine must remember.
2. An inner stream–stream join has `cT BETWEEN iT AND iT + 30`, watermark threshold
   10 on both. An impression `iT = 100` sits in state. At what global `W` is it
   evicted, and why that value?
3. Why does omitting the time-range predicate merely make an *inner* join's state
   unbounded, but make an *outer* join impossible to execute correctly?

<details>
<summary>Answers</summary>

1. Stream–static: the static side is fully present, so each streaming row is
   decided immediately and forgotten — nothing to remember. Stream–stream: both
   sides are unbounded and a match may arrive later, so both sides must be buffered
   in state until the watermark proves no match can still come.
2. Evict when `iT + interval < W` → `100 + 30 < W` → `W > 130`, i.e. the first
   `W = 131` (strict `<`). Not `130`: at `W = 130` a future click at exactly
   `cT = 130` could still satisfy `cT <= 100 + 30` and `cT >= W`, so the impression
   is not yet safe to drop.
3. Inner emits only matches, so an unbounded buffer is a resource problem, not a
   correctness one. Outer must emit unmatched rows as nulls, and "unmatched" is only
   decidable once the match window closes under the watermark — no time bound means
   no closing point, so the engine can never decide to emit the null row.

</details>

---

[← Concept 4: Watermarks Part 2](./04-watermarks-part2.md) · [Next: Deduplication →](./06-de-duplication.md)