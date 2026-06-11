# Tier 1 — Structured Streaming Core

The everyday API of Structured Streaming, derived from first principles
and anchored at the end to the checkpoint that coordinates it all.

This tier builds on Tier 0's foundations (stream–table duality, the
incrementalized batch query, the clocks, delivery semantics, processing
models, DStreams for contrast). Each concept below is a piece of the
streaming machinery in normal API use; Tier 2 will add event time and
state, and Tier 4 will go below the API into engine internals.

---

## How to read this tier

Concepts are numbered in the recommended reading order. Each one
builds on the previous: by Concept 8, you have the complete mental
model of how a streaming query is planned, executed, and recovered.

For each concept:

- The `.md` file is the durable artefact — the precise derivation,
  cross-references, and prove-you-got-it questions with answers.
- Selected concepts have runnable demos in `src/main/scala/demos/`.
  Demos are linked at the bottom of the concept file.

The tier ends at Concept 8 — Checkpointing Basics — by design. The
checkpoint is the shared substrate every other concept relies on,
but it only makes sense once you've seen the things it coordinates.

---

## Concepts

### [Concept 1 — `readStream` / `writeStream`](./01-readstream-writestream.md)

The entry and exit points of every Structured Streaming program. The
streaming flag on the Catalyst plan is what turns the same DataFrame
API into a long-running incremental computation.

**Key fact:** `readStream` returns the *same* `DataFrame` type as
`read`. The difference is a flag on the logical plan that turns
Catalyst into a correctness gate at plan time and a long-running
trigger loop at execution time.

---

### [Concept 2 — Streaming DataFrames & Datasets](./02-streaming-dataframes-and-datasets.md)

Why schema must be declared up front for most streaming sources, and
why `Dataset[T]`'s `Encoder[T]` requirement is non-negotiable in the
streaming world. Both questions share a root cause: planning happens
once, execution runs forever.

**Key fact:** the schema and the encoder are two contracts, both
fixed at plan time. Schema fails at `start()`; encoder fails at
compile time.

---

### [Concept 3 — Sources & Replayability](./03-sources-and-replayability.md)

The first leg of the end-to-end exactly-once chain. A source is
replayable when it has addressable positions, deterministic re-reads,
and monotonic progress. Only Kafka satisfies all three
unconditionally.

**Key fact:** the socket source is at-most-once by construction. The
file source borrows replayability from storage immutability. The rate
source's `timestamp` is regenerated on replay. Kafka is the only
production-grade source for exactly-once.

---

### [Concept 4 — Sinks & `foreachBatch`](./04-sinks-and-foreachbatch.md)

The third leg of the chain, and the most important production pattern
in Tier 1. The contract is sink-side idempotency or transactionality;
`foreachBatch` is the universal escape hatch that lets you build
either against any target. Includes the two paths for Kafka → Spark →
Kafka exactly-once.

**Key fact:** the unit of idempotency is the batch (via `batchId`),
not the record. `foreachBatch`'s `batchId` is the same id Spark
records in its commit log, which is the same id Delta records in its
`txn` action.

---

### [Concept 4.5 — Delta Lake as a Streaming Sink](./04.5-delta-as-streaming-sink.md)

An interlude between Concept 4 (general sink contract) and Concept 5
(output modes). The mechanism by which Delta closes the exactly-once
chain — the `txn` action of `(appId, batchId)` written atomically
with the data — and the subtle hole that `foreachBatch` re-opens.

**Key fact:** streaming `format("delta")` writes `txn` actions
automatically. `foreachBatch` + plain `MERGE` does not — the batch
Delta API has no streaming context. Re-establish idempotency with
`.option("txnVersion", batchId).option("txnAppId", "...")` when the
merge is not naturally idempotent.

---

### [Concept 5 — Output Modes](./05-output-modes.md)

The three ways Structured Streaming converts the evolving result
table back into records — and the place where Tier 0's stream–table
duality finally surfaces as an API choice. `complete` is snapshots,
`update` is CDC on the result table, `append` is the finalised
stream.

**Key fact:** output modes are *not* arbitrary API choices. Each
mode has a specific semantic contract, and a query is legal in a
mode only when the engine can honour that contract on it.

**Demos:** `src/main/scala/demos/tier1/output_modes/`

---

### [Concept 6 — Triggers](./06-triggers.md)

The control that decides *when* a streaming query fires a new
micro-batch. Triggers are a four-way knob — latency, throughput,
cluster cost, and commit frequency — that you turn to match the
pipeline's actual product purpose.

**Key fact:** the default trigger is rarely what you want. The
`AvailableNow` trigger lets you run streaming queries in batch-job
patterns — same machinery, none of the always-on cost. The catchup
pattern (`AvailableNow` then `ProcessingTime`) is the right move
after an outage.

---

### [Concept 7 — Stateless Transformations](./07-stateless-transformations.md)

The shortest concept in the tier, by design. Stateless transformations
are batch transformations applied to a streaming DataFrame, and Spark
goes to considerable trouble to make sure they behave identically.

**Key fact:** the absence of cross-row dependency is what makes
incremental execution trivial — and trivial incremental execution
is what makes batch–streaming equivalence possible. Two genuine
exceptions: `dropDuplicates` is stateful with unbounded state,
`orderBy` over the whole stream is illegal.

---

### [Concept 8 — Checkpointing Basics](./08-checkpointing-basics.md)

The final concept of Tier 1, and the one that ties everything
together. The checkpoint stores five things: query identity, offsets,
commits, source metadata, and state. Knowing what survives across
runs and what doesn't is the operational backbone of production
streaming work.

**Key fact:** safe changes are runtime-loop changes; loud-failure
changes are structural changes the planner detects; silently
dangerous changes are semantic changes that look equivalent to the
planner but aren't. The discipline: a new logical query gets a new
checkpoint location.

---

## Demos in this tier

Each demo runs against a local Spark + Kafka setup and writes to
local Delta tables. Producer runs in its own JVM; demos run
independently against the bronze table the producer feeds.

| Demo                                    | Concept | What it proves                                                         |
| --------------------------------------- | ------- | ---------------------------------------------------------------------- |
| `KafkaSensorProducer`                   | 5       | Synthetic sensor fleet → Kafka topic (shared by all output-modes demos)|
| `Demo01_AppendBronzeIngest`             | 5       | Kafka → Delta in `append` mode; `txn` actions written automatically    |
| `Demo02_CompleteDashboard`              | 5       | Bronze Delta → console in `complete` mode; live snapshot dashboard    |

A planned `Demo03_UpdateCurrentState` (the canonical
`update` + `foreachBatch` + `MERGE` pattern) is deferred to Tier 2,
where watermarked aggregations make it a *true* `update`-mode demo
rather than a per-batch workaround.

Demo path: `src/main/scala/demos/tier1/output_modes/`

---

## What's deferred to later tiers

Items called out in Tier 1 concepts that depend on prerequisites we
will cover in later tiers:

- **Watermarks and event-time aggregations** — Tier 2. The
  `groupBy + watermark` legality unlock for `append` mode, the
  `dropDuplicatesWithinWatermark` operator, true streaming `update`
    + `MERGE` demos.
- **State store internals** — Tier 4. The State Data Source reader,
  changelog checkpointing, RocksDB state store.
- **End-to-end state migration demo** — Tier 4. The Approach 2
  worked example (extract existing state, apply key-set change in
  a batch job, restart streaming against migrated state) needs the
  State Data Source reader, which Tier 4 covers.
- **`transformWithState`** — Tier 2 (modern API beyond
  `mapGroupsWithState`). The Spark 4.0 stateful processor API.

---

[← Repo root](../../README.md) · [Tier 0 →](../tier-0-first-principles/README.md) · [Tier 2 →](../tier-2-event-time-and-state/README.md)