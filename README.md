# Spark Streaming, From First Principles

Notes and mental models for going from *"I know batch Spark"* to *"I can design and
operate production streaming pipelines."* Built while working through the
[Rock-the-JVM](https://courses.rockthejvm.com/p/the-spark-bundle) **"Spark Streaming with Scala"** course and a beginner → senior Data
Engineer roadmap, with everything **derived, not asserted** — the goal is correct
mental models you can re-derive, not facts you memorize.

**Focus:** Spark **Structured Streaming** (DStreams referenced only for contrast).
**Language:** Scala.

---

## How this repo is organized

- **`notes/`** — one folder per tier of the roadmap, one markdown file per concept.
  Each tier folder has a short `README.md` index; each concept file ends with a
  **"Prove you got it"** self-check (answers in a collapsible block).
- **`src/main/scala/demos/`** — runnable Scala demos that empirically verify the
  concepts. Standard sbt layout. Each demo's tier folder mirrors the notes
  hierarchy (`demos/tier1/output_modes/`, etc.).
- **`build.sbt`** — pinned to Scala 2.13.16, Spark 4.0.0, Delta 4.0.0; JDK 17+.

Setup: `sbt compile` from the repo root resolves dependencies. Demos read from
a local Kafka broker on `localhost:9092` and write to local Delta tables under
`/tmp/`.

---

## Roadmap & progress

**Legend:** ✅ written · 🚧 in progress · ⬜ not started

### ✅ [Tier 0 — First Principles & Mental Models](./notes/tier-0-first-principles/README.md)

Correct mental models before any code. **Status: complete.**

1. [Stream–Table Duality](./notes/tier-0-first-principles/01-stream-table-duality.md)
2. [Unbounded vs Bounded Data](./notes/tier-0-first-principles/02-unbounded-vs-bounded-data.md)
3. [The Clocks](./notes/tier-0-first-principles/03-the-clocks.md)
4. [Delivery Semantics](./notes/tier-0-first-principles/04-delivery-semantics.md)
5. [Processing Models](./notes/tier-0-first-principles/05-processing-models.md)
6. [DStreams (for contrast)](./notes/tier-0-first-principles/06-dstreams-for-contrast.md)

### ✅ [Tier 1 — Structured Streaming Core](./notes/tier-1-structured-streaming-core/README.md)

The everyday API of Structured Streaming, derived from first principles.
**Status: complete.**

1. [`readStream` / `writeStream`](./notes/tier-1-structured-streaming-core/01-readstream-writestream.md)
2. [Streaming DataFrames & Datasets](./notes/tier-1-structured-streaming-core/02-streaming-dataframes-and-datasets.md)
3. [Sources & Replayability](./notes/tier-1-structured-streaming-core/03-sources-and-replayability.md)
4. [Sinks & `foreachBatch`](./notes/tier-1-structured-streaming-core/04-sinks-and-foreachbatch.md)
   4.5. [Delta Lake as a Streaming Sink](./notes/tier-1-structured-streaming-core/04.5-delta-as-streaming-sink.md)
5. [Output Modes](./notes/tier-1-structured-streaming-core/05-output-modes.md)
6. [Triggers](./notes/tier-1-structured-streaming-core/06-triggers.md)
7. [Stateless Transformations](./notes/tier-1-structured-streaming-core/07-stateless-transformations.md)
8. [Checkpointing Basics](./notes/tier-1-structured-streaming-core/08-checkpointing-basics.md)

Includes runnable demos at `src/main/scala/demos/tier1/` covering Delta as a
streaming sink (Concept 4.5) and output modes (Concept 5).

### ⬜ Tier 2 — Event Time & State

Streaming aggregations, event-time/session/processing-time windows, **watermarks**,
streaming joins, deduplication, arbitrary stateful processing (legacy
`mapGroupsWithState` and the modern Spark 4.0 `transformWithState`).

### ⬜ Tier 3 — Integrations & the Lakehouse

Kafka deep dive, Schema Registry + Avro/Protobuf, idempotent JDBC upserts,
`foreachBatch` patterns, Delta/Iceberg lakehouse sinks, CDC.

### ⬜ Tier 4 — Production Engineering

End-to-end exactly-once, state-store internals (RocksDB), changelog checkpointing,
State Data Source reader, async progress tracking, backpressure, performance tuning,
checkpoint evolution, monitoring & observability.

### ⬜ Tier 5 — Reliability, Quality & Ops

Testing streaming pipelines, data quality & dead-letter queues, declarative
pipelines, idempotency/reprocessing/backfill, deployment & runtime, CI/CD, cost & DR.

### ⬜ Tier 6 — Architecture & System Design

Lambda vs Kappa, medallion architecture, engine selection, the
latency/throughput/cost triangle & SLAs, governance & lineage.

### ⬜ Tier 7 — Scala Skills That Carry the Pipeline

Encoders & the typed Dataset API, implicits and Scala 3 `given`/`using`,
ADTs/sealed traits for event modeling, and where *not* to over-invest.

---

## The senior delta (why this roadmap exists)

A foundations course makes you **fluent in the Structured Streaming model and
event-time/state semantics.** It does **not** cover production engineering
(exactly-once wiring, state-store internals, monitoring, testing, deployment), the
**lakehouse/CDC integration layer**, the **Spark 4.0 `transformWithState` API**, or
**architecture-level design**. Those four areas *are* the senior delta.

---

## Status

Tier 0 and Tier 1 complete. Tier 2 (Event Time & State) next.