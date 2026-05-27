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

- **One folder per tier** of the roadmap.
- **One markdown file per concept**, named for the concept, numbered in reading
  order.
- Each tier folder has a short **`README.md` index**; each concept file ends with a
  **"Prove you got it"** self-check (answers in a collapsible block).

---

## Roadmap & progress

**Legend:** ✅ written · 🚧 in progress · ⬜ not started

### ✅ [Tier 0 — First Principles & Mental Models](./tier-0-first-principles/README.md)

Correct mental models before any code. **Status: complete.**

1. [Stream–Table Duality](./tier-0-first-principles/01-stream-table-duality.md)
2. [Unbounded vs Bounded Data](./tier-0-first-principles/02-unbounded-vs-bounded-data.md)
3. [The Clocks](./tier-0-first-principles/03-the-clocks.md)
4. [Delivery Semantics](./tier-0-first-principles/04-delivery-semantics.md)
5. [Processing Models](./tier-0-first-principles/05-processing-models.md)
6. [DStreams (for contrast)](./tier-0-first-principles/06-dstreams-for-contrast.md)

### ⬜ Tier 1 — Structured Streaming Core

`readStream`/`writeStream`, streaming DataFrames & Datasets, sources & sinks
(incl. `foreachBatch`), output modes, triggers, stateless transforms,
checkpointing basics.

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
**architecture-level design**. Those four areas *are* the senior delta .

---

## Status

Tier 0 complete. Tier 1 next.
