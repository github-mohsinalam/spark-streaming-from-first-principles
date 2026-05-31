# Tier 0 — First Principles & Mental Models

Build these *before* writing a line of streaming code. Most streaming bugs are
really broken mental models — this tier installs the correct ones.

These six concepts are ordered and build on each other: the duality (1) underpins
the "incrementalized batch query" framing (2); the clocks (3) explain *why*
event-time correctness is hard; delivery semantics (4) wire the duality and clocks
into a real exactly-once pipeline; processing models (5) place Spark in the
landscape; and DStreams (6) sharpen the whole thing by contrast.

| # | concept | one-line takeaway |
|---|---------|-------------------|
| 1 | [Stream–Table Duality](./01-stream-table-duality.md) | a table is a *fold* of a stream; a stream is the *changes* of a table — they are inverses |
| 2 | [Unbounded vs Bounded Data](./02-unbounded-vs-bounded-data.md) | streaming = batch *meaning* + incremental *execution* of the same query |
| 3 | [The Clocks](./03-the-clocks.md) | event vs processing vs ingestion time; skew is variable, so records arrive out of order |
| 4 | [Delivery Semantics](./04-delivery-semantics.md) | exactly-once *delivery* is impossible; exactly-once *effect* = replayable source + idempotent sink + correct WAL ordering |
| 5 | [Processing Models](./05-processing-models.md) | Spark is micro-batch: latency floor traded for throughput + easy exactly-once; the SLA picks the engine |
| 6 | [DStreams (for contrast)](./06-dstreams-for-contrast.md) | legacy RDD-per-interval API; reference only — shows what Structured Streaming changed |

---

## How to read this tier

Each file ends with a **"Prove you got it"** section with collapsible answers — work
the checks before moving on. Jargon is unpacked in plain language on first use, so
the files are readable cold by any experience level without losing precision.

> **Course mapping:** these first-principles map onto the foundations the
> Rock-the-JVM "Spark Streaming with Scala" course assumes or introduces in its
> *Introduction* and early sections. The course teaches the *API*; this tier makes
> sure the *why* underneath it is solid.

[← Back to repo root](../../README.md)
