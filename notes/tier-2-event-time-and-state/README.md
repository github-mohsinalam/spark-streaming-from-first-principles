# Tier 2 — Event Time & State

The heart of Structured Streaming: how the engine reasons about *when* an event
happened, what it must remember, and when it is allowed to forget.

Seven concepts. Each note is derived from first principles, checked against a
runnable demo, and sourced to the Spark code.

---

## Concepts

| # | Concept | Notes | Demo                                                                                                                                                                                            |
|---|---|---|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1 | **Streaming Aggregations** — `groupBy.agg` as an incremental fold; unbounded state without eviction | [`01-streaming-aggregations`](./01-streaming-aggregations.md) | —                                                                                                                                                                                               |
| 2 | **Event-Time Windows** — `window()` as a time-interval group key; tumbling vs sliding | [`02-event-time-windows`](./02-event-time-windows.md) | —                                                                                                                                                                                               |
| 3 | **Watermarks** — the eviction rule, and what governs it when there is more than one | [`3-watermarks-part1`](./03-watermarks-part1.md) · [`03-watermarks-part2`](./03-watermarks-part2.md) | [WatermarkMemoryStreamDemo](../../src/main/scala/demos/tier2/WatermarkMemoryStreamDemo.scala)                                                                                                   |
| 4 | **Streaming Joins** — stream–static is stateless; stream–stream is a symmetric hash join bounded by watermarks | [`04-stream-joins`](./04-stream-joins.md) | [StreamStreamJoinDemo](../../src/main/scala/demos/tier2/StreamStreamJoinDemo.scala)                                                                                                             |
| 5 | **Deduplication** — a keyed state set, and when a key may be forgotten | [`05-de-duplication`](./05-de-duplication.md) | [DropDuplicatesWithinWatermarkDemo](../../src/main/scala/demos/tier2/DropDuplicatesWithinWatermarkDemo.scala)                                                                                   |
| 6 | **Arbitrary Stateful Processing (legacy)** — `flatMapGroupsWithState`: you write the update function and manage eviction | [`06-arbitrary-stateful-legacy-part1`](./06-arbitrary-stateful-legacy-part1.md) · [`06-arbitrary-stateful-legacy-part2`](./06-arbitrary-stateful-legacy-part2.md) | [HeartbeatNoTimeoutDemo](../../src/main/scala/demos/tier2/HeartbeatNoTimeoutDemo.scala) · [HeartbeatEventTimeTimeoutDemo](../../src/main/scala/demos/tier2/HeartbeatEventTimeTimeoutDemo.scala) |
| 7 | **`transformWithState`** — the Spark 4.0 replacement: composite state, timers, TTL | [`07-transform-with-state`](./07-transform-with-state.md) | [HeartbeatTransformWithStateDemo](../../src/main/scala/demos/tier2/HeartbeatTransformWithStateDemo.scala)                                                                                                                                                       |


**Probes** (throwaway, kept for the findings they produced):
[TwsTtlProbe](../../src/main/scala/demos/tier2/TwsTtlProbe.scala) — TTL is per-element on `ListState`, scoped to the variable.

---

[← Tier 1](../tier-1-structured-streaming-core/README.md) · [Repo index](../../README.md) · [Tier 3 →](../tier-3-integrations-and-lakehouse/README.md)