# Kafka as a Source — Part 3: Consumers, Memory, and Offset Variants

> **Tier 3 · Concept 1 — Part 3 of 3**
> Part 2 turned the plan into tasks. Each task needs a `KafkaConsumer`, and that
> consumer holds state the batch's record count never accounts for. This part is
> that state — what's resident, what OOMs, what the one log line tells you — plus
> the remaining ways to name a starting position. Closes the source side.
>
> **Depth note:** consumer internals are worth *recognising* while debugging, not
> deep study. This keeps only what earns its place in a future incident.

---

## The gap the batch size doesn't cover

You sized executors for a 200 MB batch, as Part 1 said to. They OOM anyway, and
the heap shows hundreds of MB of Kafka records that belong to no running task.
Two more facts from the same run:

- A task reading 250 records issues **one** network round-trip, not 250.
- Re-running the same batch is faster on the read side, on a cluster caching
  nothing.

One fact, three angles: **the executor holds Kafka state that outlives the task
that created it.** `maxOffsetsPerTrigger` counts records *in a batch*; what OOMs
is what's *resident*. Different quantities.

---

## Why `poll()` can't honour your range

A task reads `p3 [500, 750)` and its last poll returns records `[700, 1400)` — 650
surplus, discarded. Why not fetch exactly `[500, 750)`?

**Because the range is a Spark idea; Kafka has never heard of it.** The consumer
API is `assign` → `seek(offset)` → `poll(timeout)`. There is no "stop at 750" — no
method, no field in the fetch request. Spark's call passes only a start:

```scala
consumer.fetch(offset, pollTimeoutMs)   // untilOffset never leaves the JVM
```

The broker is asked: *from offset 500, up to `max.partition.fetch.bytes` of
records.* It fills that budget and returns; the `untilOffset` is a Spark-side loop
bound.

This is structural, not a Spark inefficiency. The broker serves fetches with
`sendfile` — a byte range from a segment file, kernel to kernel, records never
entering user space. Honouring an offset end would force the broker to *parse*
record batches to find 750, destroying zero-copy. A byte budget is checkable
without reading contents; an offset budget is not. **The protocol trades an exact
end for zero-copy** — the right trade, since discarding a tail is cheap and happens
in the consumer's user space where it costs no one else. The fetched-data pool
below exists to recover that discarded tail.

---

## The read path: two pools, two keys, one consumer

A task reading `p3 [500, 750)`:

**Identity.** Reuse needs a key. A consumer positioned on `p3` is useless to a
task reading `p7` (switching costs `assign` + `seek` — most of the setup). So:

```scala
case class CacheKey(groupId: String, topicPartition: TopicPartition)
```

This one key addresses **both** pools. Part 2's `getLocation` hashes the
topic-partition to an executor *precisely so this key hits* — locality exists to
make reuse possible.

**Setup cost being avoided** (per new consumer): TCP connections + coordinator
lookup, a metadata fetch, socket/fetch buffers. Tens of ms, a few hundred KB.
Irrelevant for a million-record task; **dominant** for a 250-record task on a
1-second trigger, paid every batch. Hence the pools.

**The steps:**

1. `getOrRetrieveConsumer()` — consumer pool, keyed `(groupId, p3)`. Idle instance
   → reuse. None and pool < 64 → create. None and pool == 64 → try to evict an
   idle one; if none idle, **create anyway** (see soft cap).
2. `getOrRetrieveFetchedData(500)` — fetched-data pool, same key + start offset.
   Hit → **skip the network**. Miss → empty buffer.
3. Empty buffer → `fetchData`: `consumer.fetch(500, …)`. Broker returns 500..1399
   (whatever fits). Records 501–749 served **from the buffer, zero network** — the
   one-round-trip observation.
4. Task stops at 750 because *it* counts, not Kafka. `releaseConsumer()` → pool,
   now idle. `releaseFetchedData()` → pool, holding the 650 unread records.
5. **Next batch** starts p3 at 750, scheduled to the same executor by
   `preferredLoc`. Consumer found (no TCP), buffer found at 750 (no poll): first
   650 records cost one hash lookup — the "faster re-run" observation.
6. Idle > 5 min → evictor reclaims both.

---

## The two pools, and which one OOMs

### Consumer pool — bounded, soft

| config | default | meaning |
|---|---|---|
| `spark.kafka.consumer.cache.capacity` | **64** | consumers cached per executor JVM |
| `spark.kafka.consumer.cache.timeout` | **5m** | min idle before eviction |
| `spark.kafka.consumer.cache.evictorThreadRunInterval` | **1m** | evictor sweep interval |

**64 is soft.** Source: *"do its best effort to respect soft capacity but it can
exceed when there's also request to build a new object."* At capacity with no idle
instance for the requested key, it **creates anyway** — correctness over the bound.
So an executor running **> 64 concurrent partition-tasks** exceeds 64 live
consumers and the cache stops functioning: every task creates and discards. A
number to remember against a wide topic or an aggressive `minPartitions`.

### Fetched-data pool — **unbounded**, and the OOM

| config | default |
|---|---|
| `spark.kafka.consumer.fetchedData.cache.timeout` | **5m** |
| `…fetchedData.cache.evictorThreadRunInterval` | **1m** |

**No capacity limit.** A `mutable.HashMap[CacheKey, …]` with an idle-timeout
evictor and nothing capping size or bytes. Resident Kafka data is bounded by:

```
(partitions touched on this executor) × (fetch buffer size) × (entries per key)
```

held up to 5 min after last use. 200-partition topic, 2 MB records, 1 MiB buffer,
an executor touching 50 partitions → ~50 MiB of fetched data belonging to no
running task, on top of the current batch. **Nothing in `maxOffsetsPerTrigger`
accounts for it.** This is the heap-dump OOM.

### Retry invalidation

```scala
if (TaskContext.get().attemptNumber() >= 1) consumerPool.invalidateKey(cacheKey)
```

On a **retry**, the cached consumer for that key is discarded, not reused — a
mid-read failure leaves the position unknown, and inheriting it would make the
retry depend on how far the prior attempt got (non-deterministic replay, the
property Part 1 protects). Cost: a cluster with flaky tasks silently loses its
consumer cache and churns.

---

## Reading the one log line

`release()` logs at **INFO**, once per task, and it's the whole read-path toolkit:

```
From Kafka topicPartition=…-3 groupId=spark-kafka-source-…-executor
read 250 records through 1 polls (polled out 900 records),
taking 12.4 ms, during time span of 38.1 ms for taskId=1043 partitionId=3.
```

Three ratios:

- **`polledOut / read`** — buffer waste. ≫ 1 means fetching far more than the task
  needs → fills the fetched-data pool. High under a mid-partition stop
  (`maxOffsetsPerTrigger` cap, or a `minPartitions` split whose boundary falls
  mid-buffer).
- **`polls / read`** — reuse quality. `read` large with **0 polls** = complete
  fetched-data cache hit.
- **`taking` vs `time span`** — read time vs wall time. A gap means the task spent
  its life off the read path (deserialization, the write side).

**Measured** (`t3c1-ratelimit-a`, no configs): `read == polledOut` on every task —
full drain to a partition end leaves no tail to discard, so overshoot needs a
mid-buffer stop. Polls track records at ~500/poll: `9000→18`, `900→2`, `10→1`.
**500/poll = `max.poll.records`** (Kafka default) bound before `max.partition.fetch.bytes`,
because records are tiny. On 2 MB records the byte cap binds first and you'd see
1–2 records/poll. That ratio tells you which limit governs.

### Where else to look, ranked

1. **Executor INFO log** — the line above. Start here; no config.
2. **JMX** — consumer pool is a commons-pool2 `GenericKeyedObjectPool`, exposing
   `NumActive/NumIdle/CreatedCount/…` per key **only if**
   `spark.kafka.consumer.cache.jmx.enabled=true` (off by default). **`CreatedCount`
   climbing every batch = the cache isn't working** (>64, lost `preferredLoc`, or
   retry churn). `FetchedDataPool` has **no JMX, no metrics** — heap dump only.
3. **Spark UI** — no Kafka-pool info. Task count/input verifies stage arithmetic;
   **Locality Level** is the visible symptom of `preferredLoc=None` (`ANY` vs
   `NODE_LOCAL`) *on a real cluster* — always `PROCESS_LOCAL` on `local[*]`, so not
   observable single-node; GC time / peak execution memory is where fetched-data
   pressure surfaces.
4. **Kafka UI** — fetch rate and bytes-out per partition, the overshoot seen from
   the broker. No consumer-group state (Part 1: `assign()`, not `subscribe()`).

---

## The forced executor consumer config

Three settings Spark imposes on executors (`kafkaParamsForExecutors`, v4.1.2),
each a consequence of something already derived:

```scala
.set(AUTO_OFFSET_RESET_CONFIG, "none")        // unconditional
.set(ENABLE_AUTO_COMMIT_CONFIG, "false")      // unconditional
.setIfUnset(RECEIVE_BUFFER_CONFIG, 65536)     // default, overridable
```

- **`auto.offset.reset = none`** — no silent repositioning if a requested offset is
  gone; Kafka throws, and `failOnDataLoss` (Part 1) decides. Letting Kafka reset
  would steal the decision from the policy. Setting `kafka.auto.offset.reset`
  yourself is a hard error, not a warning.
- **`enable.auto.commit = false`** — Part 1's empty `commit()`, enforced at the
  client so no stray timer writes an offset.
- **`receive.buffer.bytes = 65536`** — a default you *can* override, unlike the
  other two.

Also: setting `kafka.group.id` yourself triggers `CUSTOM_GROUP_ID_ERROR_MESSAGE`
and disables `groupIdPrefix` — Part 1's rebalance-risk warning.

---

## `startingOffsets` variants

Five ways to name a starting position → five `KafkaOffsetRangeLimit` cases. **All
obey Part 1 unchanged: resolved once at birth, written to `sources/0/0`, ignored
forever.** The variant changes only *how the initial position is computed*, never
*when*.

| form | option | case | notes |
|---|---|---|---|
| earliest / latest | `startingOffsets=earliest|latest` | `Earliest/Latest…Limit` | the two you've run |
| per-partition | `startingOffsets={"t":{"0":5000,"1":-2}}` | `SpecificOffsetRangeLimit` | `-2`=earliest, `-1`=latest; omitted partitions **not** defaulted |
| timestamp, global | `startingTimestamp` / `startingOffsetsByTimestamp` (one ts) | `GlobalTimestampRangeLimit` | "since 09:00" without knowing offsets |
| timestamp, per-partition | `startingOffsetsByTimestamp` (map) | `SpecificTimestampRangeLimit` | rarely needed |

**Timestamp caveats** (the one variant worth real caution): it uses Kafka's
`offsetsForTimes` — the first offset with record timestamp ≥ ts — which is the
**broker/producer timestamp, not your event-time field**, so only as trustworthy as
the producer's clock. If a partition has no record ≥ ts, the default **throws**
(`StrategyOnNoMatchStartingOffset.ERROR`); set the strategy to `latest` to start
such partitions at the end instead.

Per-partition JSON is a **birth snapshot, not a standing policy** — its realistic
use is resuming from externally recorded positions (migration, known-good
reprocess point). A later-added partition still → earliest (Part 1).

---

## `endingOffsets` — a batch concept

`endingOffsets` / `endingOffsetsByTimestamp` exist, but validation is decisive:
**streaming rejects an ending offset; batch requires start ≠ latest and end ≠
earliest.** A stream has no end — its end is "wherever the log is now, forever
advancing". So ending offsets only mean something for `spark.read.format("kafka")`,
a bounded `[start, end)`:

- **Streaming:** `startingOffsets` only, birth-only, any of the five forms.
- **Batch:** both ends required-ish; start can't be `latest`, end can't be
  `earliest` (would make the range empty / ill-defined).

The batch read is the tool for one-off replays and backfills — and Part 1's
"reprocess into a new checkpoint" escape from a crash loop is usually a batch read
of a **timestamp range**, not a streaming restart.

---

## Spark 3.x → 4.x

- Pool classes stable in shape; `InternalKafkaConsumerPool`, `FetchedDataPool`
  under `…kafka010.consumer` in 4.1.2.
- **Unverified:** the release that introduced `startingOffsetsByTimestamp` /
  `startingOffsetsByTimestampStrategy`. Present in 4.1.2.

---

## Prove you got it (Part 3)

6. A task reads 250 records; one fetch returns 900. Where do the other 650 go, for
   how long, which config governs it?
7. Why key the consumer cache on `(groupId, topicPartition)` and not just
   `topicPartition`? When does groupId matter?
8. Executors run 100 concurrent partition-tasks. What happens at the 65th, and
   what does the cache do from then on?
9. You set `minPartitions` (Part 2) and lost `preferredLoc`. Trace that to a
   concrete effect on **both** pools.
10. An executor holds 400 MB of Kafka records with no task running, nothing
    leaking. Explain, and name the two configs that bound it.
11. `startingTimestamp` set to 09:00. One partition received nothing since 08:00.
    What happens by default, and how do you change it?

<details>
<summary>Answers</summary>

6. Into the fetched-data pool, keyed `(groupId, tp)` at the next offset, up to
   `spark.kafka.consumer.fetchedData.cache.timeout` (5m) idle. The next batch's
   task on that partition reuses them with no poll.
7. A consumer is positioned on a specific partition; one on `p3` can't serve `p7`.
   groupId matters because two queries (or driver vs executor readers) use
   different generated group ids and must not share a pooled consumer — same
   partition, different client identity/config.
8. At the 65th the soft cap (64) is exceeded — the pool creates anyway rather than
   fail. From then the cache is effectively off: no idle instance is ever free for
   a new key, so tasks create-and-discard, paying full setup every batch.
9. Both pools are keyed `(groupId, tp)` and hit only when a partition returns to
   the executor holding its warm state. `preferredLoc=None` lets the scheduler
   place the task anywhere, so consumer-pool and fetched-data-pool lookups miss:
   cold consumer creation **and** a cold fetch, per task, for the whole batch.
10. Fetched-data pool: buffers from many partitions, each up to the fetch-byte size,
    retained 5 min after last use, with **no capacity cap**. Bounded only by
    `fetchedData.cache.timeout` and the evictor interval. Not a leak — eviction is
    time-based, not size-based.
11. Default `StrategyOnNoMatchStartingOffset.ERROR` → the query throws, because that
    partition has no offset ≥ 09:00. Set
    `startingOffsetsByTimestampStrategy=latest` to start it at its current end
    instead.

</details>

---

## Sources

Spark source fetched firsthand at tag **`v4.1.2`**.

1. `connector/kafka-0-10-sql/.../consumer/KafkaDataConsumer.scala` — `CacheKey`;
   `get`/`fetchData` (start-only fetch); retry `invalidateKey`; the release INFO
   line.
2. `…/consumer/InternalKafkaConsumerPool.scala` — soft-capacity behaviour,
   commons-pool2, JMX flag; `CONSUMER_CACHE_{CAPACITY,TIMEOUT}` defaults.
3. `…/consumer/FetchedDataPool.scala` — unbounded HashMap, time-only eviction.
4. `…/KafkaSourceProvider.scala` — `kafkaParamsForExecutors` forced settings;
   `group.id` / `auto.offset.reset` guards; the five offset-limit cases and
   streaming/batch validation; timestamp no-match strategy.
5. `connector/kafka-0-10-sql/.../package.scala` — pool config keys/defaults.
6. Executor INFO log, this repo.

---

[Tier 3 index](./README.md) ·
[← Part 2](01-kafka-source-part2.md) ·
[Concept 1 planning exercise →](01-kafka-source-planning.md)