# Event-Time Windows

> **Tier 2 · Concept 2 of 9**
> Grouping by *when* instead of *who*. Everything from streaming aggregations
> applies; the key space gains a time dimension, and that changes the cost model.

---

## Anchoring to Concept 1

Concept 1 established the streaming aggregation execution model: a `foldLeft`
running one micro-batch at a time, with accumulated state persisted in the state
store between triggers. The example — `groupBy("sensorId")` — grouped by an
identity key. One bucket per sensor, accumulating indefinitely.

Event-time windows apply the same model with a different grouping key: a
**time interval** derived from a timestamp embedded in the event itself. The fold,
the state store, the output mode rules, and the unbounded-state hazard are all
unchanged. What changes is the shape of the key and the semantics of bucket
boundaries.

---

## The two clocks

Two timestamps are always present in a streaming system:

**Event time:** the timestamp recorded in the data by the producing system — when
the event actually occurred. A sensor reading at 09:00:00 has event time 09:00:00
regardless of when Spark processes it.

**Processing time:** the wall-clock time at which Spark processes the record. If
that reading is delayed in transit and arrives at Spark at 09:00:45, the
processing time is 09:00:45 even though the event time is 09:00:00.

These clocks disagree in any real system. Network delays, producer backpressure,
Kafka partition lag, and cold-start bursts all cause events to arrive later than
their timestamps. The gap is called **event skew** or **late data**.

Event-time windows use event time. This is the correct choice for almost all
business questions: "how many sensor readings occurred between 09:00 and 09:05?"
should be answered using the time the readings *occurred*, not when Spark *saw*
them. Processing-time windows are Concept 3 — and part of that lesson is
understanding when processing time is the *wrong* choice.

---

## What a window is

A window is a **contiguous time interval used as a grouping key**. Where a plain
aggregation produces one bucket per group key:

```
groupBy("sensorId")  →  one bucket per sensor, accumulates forever
```

a windowed aggregation produces one bucket per *(time interval, group key)* pair:

```
groupBy(window("eventTime", "5 minutes"), "sensorId")
  →  one bucket per (5-minute interval, sensor)
```

The Spark SQL `window` function, given a timestamp column and a duration, produces
a struct `{ start: Timestamp, end: Timestamp }` representing the interval that
contains that timestamp. The engine uses this struct as part of the composite group
key.

An event with `sensorId = "A"` and `eventTime = 09:03:17` maps to the bucket
`([09:00, 09:05), A)`. The same sensor at `09:07:44` maps to `([09:05, 09:10), A)`.
Different intervals, different state store entries.

---

## Tumbling windows

A tumbling window is a sequence of **non-overlapping, fixed-size intervals** that
tile the time axis without gaps:

```
|-- 09:00–09:05 --|-- 09:05–09:10 --|-- 09:10–09:15 --|  ...
```

Every point in time belongs to exactly one window. Every event maps to exactly one
state store bucket. The API takes two arguments — the timestamp column and the
window size:

```scala
import org.apache.spark.sql.functions.{window, sum, col}

streamingDF
  .groupBy(
    window(col("eventTime"), "5 minutes"),  // two args = tumbling
    col("sensorId")
  )
  .agg(sum("reading").as("totalReading"))
```

**State store shape:** one entry per `(windowInterval, sensorId)`. An event for
sensor A at 09:03 updates `([09:00, 09:05), A)`. An event for sensor A at 09:07
updates a separate entry, `([09:05, 09:10), A)`. All open windows are held
simultaneously.

---

## Sliding windows

A sliding window has a fixed size *and* a fixed slide interval, where the slide is
smaller than the size. This causes windows to **overlap** — a single event can
belong to multiple windows simultaneously.

```
size = 10 min, slide = 5 min:

|---- 09:00–09:10 ----|
          |---- 09:05–09:15 ----|
                    |---- 09:10–09:20 ----|
```

An event at 09:07 falls in both `[09:00, 09:10)` and `[09:05, 09:15)` — it is
counted in both aggregates. The API adds the slide as a third argument:

```scala
streamingDF
  .groupBy(
    window(col("eventTime"), "10 minutes", "5 minutes"),  // size, then slide
    col("sensorId")
  )
  .agg(sum("reading").as("totalReading"))
```

**Write amplification:** each event updates one state store entry per window it
falls into. The maximum number of windows containing any single event is
`ceil(windowSize / slideInterval)`. For size=10min, slide=5min: `ceil(10/5) = 2`.
For size=60min, slide=1min: 60 entries updated per event.

**Why the formula works:** consider the window `[09:00, 09:15)` with size=15min,
slide=5min. Inside this window there are slide boundaries at 09:05 and 09:10, each
the start of a different overlapping window. An event anywhere in `[09:00, 09:15)`
can also fall in the window starting at 09:05 and the one starting at 09:10 — up
to `ceil(15/5) = 3` windows total. The formula counts the number of slide
boundaries that fit inside one window, which equals the maximum overlap.

**Important:** `ceil(size/slide)` is the **maximum**, not a guaranteed count. An
event near the early edge of the latest possible window may fall in fewer. With
size=15min, slide=5min: an event at `09:08:22` falls in `[09:00, 09:15)` and
`[09:05, 09:20)`, but *not* `[09:10, 09:25)` — that window requires
`window.start ≤ eventTime`, and `09:10 > 09:08:22`. Two entries updated, not
three. An event at `09:12:00` would hit all three.

---

## The window struct in output

`window(...)` in a `groupBy` produces a struct column, not a scalar:

```
window: struct
  start: timestamp
  end:   timestamp
```

Reference its fields downstream with dot notation:

```scala
.select(
  col("window.start"),
  col("window.end"),
  col("sensorId"),
  col("totalReading")
)
```

This matters for sink schemas and for `MERGE` conditions — you join on
`window.start` and `window.end` (or a derived window ID) when upserting into Delta.

---

## The cost model: time-driven state growth

Without watermarks, **window state is never evicted.** Past windows accumulate in
the state store indefinitely. This is worse than an unwatermarked plain aggregation
in a concrete way: state grows along **two independent axes**.

For a plain `groupBy("customerId")`, state is bounded by the number of distinct
customers ever seen. New customers create new entries; elapsed time alone creates
nothing new.

For a `groupBy(window(..., "1 hour"), "customerId")` with no watermark, state grows
with **both** distinct customers **and** elapsed time — even if zero new customers
arrive, a new hour-window bucket opens every hour. After 90 days with 100K customers:

```
24 hours/day × 90 days  =  2,160 window slots
2,160 slots × up to 100,000 customer keys per slot  =  up to 216,000,000 state entries
```

Both executor heap and checkpoint storage grow at this rate. The time-driven axis
is what makes windowed aggregations without watermarks acutely more dangerous than
plain key aggregations.

Watermarks close this down: when the watermark advances past a window's end time,
the engine evicts that window's state. Concept 4.

---

## Late data and why windows naturally handle it (at a cost)

Two events for sensor A, both with event times in `[09:00, 09:05)`:

- Event 1: `eventTime = 09:03:00`, arrives at Spark on time
- Event 2: `eventTime = 09:03:45`, arrives 7 minutes late (processing time 09:11)

Without watermarks, the engine keeps `([09:00, 09:05), A)` open in the state store
and correctly includes Event 2 when it finally arrives. Correctness is preserved —
at the cost of keeping this window (and every other past window) open forever.

With watermarks, the engine closes windows after a configured delay and drops events
that arrive after the cutoff. That is an explicit tradeoff: bounded state at the
cost of potentially dropping very late events. Concept 4.

---

## Spark 3.x → 4.x note

No gap. The `window` function, tumbling and sliding window semantics, and state
store behavior for windowed aggregations are unchanged between Spark 3.x and 4.x.
Two-argument `window` = tumbling; three-argument = sliding. Stable across versions.

---

## Prove you got it

1. **Multiplicity.** A sliding window is configured with `size = 15 minutes,
   slide = 5 minutes`. An event arrives with `eventTime = 09:08:22`. How many
   state store entries does this event update, and what are the window intervals?
   Show the boundary arithmetic, not just the formula result.

2. **The cost question.** A tumbling 1-hour window aggregation groups by
   `(window, customerId)` with no watermark. It has been running 90 days with
   100K distinct customers. Describe what has accumulated in the state store and
   why this is concretely worse than an unwatermarked `groupBy("customerId")`
   from Concept 1.

<details>
<summary>Answers</summary>

1. With size=15min, slide=5min, the candidate windows are `[09:00, 09:15)`,
   `[09:05, 09:20)`, and `[09:10, 09:25)`. The third requires
   `window.start ≤ eventTime`, but `09:10 > 09:08:22`, so it does not contain
   this event. Two entries are updated: `[09:00, 09:15)` and `[09:05, 09:20)`.
   The formula `ceil(15/5) = 3` is the maximum; this event falls near the early
   edge of the third candidate window and misses it.

2. `24 × 90 = 2,160` window slots have opened. Each holds up to 100K customer
   entries: up to 216 million state store entries total, all retained with no
   eviction. A plain `groupBy("customerId")` produces at most 100K entries
   regardless of runtime — elapsed time creates no new keys. The windowed version
   adds a time-driven growth axis: new buckets open every hour independent of
   data arrival, compounding the key-cardinality axis across both executor heap
   and checkpoint storage.

</details>

---

[← Concept 1: Streaming Aggregations](./01-streaming-aggregations.md) · [Next: Processing-Time Windows →](./03-processing-time-windows.md)