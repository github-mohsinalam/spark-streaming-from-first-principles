# Arbitrary Stateful Processing (legacy) — Part 2: Timeouts

> **Tier 2 · Concept 8 · Part 2 of 2**
> Part 1 gave arbitrary state (A) and hit a ceiling: the function is invoked only
> for groups present in the batch, so an outage was detectable only *after* it
> ended, and a device that never returned was never detected at all. Part 2 is
> (B) — being invoked when nothing arrives.

---

## Deriving the mechanism

### Why not invoke every key every batch?

The obvious fix: each trigger, walk the state store and call the function for
every key. Part 1's own trace shows why not — `total=3, upd=1` in b3–b6 wasn't a
limitation, it was the engine *avoiding work*. With a million devices and
heartbeats from a hundred, this design does a million deserializations and a
million user-function calls per trigger to find that 999,900 have nothing to do.

Cost would scale with **state size**, not data volume — and the output is
discarded almost always, since alerts are outliers. Wrong trade.

### Registration

If the engine can't scan everything, the key must declare its own interest:

> "If nothing more arrives for me, call me again at time **T**."

The engine then range-scans a timestamp instead of scanning state. Three
consequences:

1. **Per key** (each device goes silent at a different time), while *whether*
   timeouts exist at all is operator-level — the engine must know at plan time
   whether to maintain the index.
2. **A fresh invocation, not a callback** — the engine re-enters the same
   function with the same state handle. There is nothing else to call.
3. **The iterator is empty**, by construction.

Point 3 is ambiguous: "your timer fired" and "here's an empty batch" look
identical. So the state handle needs a flag. **Two doors into one function**, and
the correct behaviour differs per door — the asymmetry from Part 1's Step 0:
`DOWN → ACTIVE` is data-driven, `ACTIVE → DOWN` is time-driven.

### Which clock

- **Processing time** — wall clock. No watermark needed; not reproducible on
  replay (same defect as processing-time *windows*, C3).
- **Event time** — the watermark. Reproducible, requires `withWatermark`.

Our requirement is stated in event time, so event time it is.

### The guarantee you can't have

The engine only runs during a **trigger**, and triggers are driven by data. A
registration is a request to be woken at the next opportunity *after* `T`, not an
alarm clock.

> **Never before `T`; no upper bound after.** [1]

A one-sided bound, same shape as the watermark promise.

**The trap worth internalising:** a **no-data batch cannot advance the
watermark.** The watermark is `max(event time seen) − threshold`; zero rows
contribute no event times. No-data batches only *apply* an already-computed
watermark (C4's inherited timing). So:

| situation | event-time timeout |
|---|---|
| stream silent entirely | **never fires** — watermark frozen |
| data arriving, event times too low | waits; fires once wm passes `T` |
| wm past `T`, then stream quiets | fires at next batch — may need no-data batches |

Operationally: **the device that goes down is often the one whose absence stops
the clock.** If it is your only source, its own outage is undetectable. Event-time
timeouts are only as live as your slowest-but-still-alive source — and with
multiple streams the global watermark is the `min` (C5), so one stalled stream
freezes everything.

In the demo, D2 does this work.

---

## The API

```scala
GroupStateTimeout.NoTimeout | ProcessingTimeTimeout | EventTimeTimeout
```

```scala
// ProcessingTimeTimeout only — RELATIVE
state.setTimeoutDuration(90000)          // ms
state.setTimeoutDuration("90 seconds")

// EventTimeTimeout only — ABSOLUTE, epoch ms
state.setTimeoutTimestamp(180000)
state.setTimeoutTimestamp(180000, "1 hour")

state.hasTimedOut                    // woken by the timer?
state.getCurrentWatermarkMs()        // 0 in the first micro-batch [1]
state.getCurrentProcessingTimeMs()
```

Calling the wrong setter for your `timeoutConf` throws
`UnsupportedOperationException`; `setTimeoutTimestamp` below the current
watermark throws `IllegalArgumentException`. [1]

**Why relative vs absolute.** The watermark is a *position on the data's
timeline*, not a clock that runs — there is no meaningful "now" to offset from,
so you name the instant you care about (`lastSeen + 90`). Processing time always
advances, so an absolute wall-clock instant is expressible but a footgun: on
replay it is long past, and everything fires immediately on batch 0. The relative
form preserves "90 seconds after I last saw this key". **The API shape follows
the clock's nature.**

### The reset rule

> The timeout is reset every time the function is called. [1]

Every invocation, through **either** door, clears the registration. Re-arm or the
key becomes invisible to the timer — back to Part 1 behaviour for that key. This
is also why a healthy device never fires: each heartbeat pushes its own timeout
further out.

### Per-batch ordering

```scala
processNewData(filteredIter) ++ processTimedOutState()
```

> …the filtering for timeout occurs only after all the data has been processed.
> This is to ensure that the timeout information of all the keys with data is
> updated before they are processed for timeouts. [2]

Per trigger: **(1)** event-time admission filter drops input below the watermark ·
**(2)** `processNewData` — keys with data; state updated, timeout re-armed ·
**(3)** `processTimedOutState` — keys due *and* without data this batch, invoked
with an empty iterator and `hasTimedOut = true`.

**Derived, then confirmed (D4 below): a key with data in the batch re-arms in
step 2 before step 3 runs, so it cannot time out in the same batch. New data wins
the tie.**

---

## `outputMode` — a plan-time declaration

`flatMapGroupsWithState` takes an `OutputMode`; `mapGroupsWithState` does not
(one row per key, revisable, so Update-only).

It is **not a runtime switch.** Databricks: the arbitrary stateful operators
emit records using their own custom logic, so the stream's output mode does not
affect their behaviour. [3] And the parameter given to the operator appears
unused during execution. [4]

It is a **promise to the analyzer** about how your function emits — `Append`:
every row is final; `Update`: rows may be superseded. The engine cannot verify
this (your function is a black box), so it takes your word and uses it for
plan-time validation: what may follow the operator, and which sink modes are
legal. A downstream stateful operator needs finalized inputs, so it is only
permitted after `Append` (C5 Part 2b, same finality requirement).

The demo declares `Append` truthfully — each alert is emitted once, never revised.

> **Unverified:** the exact legality matrix lives in `UnsupportedOperationChecker`,
> which I did not read. [4] is a secondary, Spark 2.x-era source.

---

## The two-stage timer, and `remove()`

There is **one timeout slot per key**. A device lifecycle needs two scheduled
actions:

```
silent  90s  ->  deem DOWN, alert
silent 300s  ->  deem decommissioned, remove state
```

So the slot is used **sequentially**, with a state field tracking position:

```scala
if (state.hasTimedOut) {
  val prev = state.get                       // firing deletes nothing
  if (!prev.isDown) {
    // STAGE 1 — alert, then re-arm the SAME slot for retirement
    state.update(prev.copy(isDown = true, deemedDownAt = Some(prev.lastSeen + 90)))
    armAt(state, prev.lastSeen + 300)
    Iterator(alert("DOWN", ...))
  } else {
    // STAGE 2 — still down. remove() is the ONLY way state leaves this operator.
    state.remove()
    Iterator(alert("RETIRED", ...))
  }
}
```

Both firings arrive through the same door, same empty iterator, same
`hasTimedOut = true`. **Nothing but persisted state can tell them apart** —
`isDown` is the stage discriminator. A device that recovers takes the data door,
which clears `isDown`, so stage 2 never happens for it.

`remove()` is entirely possible; what's absent is any *rule* that does it for you.
Retirement remains your policy, your schedule, your bookkeeping.

### Why `isDown` exists

Three jobs, and the third is the load-bearing one:

1. **Suppression** — would matter if you re-armed after DOWN, since re-arming
   makes a *recurring* timer and the level "past threshold" is true on every
   firing (edge vs level, Part 1).
2. **Pairing** — it records that we *emitted* a DOWN. That is a fact about our
   output, not about the data, so it is not recomputable from `lastSeen`.
   It guarantees RECOVERED never appears unpaired.
3. **Stage discrimination** — the two firings are otherwise identical.

**Trade-off made explicit:** the recovery branch emits nothing when
`!prev.isDown`, even if the gap exceeds the threshold. No unpaired RECOVERED, at
the cost of silently missing an outage the timer never caught (e.g. the watermark
jumping past both the timeout and the recovery event in one batch). The opposite
call is defensible.

### The invariant that cannot be enforced

The domain-correct state is a **sum type** — `Down` always carries a verdict,
`Active` never does — and Part 1's probe showed it does not encode: derivation
covers `Product` types only. So we flatten by hand and carry

```
deemedDownAt.isDefined  <=>  isDown
```

in discipline rather than in the type. The `getOrElse` in the recovery branch is
that cost, visible in the code.

---

## The demo

[`demos/tier2/HeartbeatEventTimeTimeoutDemo.scala`](../../src/main/scala/demos/tier2/HeartbeatEventTimeTimeoutDemo.scala) —
`EventTimeTimeout`, DOWN after 90s, RETIRE after 300s, watermark 1s,
`noDataMicroBatches.enabled=false`.

Four devices: **D1** down then returns · **D2** healthy control · **D3** down
forever · **D4** tie-break probe.

```
batch  input                    inherited wm  total upd removed   alerts
  b0   D1,D2,D3,D4 @30                0         4    4     0      (armed 120)
  b1   D1,D2,D3,D4 @60               29         4    4     0      (armed 150; last beat D1,D3)
  b2   D2@130                        59         4    1     0
  b3   D2@200                       129         4    1     0
  b4   D2@260, D4@255               199         4    4     0      D1 DOWN, D3 DOWN
  b5   D1@300, D4@300               259         4    2     0      D1 RECOVERED
  b6   D1@400, D2@400, D4@400       299         4    3     0
  b7   D2@450                       399         3    1     1      D3 RETIRED
```

```
D1 DOWN      at=150  silentFor=90
D3 DOWN      at=150  silentFor=90
D1 RECOVERED at=300  silentFor=240
D3 RETIRED   at=360  silentFor=300
```

**b4 — the headline and the tie-break.** `wm=199` puts D1, D3 *and* D4 past their
armed 150. D1 and D3 fire DOWN; **D4 does not**, because it has data this batch
and re-armed in `processNewData` first. The ordering derivation confirmed.

D3 is the device Part 1 could not detect at all — silent forever, alerted
*during* the outage.

**b7 — the second stage.** D3 fires again, but as RETIRED, not DOWN: `isDown`
routed it to stage 2. `remove()` runs, `total` drops 4→3, `removed=1`. The only
batch in either demo where state leaves the operator. b6 (`wm=299 < 360`) vs b7
(`wm=399 > 360`) brackets the firing to one batch.

### `numRowsUpdated` counts `update()` calls, not invocations

- **b4:** `in=2, upd=4` — D2, D4 via data; D1, D3 via timer. Timeout invocations
  that call `update()` **do** count, so `upd` can exceed `numInputRows`.
- **b7:** `in=1, upd=1, removed=1` — D3's stage-2 invocation ran and emitted, but
  called `remove()`, not `update()`, so it appears **only** in `numRowsRemoved`.

The two counters partition state writes by kind. **Neither counts how often your
function ran**, and there is no metric that does — timeout-driven work is partly
invisible. Matters for the Tier 4 dashboard.

> **Not established:** the firing boundary. b4 clears it by 49s, so this trace
> cannot distinguish `wm > T` from `wm >= T`. Given C4/C6/C7 each had their own
> boundary and none propagated, treat it as unverified here. Worst case is one
> batch of extra retention — an eviction-side question, low value to pin.

---

## Why `transformWithState` replaces this API

Three limitations, each derived above rather than asserted. [5]

**1. Monolithic `S`** *(Part 1)* — one slot per key, read-modify-write of the
whole object, no partial reads. The cost of updating your cheapest field is set
by the size of your largest. And the slot's type system is narrower than Scala's:
sum types don't encode, so domain models get flattened by hand.
→ **composite `ValueState` / `ListState` / `MapState`**, independently serialized.

**2. One coarse timer** *(this part)* — a multi-stage lifecycle must be
multiplexed through a single slot, with state fields tracking position and every
transition remembering to re-arm or the key silently drops out forever. Any TTL
is code you write.
→ **multiple named timers per key + declarative per-value TTL**: "expire after N"
becomes a property you set, not a stage you implement.

**3. No lifecycle** — a bare function. No init hook (connections, lookup tables,
config must be built per-invocation or captured in the closure and serialized to
every executor), no close hook, no identity. Configuration is baked into the
closure, so there's no object to construct with a 5-second threshold for a fast
test and 90 for production. Weakest alone; matters cumulatively — and note it is
the one thing a trace *cannot* show, visible only in what you can't write.
→ **an object-oriented `StatefulProcessor`** with explicit lifecycle methods and
state as fields.

The old→new migration is itself senior-interview material.

---

## Testing without a query

`TestGroupState.create[S](optionalState, timeoutConf, batchProcessingTimeMs,
eventTimeWatermarkMs, hasTimedOut)` builds a `GroupState` behaving as the engine's
would, so the update function can be unit-tested deterministically — assert final
state and final timeout timestamp, no running query. [6] That checks the *logic*;
the MemoryStream demo checks the *engine wiring*. (Tier 5.)

---

## Spark 3.x → 4.x

Timeout semantics are unchanged. `transformWithState` (4.0+) is the replacement
and postdates the course. Internals moved in 4.x to
`…execution.streaming.operators.stateful.flatmapgroupswithstate` — confirmed
firsthand from `SparkStrategies.scala` and `FlatMapGroupsWithStateSuite.scala`
imports; the pre-4.x path 404s.

---

## Prove you got it

1. A device heartbeats at `t=100`; you arm an event-time timeout at 190. At
   `t=150` another heartbeat arrives. Why doesn't it go DOWN at 190?
2. Why is processing-time registration *relative* and event-time *absolute*?
3. Your only source dies. Does its event-time timeout fire? Does enabling
   `noDataMicroBatches` change the answer?
4. You re-arm at `deemedDownAt + 90` after firing DOWN. What fires, and how many
   times, during a 300-second outage?
5. In b7, `upd=1` and `removed=1` with one input row — but the function ran twice.
   Explain, and say what this means for monitoring.
6. Both timer firings for D3 arrive with an empty iterator and
   `hasTimedOut = true`. What distinguishes them, and why can't it be derived?

<details>
<summary>Answers</summary>

1. The heartbeat invokes the function through the data door, and **the timeout is
   reset on every invocation** — re-armed to `150 + 90 = 240`. Nothing is
   registered at 190 any more. Generally: a continuously-beating device pushes its
   own timeout perpetually forward.
2. The watermark is a position on the data's timeline with no running "now" to
   offset from, so you name the instant (`lastSeen + threshold`). Wall clock
   always advances, so absolute is expressible but meaningless on replay —
   everything fires at once on batch 0. The API shape follows the clock's nature.
3. **No.** The watermark is `max(event time) − threshold`; with no data it is
   frozen, so nothing ever crosses `T`. `noDataMicroBatches` does **not** help — a
   zero-row batch contributes no event times and cannot advance the watermark; it
   only applies an already-computed one.
4. **Repeatedly** — roughly every 90s of watermark advance: fires at 90, re-arms
   180; fires at 180, re-arms 270; and so on. Re-arming creates a *recurring*
   timer, so you'd need `isDown` to suppress repeat alerts. Declining to re-arm
   makes once-per-outage structural instead.
5. D2's data invocation called `update()` (→ `upd`); D3's stage-2 invocation
   called `remove()` (→ `removed`). `numRowsUpdated` counts `update()` calls, not
   invocations, and the two counters partition state writes by kind. **No metric
   counts invocations**, so timeout-driven work is partly invisible on a dashboard.
6. Only `isDown` — persisted state. It cannot be derived because both firings see
   identical inputs; the difference is *what we already did*, which is a fact
   about our output, not about the data.

</details>

---

## Sources

1. `GroupState.scala` trait (fetched firsthand) — timeout modes and setters;
   "never before / no strict upper bound"; reset rule; `hasTimedOut`;
   `getCurrentWatermarkMs()` = 0 on first batch; exception conditions.
2. `FlatMapGroupsWithStateExec` — `processNewData ++ processTimedOutState` and the
   ordering comment. *Corroborated verbatim by two independent sources; the file
   itself not fetched (moved package in 4.x).*
3. Databricks, "Select an output mode for Structured Streaming" — the arbitrary
   stateful operators emit by their own logic; stream output mode does not affect
   them.
4. Laskowski, *Mastering Spark Structured Streaming* — the `OutputMode` given to
   `FlatMapGroupsWithStateExec` appears unused in execution. *Secondary,
   Spark 2.x-era; not confirmed against 4.x.*
5. Structured Streaming Programming Guide, Spark 4.1.2 — `transformWithState` as
   the next-generation replacement; composite state, TTL, timers.
6. `TestGroupState` (Spark testing utility) + Databricks "Legacy arbitrary
   stateful operators".
7. Demo: `demos/tier2/HeartbeatEventTimeTimeoutDemo.scala` — trace reproduced above.

---

[← Part 1: Arbitrary State](./07-arbitrary-stateful-legacy-part1.md) · [Next: Concept 9 — `transformWithState` →](./09-transform-with-state-part1.md)