# `transformWithState`

> **Tier 2 · Concept 9 of 9** — closes the tier.
> The Spark 4.0 replacement for `mapGroupsWithState` / `flatMapGroupsWithState`.
> C8 ended with three structural limits; this is the API built to remove them.
> Everything below is either derived, measured, or read off the Spark source.

---

## What the V2 API changes

Three things, and the rest of this note is their consequences.

**1 — An object, not a function.** C8 gave you one function with a fixed signature,
so every concern had to be crammed into it: new data and expired timers arrived
through the same door and were separated by a boolean. You could not add a hook, a
constructor, or a second entry point. Here you extend a class and override methods,
so the structure is yours: two doors instead of one flag, configuration as
constructor parameters, and a place for setup.

**2 — Composite state.** State is no longer a single slot. It is a set of
independently named pieces, each written and removed on its own — so **the cost of
writing your cheapest field is no longer set by the size of your largest.** Each
piece is one of three types: `ValueState[T]`, `ListState[T]`, `MapState[K, V]`.

**3 — Timers are a list.** Not one slot holding one deadline, but a collection you
add to, remove from, and enumerate. Genuinely valuable when deadlines are
**concurrent**; convenient even when they are merely sequential. Two costs: they do
**not** reset on invocation, so deleting stale ones is your job — and a fired timer
removes only itself from the list, never your state.

---

## Where C8 breaks

C8's processor satisfied all four heartbeat requirements. So the motivation here is
not "a nicer API" — it is requirements that working code cannot absorb. Extend it
the way an ops team would:

5. Keep the **last 100 readings** per device.
6. When a device goes DOWN, **ship those readings with the alert** (diagnostics).
7. If still down after **180s, ESCALATE**; **retire** the device entirely after
   **300s**.

State is one slot, so the readings go in it:

```scala
case class DeviceState(lastSeen: Int, isDown: Boolean, deemedDownAt: Option[Int],
                       recentReadings: List[Reading])   // newly added
```

Three things break.

**1 — every heartbeat rewrites the readings.** The data branch already does
`state.update(prev.copy(lastSeen = maxT))`. That `copy` changes 4 bytes; `update`
writes the **whole object**, all 100 readings with it. Nothing in the code looks
wasteful — the waste is that the slot is atomic. **You cannot write part of the
state.**

**2 — removal is all-or-nothing.** The only removal is `remove()`, which deletes
*everything for the key*. There is no way to drop the readings buffer while keeping
the status and the verdict; that granularity does not exist. Any finer policy is
hand-written pruning inside the update function, on state the engine cannot help
with because it does not know what any field of `S` means.

**3 — one timer slot, so schedules must be discovered rather than declared.**
Requirement 7 adds a second follow-up: escalate at 180s, retire at 300s.

Both are *reachable* with one slot, because the deadlines are **ordered** — retire
cannot fire before escalate, so you arm escalate and, when it fires, arm retire.
That is C8's two-stage trick with a third stage. What it costs:

- **Every stage transition must remember to re-arm.** Three stages, three chances to
  drop the key out of the mechanism permanently.
- **The schedule exists only as code.** At any moment one deadline is registered;
  the others live in the branch that will eventually compute them. Nothing can be
  inspected.
- **A state field must encode position** — which is why C8's `isDown` was doing
  stage-discrimination on top of its other jobs.

With a timer *list*, all follow-ups are registered **at the moment the device goes
down**: the whole schedule declared where the decision is made, rather than
assembled stage by stage. **For these requirements that is convenience, not
capability** — the honest statement. It becomes capability with genuinely
*unordered* deadlines, where one slot can hold only whichever is nearer.

### What's needed

1. **A structure you control**, rather than one function with a fixed signature
   carrying every concern.
2. **State written and removed in parts** — updating or clearing one piece must not
   touch the others.
3. **A collection of pending schedules**, not a slot.

---

## Who decides the state schema

Worth naming before the mechanism, because it explains C8's missing eviction.

| operator | schema comes from | decided by |
|---|---|---|
| `groupBy.agg` | **the query** — grouping cols → key, agg exprs → value | Catalyst, from the plan |
| `flatMapGroupsWithState` | **a type** — `Encoder[S]` | you, at compile time |
| `transformWithState` | **imperative registration** — `init` | you, at runtime |

A steady handover of authority, and management responsibility moves with it.
`groupBy(deviceId).agg(count("*"))` fully determines `key → count`, so the engine
*knows what the state means* and can derive eviction from the watermark. `S` is a
black box, so nothing is derivable and everything is yours (C8). Here the shape is
not even a type — it is a sequence of registration calls.

> **The engine can only manage state it understands.** TTL is the concession: you
> cannot tell the engine what your state *means*, but you can tell it how long a
> piece should *live* — enough for it to act without understanding.

---

## The processor

```scala
class DowntimeDetector(downAfter: Int, escalateAfter: Int, retireAfter: Int,
                       bufferSize: Int)
  extends StatefulProcessor[String, TimedBeat, DeviceAlert] {

  @transient private var _lastSeen: ValueState[Long] = _
  // ... more pieces

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = ...
  override def handleInputRows(key, inputRows, timerValues): Iterator[DeviceAlert] = ...
  override def handleExpiredTimer(key, timerValues, expiredTimerInfo): Iterator[DeviceAlert] = ...
  override def close(): Unit = ...
}
```

`StatefulProcessor[K, I, O]` — key, input, output. Same three types as C8's
`K`/`V`/`U`, now on a class. Call site:

```scala
ds.groupByKey(_.deviceId)
  .transformWithState(new DowntimeDetector(...), TimeMode.EventTime(), OutputMode.Append())
```

| method | when | job |
|---|---|---|
| `init` | before any key is processed | **register the state schema** |
| `handleInputRows` | once per key **with rows in this batch** | the data door |
| `handleExpiredTimer` | once per **expired timer** | the time door |
| `close` | after the batch's state store commit | teardown |
| `handleInitialState` | batch 0 only, per initial-state row | pre-populate state |

Two entry points replace `hasTimedOut`. Beyond tidiness, they carry **different
parameters**: the timer path receives which timer fired, the data path receives
rows. One signature could not carry both without unused arguments.

Two constraints the engine enforces, both structural: **timers cannot be registered
in `init`** (no key context there — a timer belongs to a key), and **input rows
cannot be read in `handleExpiredTimer`** (by definition there are none).

Step-by-step detail for each method — including the load-bearing ordering inside
`handleInputRows` — lives in the [demo's](../../src/main/scala/demos/tier2/HeartbeatTransformWithStateDemo.scala) doc string, next to the code it describes.

> **Scala pause — `@transient private var _lastSeen: ValueState[Long] = _`**
>
> The field's type is `ValueState[Long]`, a **reference type**, so `= _` (Scala 2's
> default-value initializer) gives **`null`**, not `0L`. The `[Long]` is what the
> state *holds*, not what the field is.
>
> The processor is constructed on the driver and **serialized to executors**, but a
> `ValueState` handle is bound to a live state store that exists only on the
> executor. `@transient` excludes the field from serialization; `init` populates it
> on the executor. So the field is `null` at both ends of the wire and non-null only
> after `init` has run.

---

## Registration is not value (init method in the processor)

The distinction that makes the rest of the API readable.

- **Registration** — declared in `init`, **key-independent**. It creates the
  *variable*: a name, an encoder, a TTL config, a place in the store. It exists
  whether or not any key has data.
- **Value** — per key, per variable. Created by `update`/`appendValue`/`updateValue`,
  destroyed by `clear()`.

The store is keyed by **`(groupingKey, variableName)`**. Registration establishes
the second coordinate; data establishes the first.

`getValueState` is documented as *"create new or return existing"* — a
register-or-**lookup** by name. What comes back is a **thin handle** holding
`(store, name, encoder, ttlConfig)` and no data; the engine supplies the current
grouping key around each invocation, which is why one handle serves every key in the
partition.

Three consequences:

- `_deemedDownAt.exists()` is a lookup at that pair. It returns `false` on a
  device's first heartbeat **because the variable is registered but the pair has no
  entry** — not because anything is missing.
- `clear()` removes *that pair's* entry, leaving the other pieces untouched; a later
  `update` recreates it. The variable itself lives for the whole query.
- **There is no key-level delete.** "Retire this device" means clearing every piece
  by hand. Miss one and the orphan resurfaces as `exists() == true` when the device
  returns.

Compare `GroupState`: one key, one value, and `remove()` meant *all state for this
key*. Nothing finer was expressible. Here "does it exist?" is a question you ask
**per piece** — which is what makes `exists()` usable as domain logic rather than a
null check. `_deemedDownAt.exists()` *means* "this device is down", and that is the
honest encoding because the verdict genuinely does not exist for a healthy device.

---

## The three possible pieces

Not a menu — three **access patterns**, each with a different physical layout.

### `ValueState[T]`

One value per key. `exists` · `get` · `update` · `clear`. Read and written whole, so
it is right for scalars and small records and wrong for anything that grows.

### `ListState[T]`

Append-oriented. `get` (iterator) · `appendValue` · `appendList` · `put` · `clear`.

```scala
store.createColFamilyIfAbsent(stateName, ..., useMultipleValuesPerKey = true)
store.merge(encodedKey, stateTypesEncoder.encodeValue(newState), stateName)
```

An append is a RocksDB **merge operand**, so a list of N elements is **one physical
key** and appending genuinely does not rewrite the rest. `get()` returns elements,
**oldest first**.

Two consequences worth knowing:

- **Nothing bounds it.** Trimming to the last N means read-all then `put`-all — a
  full rewrite, so a bounded buffer built this way is *worse* than a
  `ValueState[List]`.
- The store cannot report a list's length without scanning, so Spark keeps a
  **hidden entry-counter column family** (`ListStateMetricsImpl`, `isInternal = true`)
  purely for metric accuracy — *"to avoid scanning the entire list to get the
  count."* Every `appendValue` is therefore three store operations, not one.

### `MapState[K, V]`

Keyed lookups. `getValue` · `containsKey` · `updateValue` · `iterator` · `keys` ·
`values` · `removeKey` · `clear`. `updateValue` is a **point write** — it touches one
entry and does not read or rewrite the others. Needs encoders for both `K` and `V`.

### Choosing one — the bounded-buffer case

Requirement 5 ("last N readings") makes the trade-off concrete:

| approach | write cost | bounded? | rewrite |
|---|---|---|---|
| `ListState`, append-only | O(1) | **no** — leaks | none |
| `ListState` + `put` trim | O(n) | yes | **full** |
| `ValueState[List]`, prepend + `take(N)` | O(N) | yes | **full, capped at N** |
| **`MapState` ring buffer** | **O(1)** | **yes, structurally** | **none** |

The ring buffer keys the map by **slot number** — the nth reading ever appended goes
to slot `n % N`, so once full each write overwrites the oldest and the map never
exceeds N. No trimming code exists, because nothing ever needs trimming. Cost moves
to the read (iterate and sort N entries), which happens only when an alert fires.

> Honest trade-off: the slot key carries no meaning, so *"readings after time T"* is
> not answerable without a scan. Right structure for "last N", wrong one for
> time-range queries.

---

## TTL — a lifetime on a piece

Need (2) said removal should be per piece. `clear()` gives that imperatively — each
variable cleared on its own. TTL gives the same granularity **declaratively**: the
third argument to every `getXState` call in `init` method attaches a lifetime to one variable.

```scala
_status   = getHandle.getValueState[String]("status", enc, TTLConfig.NONE)
_readings = getHandle.getListState[Reading]("readings", enc, TTLConfig(Duration.ofHours(1)))
```

A **duration**, declared with the variable — not a deadline you compute. That shape
matters: a deadline requires code to run in order to be computed and refreshed, while
a duration is a standing instruction the engine acts on alone. Different variables on
the same key can carry different durations, or none.

**Mechanically**, every write computes the expiry and stores it in a secondary index:

```scala
val ttlExpirationMs = StateTTL.calculateExpirationTimeForDuration(
  ttlConfig.ttlDuration, batchTimestampMs)
```

Three consequences, all verified:

- **Processing time, always.** `batchTimestampMs` is the micro-batch's wall-clock
  timestamp, fixed at planning. The `TTLConfig` docstring says it outright: *"Any
  state update resets the ttl to current processing time plus ttlDuration."*
- **Reset on write.** A `ValueState` written every batch never expires. TTL removes
  state that stops being written, not state that gets old.
- **Per element on `ListState`.** Each element was appended in a different batch, so
  each carries its own expiry. Measured: with a 5s TTL, elements appended 7s earlier
  were gone while one appended 4s earlier survived. A sliding window, not an
  all-at-once drop.

`TTLConfig.NONE` (or a zero duration) disables it. TTL is scoped to the **variable as
declared** — a `NONE` variable on the same key is untouched while a TTL'd one expires.

There is **no TTL callback.** Expiry never invokes your processor; it surfaces only
as `numValuesRemovedDueToTTLExpiry`. You cannot react to it, only observe that it
happened.

### Caveat 1 — a convenience, not a new capability

Expiry *is* achievable without TTL: register a timer, and on firing call `clear()`.
The demo's RETIRED branch does exactly that, and it reaches keys that have gone
silent — timers are the mechanism for that (C8's whole point).

What TTL buys is being **declarative rather than imperative**: no timer to register,
no deadline to recompute, no re-arm to forget. And per-element expiry on lists, which
by hand would mean storing a timestamp with every element and scanning to prune. Real
savings — but a convenience, not something otherwise unreachable.

### Caveat 2 — TTL cannot be used with event time

```
STATEFUL_PROCESSOR_INCORRECT_TIME_MODE_TO_ASSIGN_TTL
Cannot use TTL for state=readings in timeMode=EventTime,
use TimeMode.ProcessingTime() instead.
```

Thrown by `StatefulProcessorHandleImpl.validateTTLConfig`, **per state variable, at
declaration time**. `TimeMode` picks one clock for the whole processor, and TTL exists
only on the processing-time side — its expiry is `batchTimestampMs + duration`, and
there is no way to express that against a watermark.

**Make it concrete.** Requirement 5 was "keep the last 100 readings" — a **count**
bound, which the ring buffer satisfies structurally with no TTL at all. Now suppose
ops asks instead for *"the readings from the last hour"* — an **age** bound. No state
design expresses that: a ring buffer bounds how many, never how old. You need a
lifetime, and a lifetime needs TTL.

But requirements 1–3 are stated in event time, and must be — C3 established that
processing time is not reproducible on replay, so it is the wrong basis for anything
whose answer must be stable. So the chain closes:

1. Business logic requires **event time**.
2. TTL requires **processing time**.
3. Therefore **TTL is unavailable to exactly the processors that carry business
   logic.**

A pipeline needing both must be **split into two operators** — event-time detection
chained with a processing-time buffer owner. Within one processor the choice is
forced: an event-time processor prunes by hand, with timers and `clear()`.

> This is the sharpest limitation in the API. TTL is among its best features, and it
> is unusable in the mode that correctness demands.

---

## Timers

```scala
getHandle.registerTimer(expiryTimestampMs)
getHandle.deleteTimer(expiryTimestampMs)
getHandle.listTimers()
```

Scoped to the **grouping key**, not to a state variable — no method mentions one.
Which state a timer concerns is entirely your interpretation. Registered anywhere
except `init`, including inside `handleExpiredTimer`, which is how recurrence is
built. Checkpointed, so they survive restart.

**The timestamp is the identity.** `deleteTimer` takes it; `expiredTimerInfo` reports
it. That is what retires C8's stage-discriminator field.

**`getExpiryTimeInMs()` returns the timestamp the timer was registered for**, read
straight out of the timer store — *not* the moment of firing. The watermark may have
overshot it arbitrarily (in the demo: registered 270, inherited watermark 299). Using
the registered value is what makes an alert report the **verdict** rather than an
artifact of when data happened to arrive.

### The reset-rule inversion

| | C8 (`GroupState`) | C9 (`transformWithState`) |
|---|---|---|
| on invocation | the timeout is **auto-reset** | registrations **persist**; only the timer that **fired** is deleted |
| what survives a firing | nothing — the single slot is empty | every **other** registered timer |
| your obligation | **re-arm**, every call | **delete** the ones that are now stale |
| forget it | key goes **silent** — an expected alert never comes | a **spurious** alert fires |                             |

A missing alert is eventually noticed as a gap; a wrong one gets acted on.
`listTimers()` exists because the stale timestamps were computed in a **previous**
invocation from data you no longer hold.

### Per-batch order

```scala
val outputIterator = newDataProcessorIter ++ timeoutProcessorIter
```

New data first, timers second — same shape as C8. The source gives the reason, and it
is not merely refreshing deadlines:

> Late-bind the timeout processing iterator so it is created **after** the input is
> processed … and **the state updates are written into the state store**. Otherwise
> the iterator may not see the updates (e.g. with RocksDB state store).

Consequence, visible in the demo at b8: a key whose timer is due **and** which has
data in the same batch does **not** fire, because `handleInputRows` deleted the timer
before timer processing ran. Same ordering as C8, opposite mechanism — there the
auto-reset did it, here your `deleteTimer` must.

### The firing timer is still in the store

```scala
iteratorWithImplicitKeySet(keyObj, mappedIterator, () => {
  processorHandle.deleteTimer(expiryTimestampMs)
})
```

`deleteTimer` is a **completion callback** — it runs after your callback's rows are
consumed. So `listTimers()` inside `handleExpiredTimer` returns
`{firing} ∪ {pending}`, and a DOWN branch that has just registered two follow-ups
reports **3**. Deliberate, not incidental.

---

## Metrics — read the units carefully

Three counters, three different units. Getting this wrong makes a dashboard lie.

| metric | counts | `ValueState` | `ListState` of N | `MapState` of N |
|---|---|---|---|---|
| `numRowsTotal` | **physical store keys** | 1 | **1** (merge operands under one key) | **N** (each entry is its own key) |
| `numRowsUpdated` | writes | 1 per write | 1 per element | 1 per entry write |
| `numRowsRemoved` | **logical entries** | 1 | **N** | **N** |

Two things follow:

- **`removed` is not the inverse of `total` for `ListState`.** `ListStateImpl.clear()`
  does `incrementMetric("numRemovedStateRows", entryCount)`, so clearing a 2-element
  list reports `removed = 2` while `total` drops by 1. The gap scales with list
  length. For `MapState` they agree, because both count entries.
- The hidden entry-counter column family is **excluded** from `numRowsTotal`
  (`isInternal = true`) — which is why the numbers add up without it.

`transformWithState` also publishes **custom metrics**, where the timer and TTL
counters live: `numRegisteredTimers`, `numExpiredTimers`, `numDeletedTimers`,
`numValuesRemovedDueToTTLExpiry`, `numDeletedStateVars`, plus per-type variable
counts.

**`numDeletedTimers` counts both** your explicit `deleteTimer` calls and the engine's
removal of expired ones — and **double-counts** a timer you delete while it is firing.

**`allRemovalsTimeMs` means TTL only** in this operator: *"It does not measure any
time taken by explicit calls from the user's state processor that `clear()`s state
variables."* A change from every earlier operator, where it meant watermark eviction.

---

## Lifecycle, measured

`init` and `close` run in **two different places**.

**On the driver, twice, before any batch.** `getColFamilySchemas()` and
`getStateVariableInfos()` each call `getDriverProcessorHandle()` — which runs your
`init` against a handle in `PRE_INIT` state — then `closeProcessorHandle()`. The
source is explicit: *"We initialize this processor handle in the driver to run the
init function and fetch the schemas of the state variables initialized in this
processor"*, and *"This instance of the stateful processor won't be used again."*

**That is the schema point made literal: Spark cannot know your state schema without
running your `init`, so it runs it purely to observe what you register, then throws
the instance away.**

**On executors, once per partition per micro-batch.** `close()` fires last, in the
batch's terminal `CompletionIterator`:

```
doTtlCleanup() → store.commit() → setStoreMetrics/setOperatorMetrics
               → closeStatefulProcessor() → setHandle(null) → CLOSED
```

Two operational consequences:

- **`close()` runs after the state store commit.** Nothing state-dependent belongs
  there.
- **`init` runs every batch, not once per query.** So it is *not* the place for
  expensive setup — a connection opened there is opened per micro-batch. Its job is
  registering state; use a lazily-initialized `object` for anything that should
  outlive a batch.

**Measured: 10 batches at one partition produced 12 `init`/`close` pairs — 2 driver + 10 executor.**

---

## The demo

[`demos/tier2/HeartbeatTransformWithStateDemo.scala`](../../src/main/scala/demos/tier2/HeartbeatTransformWithStateDemo.scala)
— `TimeMode.EventTime`, DOWN after 90s, ESCALATE at 180s, RETIRE at 300s, ring buffer
of 3 (the requirement says 100; 3 makes the wrap visible). Four state variables:
`_lastSeen`, `_deemedDownAt`, `_readings` (`MapState` ring), `_readingCount`.

```
batch  input              inherited wm   total upd removed   alert
  b0   D1,D2,D3 @30            0           9    9     0      3× OPENED
  b1   D1,D2,D3 @60           29          12    9     0
  b2   D1,D2,D3 @90           59          15    9     0      D3's last beat
  b3   D1,D2 @120            119          15    6     0      D1 ring WRAPS
  b4   D2@200               119→199        15    3     0
  b5   D2@260                199          16    4     0      D3 DOWN
  b6   D2@300                259          17    4     0      D1 DOWN
  b7   D2@360                299          17    3     0      D3 ESCALATED
  b8   D1,D2 @400            359          16    6     1      D1 RECOVERED
  b9   D2@450                399          10    3     6      D3 RETIRED
```

```
D3 DOWN      at=180 silentFor=90  readings=[{90,9},{60,6},{30,3}]      pendingTimers=3
D1 DOWN      at=210 silentFor=90  readings=[{120,10},{90,7},{60,4}]    pendingTimers=3
D3 ESCALATED at=270 silentFor=180 readings=[{90,9},{60,6},{30,3}]      pendingTimers=2
D1 RECOVERED at=400 silentFor=280 readings=[{400,16},{120,10},{90,7}]  pendingTimers=1
D3 RETIRED   at=390 silentFor=300 readings=[]                          pendingTimers=0
```

**The ring wrap, twice over.** D1 beat at 30/60/90/120 with a buffer of 3, so the
fourth write landed in slot 0 and overwrote t=30 — its DOWN alert carries
`[120, 90, 60]`, latest first, **t=30 absent**. And **b3 proves the bound is
structural**: D1 and D2 each appended a reading and `total` did not move (15 → 15).
Had the buffer been unbounded it would have risen; had `total` counted the map as one
row it would have been 6 all along.

**Concurrent timers.** D3's DOWN at b5 registers escalate@270 and retire@390 in one
place — `pendingTimers=3` there (two registered plus the down-check still firing),
2 at ESCALATED, 0 at RETIRED.

**Disambiguation with no stage field.** b5 DOWN, b7 ESCALATED, b9 RETIRED all arrive
through the same door with an empty iterator, told apart by the fired timestamp plus
`_deemedDownAt.exists()`.

**Delete works.** At b8 D1's escalate@300 is due (`wm=359`) and `numExpiredTimers=0`
— it never fired, because `handleInputRows` deleted it first. D1 is never escalated
and never retired.

**The counters.** b9: `removed=6` = `lastSeen` 1 + `deemedDownAt` 1 + `readingCount` 1
+ three ring entries; `total` drops 16 → 10, also 6 — they agree because `MapState`
  counts entries on both sides. `numDeletedTimers=3` at b9 = D2's explicit delete +
  D3's `clearTimers()` deleting the firing timer + the engine's own removal of it.

---

## C8 → C9, side by side

| | `flatMapGroupsWithState` | `transformWithState` |
|---|---|---|
| shape | one function, fixed signature | class with lifecycle methods |
| config | closure-captured `val`s | constructor parameters |
| state | one `S` per key | named pieces: value / list / map |
| write granularity | whole object | per piece (per entry for map) |
| removal | `remove()` — whole key | `clear()` per piece; no key-level delete |
| schema | `Encoder[S]` at compile time | `init` registration at runtime |
| two doors | one function + `hasTimedOut` | `handleInputRows` / `handleExpiredTimer` |
| timers | one slot, auto-reset | a list; persist until deleted |
| which timer fired | inferred from state | `expiredTimerInfo` |
| declarative expiry | none | TTL — **processing time only** |
| sum types as state | fails (`Product` bound) | fails, but pressure removed |


---

## Prove you got it

1. Why must state pieces have **names** rather than positions? Give a consequence
   beyond "the engine needs to address them".
2. `init` runs before any key is processed. Why is that **forced** by the need for
   partial writes rather than being a convenient hook?
3. On a device's first heartbeat `_deemedDownAt.exists()` is `false`, yet the variable
   was registered in `init`. What exactly does not exist?
4. Requirement 5 is "last 100 readings". Rank `ListState`, `ValueState[List]` and a
   `MapState` ring buffer, and say what each costs.
5. Requirement 5 changes to "readings from the last hour". Why does that break every
   state design, and why can't this processor use the feature that would fix it?
6. Under C8, forgetting to re-arm was the classic bug. What is the equivalent bug
   here, why is it worse, and why does `listTimers()` exist?
7. A `ListState` holding 2 elements is cleared. What do `numRowsTotal` and
   `numRowsRemoved` report, and why do they disagree? Would a `MapState` of 2 behave
   the same?
8. A timer callback registers two new timers and then calls `listTimers()`. Why does
   it report three?

<details>
<summary>Answers</summary>

1. Positions are brittle — reordering fields would silently repoint your state and
   break the checkpoint. Beyond addressing, names make state **externally
   addressable**: the State Data Source reader reads one variable by `stateVarName`,
   and schema evolution operates per variable. Neither works over an anonymous blob.
2. Partial writes require the engine to know the parts exist, which means declaring
   them; and the set of parts is a property of the processor, not of a batch, so
   declaration must precede any key. Spark literally runs `init` on the driver just
   to collect that schema.
3. The **variable** exists — registered, with its encoder and TTL config. What does
   not exist is the **entry for the pair `(deviceId, "deemedDownAt")`**. Registration
   establishes one coordinate; data establishes the other.
4. `MapState` ring buffer is best: O(1) point write, bounded structurally, no
   rewrite — cost is an N-entry read when an alert fires. `ValueState[List]` is next:
   simple and full `List` API, but every append rewrites all N (capped at N, so
   tolerable). `ListState` is worst here: append is O(1) but nothing bounds it, and
   trimming means read-all + `put`-all — a full rewrite plus an O(n) read.
5. A ring buffer bounds *how many*, never *how old*; no state design expresses an age
   bound, so you need a lifetime — i.e. TTL. But TTL is rejected under
   `TimeMode.EventTime` (`validateTTLConfig`, per variable, at declaration) because
   expiry is `batchTimestampMs + duration`, always processing time. Business logic
   needs event time, so the options are (a) split into two operators, or (b) stay in
   event time and prune by hand with timers and `clear()`.
6. Forgetting `deleteTimer`. Timers persist rather than auto-resetting, so a stale
   deadline fires for a device that has recovered — a **spurious alert**, whereas C8's
   failure was **silence**. A missing alert is noticed as a gap; a wrong one gets
   acted on. `listTimers()` exists because the stale timestamp was computed in a
   previous invocation from data no longer in hand.
7. `numRowsRemoved` reports **2** (`clear()` increments by `entryCount`) while
   `numRowsTotal` drops by **1**, because merge operands make the whole list one
   physical key. A `MapState` of 2 behaves differently: each entry is its own key, so
   both report 2 and they agree.
8. The firing timer is **still in the store** — the engine's `deleteTimer` is a
   completion callback that runs only after the callback's rows are consumed. So
   `listTimers()` returns `{firing} ∪ {2 new}` = 3.

</details>

---

## Spark 3.x → 4.x

- **New in Spark 4.0**
- **Requires the RocksDB state store provider**
  (`spark.sql.streaming.stateStore.providerClass`) — `ListState`'s merge operands and
  the TTL secondary index depend on it.
- Internals live under
  `org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate`;
  state-variable implementations under `…/statevariables`. The user-facing API is in
  `org.apache.spark.sql.streaming`.

---

## Sources

1. `ListStateImpl.scala` (Spark master, fetched firsthand) —
   `useMultipleValuesPerKey = true`, `store.merge` on `appendValue`, `clear()`
   incrementing `numRemovedStateRows` by `entryCount`.
2. `ListStateMetricsImpl.scala` (firsthand) — hidden entry-counter column family,
   `isInternal = true`, rationale quoted.
3. `TransformWithStateExec.scala` (firsthand) — `getDriverProcessorHandle` /
   `closeProcessorHandle`; `processData`'s `init`; the terminal `CompletionIterator`
   ordering; `newDataProcessorIter ++ timeoutProcessorIter` and its late-binding
   comment; `handleTimerRows` passing the registered timestamp and deleting the timer
   as a completion callback; the `allRemovalsTimeMs` comment.
4. `TTLConfig` scaladoc — duration semantics, reset-on-update, `TTLConfig.NONE`.
5. `StatefulProcessorHandleImpl.validateTTLConfig` — TTL rejected under
   `TimeMode.EventTime`, per variable, at declaration.
6. SPARK-50584 — "Modify state eviction metrics in TransformWithState to represent
   physical deletions"; SPARK-49846 — added `ListStateMetricsImpl`.
7. Structured Streaming Programming Guide (Spark 4.x) — `StatefulProcessor` methods,
   `implicits` object, state-variable and timer APIs, custom metrics.
8. Probes, this repo — `TwsTtlProbe` (per-element TTL on `ListState`, variable
   scoping); `AdtStateEncoderProbe` (sealed-trait ADT fails the `T <: Product` bound).
9. Demo: `demos/tier2/HeartbeatTransformWithStateDemo.scala` — trace reproduced above.

---

[← Concept 8 Part 2: Timeouts](./08-arbitrary-stateful-legacy-part2.md) · [Tier 2 index](./README.md)