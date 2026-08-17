package demos.tier2

import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.functions.{col, timestamp_seconds}
import org.apache.spark.sql.{Dataset, Encoders, SQLContext, SparkSession}
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryIdleEvent, QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

/**
 * Concept 9 -- transformWithState. The SAME heartbeat problem as Concept 8,
 * rewritten with the Spark 4 API.
 *
 * ============================== PURPOSE ==============================
 *
 * C8's processor satisfied requirements 1-4. This demo adds three more that its
 * single state slot and single timeout slot cannot absorb:
 *
 *   5. Keep the last N readings per device.
 *   6. When a device goes DOWN, ship those readings with the alert.
 *   7. If still down after 180s ESCALATE; retire the device entirely after 300s.
 *
 * FOUR CLAIMS, each observable in the trace:
 *
 *  1. COMPOSITE STATE. Four independently-named, independently-written variables
 *     instead of one packed case class. Appending a reading is a POINT WRITE to one
 *     ring slot -- it does not touch _lastSeen or _deemedDownAt. In C8 every one of
 *     those writes rewrote the whole object, readings included.
 *
 *  2. A TIMER LIST. At the moment a device is deemed DOWN we register BOTH
 *     follow-ups -- escalate and retire -- in one place. The deadlines are ORDERED
 *     (retire cannot precede escalate), so C8's one slot could have reached them
 *     stage by stage; what the list buys is that the whole schedule is DECLARED
 *     where the decision is made, instead of each stage having to remember to arm
 *     the next. Convenience and fewer failure modes, not new capability.
 *
 *  3. expiredTimerInfo DISAMBIGUATE. Each firing reports the timestamp it was
 *     registered for, so DOWN / ESCALATE / RETIRE are told apart with NO state
 *     field encoding stage position. C8's isDown did that job.
 *
 *  4. exists() IS THE STATUS. _deemedDownAt has no entry for a healthy device, so
 *     "is it down?" is a per-piece existence question. C8 needed a
 *     (Boolean, Option[Int]) pair inside one record with an unenforceable invariant.
 *
 * WHAT THIS DOES NOT DO: no handleInitialState, no schema evolution, no State Data
 * Source reader (Tier 4), and no TTL -- see below.
 *
 * ==================== TTL REQUIRES PROCESSING TIME ====================
 *
 * TimeMode picks ONE clock for the whole processor. Timers read it; TTL exists only
 * on the processing-time side, because expiry is computed as
 * batchTimestampMs + ttlDuration -- always processing time.
 *
 * Declaring a non-NONE TTL under TimeMode.EventTime is rejected per state variable
 * at declaration time by StatefulProcessorHandleImpl.validateTTLConfig:
 *   STATEFUL_PROCESSOR_INCORRECT_TIME_MODE_TO_ASSIGN_TTL
 *
 * This processor is EventTime (requirements 1-3 need it for replay determinism), so
 * every variable here carries TTLConfig.NONE. Requirement 5 is "last N", a COUNT
 * bound, which the ring buffer satisfies structurally with no TTL at all. Had the
 * requirement been "readings from the last hour" -- an AGE bound -- no state design
 * would express it and TTL would be the only answer, which this processor cannot
 * have. TTL semantics are established separately by TwsTtlProbe (ProcessingTime).
 *
 * ============================ THE SCENARIO ============================
 *
 * DOWN after 90s silent, ESCALATE at 180s, RETIRE at 300s -- all measured from
 * lastSeen, in EVENT time. Watermark delay 1s. Ring buffer size 3.
 *
 * BufferSize is 3, not the requirement's 100, so the ring VISIBLY WRAPS in a
 * readable trace: D1 beats four times, so its oldest reading is overwritten and
 * disappears from the DOWN alert. It is a constructor parameter, which is also the
 * point -- C8's thresholds were file-level vals captured by closure.
 *
 *   D1 -- beats 4x (ring wraps), goes down, ESCALATE is pending when it RETURNS.
 *         Recovery must delete both pending timers. Proof: D1 is never escalated
 *         and never retired.
 *   D2 -- healthy control. Beats throughout, so it advances the watermark for
 *         everyone else, and never alerts.
 *   D3 -- goes down, escalates, and is retired. Never returns.
 *
 * ============ READING THE TRACE (all four source-verified) ============
 *
 * A. THE FIRING TIMER IS STILL IN THE STORE during handleExpiredTimer. In
 *    TransformWithStateExec.handleTimerRows, deleteTimer is a COMPLETION CALLBACK
 *    on the output iterator, so it runs after your rows are consumed:
 *        iteratorWithImplicitKeySet(keyObj, mappedIterator, () => {
 *          processorHandle.deleteTimer(expiryTimestampMs) })
 *    So listTimers() inside a timer callback returns {firing} u {pending}: DOWN
 *    reports 3 pending, not the 2 it just registered.
 *
 * B. numDeletedTimers COUNTS BOTH our explicit deleteTimer calls AND the engine's
 *    removal of expired ones -- and DOUBLE-COUNTS a timer we delete while it is
 *    firing (the RETIRED branch does exactly that).
 *
 * C. THE COUNTERS USE DIFFERENT UNITS.
 *      numRowsTotal   -> user-visible physical store keys. one per (key, variable)
 *      numRowsUpdated -> one per (key, variable) write; one per ELEMENT for list
 *                        appends.
 *      numRowsRemoved -> LOGICAL entries: ListStateImpl.clear() increments by
 *                        entryCount, so a cleared 2-element list reports 2 while
 *                        total drops by 1.
 *    `removed` is NOT the inverse of `total`.
 *
 * D. NEW DATA IS PROCESSED BEFORE TIMERS, and the source gives the reason -- not
 *    merely to refresh deadlines, but so timer processing SEES the state writes:
 *        "Late-bind the timeout processing iterator so it is created *after* the
 *         input is processed ... and the state updates are written into the state
 *         store. Otherwise, the iterator may not see the updates."
 *    Visible at b8: D1's escalate timer is due AND D1 has data, and it does not
 *    fire -- because handleInputRows deleted it first.
 *
 * ======================= LIFECYCLE (source-verified) =======================
 *
 * init/close run TWICE ON THE DRIVER before any batch -- from getColFamilySchemas()
 * and getStateVariableInfos(), each discarding the instance afterwards ("This
 * instance of the stateful processor won't be used again") -- and then ONCE PER
 * PARTITION PER MICRO-BATCH on executors.
 *
 * close() fires last in the batch's CompletionIterator:
 *     doTtlCleanup() -> store.commit() -> setStoreMetrics/setOperatorMetrics
 *     -> closeStatefulProcessor() -> setHandle(null) -> CLOSED
 * So it runs AFTER the state store commit; nothing state-dependent belongs there.
 *
 * Also: in TWS, allRemovalsTimeMs measures TTL removals ONLY. Time spent in your
 * own clear() calls is not counted -- a change from every earlier operator, where
 * that metric meant watermark eviction.
 */
object HeartbeatTransformWithStateDemo {

  private val DownAfterSec     = 90
  private val EscalateAfterSec = 180
  private val RetireAfterSec   = 300
  private val BufferSize       = 3      // requirement says 100; we kept 3 to make the wrap visible
  private val WatermarkDelay   = "1 second"

  /**
   * Raw input event: one heartbeat from one device.
   *
   * @param deviceId grouping key -- what groupByKey extracts
   * @param t        event time, in seconds
   * @param value    the sensor reading; carried so the buffer has something worth keeping
   */
  final case class Heartbeat(deviceId: String, t: Int, value: Int)

  /**
   * A Heartbeat with a real Timestamp column added, so withWatermark has a column
   * to work on. This is the processor's input type I.
   *
   * `t` is kept alongside `eventTime` deliberately: the processor reasons in integer
   * seconds, while the ENGINE reasons about lateness and timer firing using
   * `eventTime`. Two representations of the same instant, for two readers.
   */
  final case class TimedBeat(deviceId: String, t: Int, value: Int,
                             eventTime: java.sql.Timestamp)

  /**
   * One entry in the per-device ring buffer.
   *
   * Narrower than TimedBeat on purpose: deviceId is the grouping key, so storing it
   * per element would repeat it once per reading, and eventTime is redundant with t.
   * State is where field-by-field frugality pays -- every element is serialized into
   * the state store and checkpoint.
   */
  final case class Reading(t: Int, value: Int)

  /**
   * Output row -- the alert emitted to ops. This is the processor's output type O.
   *
   * @param deviceId      the device this alert is about
   * @param alertType     OPENED | RECOVERED | DOWN | ESCALATED | RETIRED
   * @param at            event-time second the verdict refers to. For DOWN this is
   *                      lastSeen + DownAfterSec -- the VERDICT instant, not the
   *                      watermark value at which the timer happened to fire.
   * @param silentFor     seconds of event-time silence being reported; 0 for OPENED
   * @param readings      requirement 6: the buffered readings, ORDERED LATEST FIRST.
   *                      Watch D1's DOWN alert -- its oldest reading is absent,
   *                      because the ring wrapped and overwrote it.
   * @param pendingTimers timers registered for this key after this invocation. In a
   *                      timer callback this INCLUDES the timer currently firing
   *                      (see note A in the class doc).
   */
  final case class DeviceAlert(
                                deviceId:      String,
                                alertType:     String,
                                at:            Int,
                                silentFor:     Int,
                                readings:      Seq[Reading],
                                pendingTimers: Int
                              )

  // ==========================================================================
  // THE PROCESSOR
  // ==========================================================================
  class DowntimeDetector(downAfter: Int, escalateAfter: Int, retireAfter: Int,
                         bufferSize: Int)
    extends StatefulProcessor[String, TimedBeat, DeviceAlert] {

    /**
     * Last event-time second at which this device was seen. Monotonic -- a late beat
     * never moves it backwards. Every timer deadline for the key is derived from it.
     *
     * ValueState: a single scalar, read and written whole.
     */
    @transient private var _lastSeen: ValueState[Long] = _

    /**
     * The instant we CONCLUDED the device was down (lastSeen + downAfter).
     *
     * exists() on this variable IS the device's status. There is no isDown flag: a
     * healthy device has no verdict, so this variable simply has no entry for that
     * key. In C8 the same fact had to be a (Boolean, Option[Int]) pair inside one
     * packed record, with an invariant the type could not enforce.
     *
     * It stores a VERDICT, not a derived value. Recomputing it later from
     * lastSeen + downAfter would silently rewrite history if the threshold changed.
     */
    @transient private var _deemedDownAt: ValueState[Long] = _

    /**
     * Ring buffer of the most recent readings -- requirement 5.
     *
     * THE KEY IS A SLOT NUMBER (0 ... bufferSize-1), NOT a timestamp. The nth reading
     * ever appended goes to slot (n % bufferSize), so once full each write overwrites
     * the oldest entry and the map NEVER EXCEEDS bufferSize.
     *
     * Why MapState rather than the alternatives:
     *   ValueState[List] -- prepend is easy, but every append rewrites the whole list
     *   ListState        -- append is O(1) at the storage layer (RocksDB merge
     *                       operand), but nothing bounds it; trimming to the last N
     *                       means read-all + put-all, i.e. a full rewrite
     *   MapState (this)  -- updateValue is a POINT WRITE touching ONE entry, and the
     *                       bound is structural. No trimming code exists.
     *
     * Cost moves to the read: iterate and sort bufferSize entries -- but only when an
     * alert fires, which is rare, versus the write path on every heartbeat.
     *
     * Honest trade-off: the key carries no meaning, so "readings after time T" is not
     * answerable without a scan. Right structure for "last N", wrong one for
     * time-range queries.
     */
    @transient private var _readings: MapState[Long, Reading] = _

    /**
     * Count of readings ever appended for this key. Sole purpose: compute the next
     * ring slot as (count % bufferSize).
     *
     * Must be state, not a local -- the next batch's slot depends on it. A scalar,
     * written once per invocation, so the write is cheap.
     */
    @transient private var _readingCount: ValueState[Long] = _

    /**
     * REGISTERS the state schema. It allocates nothing and reads nothing.
     *
     * getXState is "create new or return existing" -- a register-or-lookup BY NAME
     * against the partition's state store. What comes back is a thin handle holding
     * (store, name, encoder, ttlConfig) and no data; the engine supplies the current
     * grouping key around each invocation, which is why one handle serves every key
     * in the partition.
     *
     * So after init, four VARIABLES exist and no key has a VALUE in any of them. That
     * is exactly why _deemedDownAt.exists() is false on a device's first heartbeat:
     * registration establishes one coordinate of (groupingKey, variableName), data
     * establishes the other.
     *
     * WHEN IT RUNS -- not once per query: twice on the DRIVER before any batch (so
     * Spark can observe what gets registered and learn the schema), then once per
     * partition PER MICRO-BATCH on executors. So this is NOT the place for expensive
     * setup: a connection opened here is opened every batch. Use a lazily-initialized
     * object for that; init's job is registering state.
     *
     * TTLConfig.NONE is FORCED on every variable -- see the class doc. Timers cannot
     * be registered here either: a timer belongs to a key, and there is no key
     * context yet.
     */
    override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
      println(s"[init]  ${java.time.Instant.now()}")

      _lastSeen = getHandle.getValueState[Long](
        "lastSeen", Encoders.scalaLong, TTLConfig.NONE)

      _deemedDownAt = getHandle.getValueState[Long](
        "deemedDownAt", Encoders.scalaLong, TTLConfig.NONE)

      _readings = getHandle.getMapState[Long, Reading](
        "readings", Encoders.scalaLong, Encoders.product[Reading], TTLConfig.NONE)

      _readingCount = getHandle.getValueState[Long](
        "readingCount", Encoders.scalaLong, TTLConfig.NONE)
    }

    // ---- small helpers -----------------------------------------------------

    private def secToMs(s: Int): Long = s.toLong * 1000L

    private def pendingTimerCount(): Int = getHandle.listTimers().size

    /**
     * Delete EVERY pending timer for this key.
     *
     * Necessary because timers persist until deleted -- there is no auto-reset (C8's
     * GroupState cleared its single slot on every invocation). listTimers() exists
     * because the stale timestamps were computed in a PREVIOUS invocation from data
     * no longer in hand.
     */
    private def clearTimers(): Unit =
      getHandle.listTimers().toList.foreach(ts => getHandle.deleteTimer(ts))

    /** Buffered readings, ORDERED LATEST FIRST -- requirement 6. */
    private def bufferedReadings(): Seq[Reading] =
      _readings.iterator().map(_._2).toList.sortBy(-_.t)

    /** Append one reading into the ring at slot (count % bufferSize). */
    private def appendReading(r: Reading): Unit = {
      val n = if (_readingCount.exists()) _readingCount.get() else 0L
      _readings.updateValue(n % bufferSize, r)   // POINT WRITE -- one entry
      _readingCount.update(n + 1)
    }

    /**
     * THE DATA DOOR -- invoked once per key that has rows in this batch, and never
     * for a key that is merely in state. (C8's lesson, unchanged: absence is not an
     * event; that is what timers are for.)
     *
     * FIVE STEPS, and the order is load-bearing:
     *
     *  1. READ THE PRIORS, before any write. prevSeen and wasDown are the PAST, and
     *     once you write, the past is gone. An edge is a function of present AND
     *     past, so writing before reading silently breaks the RECOVERED alert.
     *
     *  2. COMPUTE over the whole batch.In this example, iterator order is NOT guaranteed, so reduce
     *     with something order-independent (max). "Last row wins" would make the
     *     output depend on arrival order, and therefore differ on replay.
     *
     *  3. WRITE THE PIECES INDEPENDENTLY. appendReading is a point write to one ring
     *     slot; it does not touch _lastSeen or _deemedDownAt. In C8 each of these
     *     rewrote the entire packed object, readings included.
     *
     *  4. RECONCILE THE TIMERS. Every pending deadline was computed from the OLD
     *     lastSeen, so all of them are stale: delete them, then register the new
     *     down-check. Steps 3 and 4 are ONE obligation, not two -- bring the schedule
     *     into line with the state just written.
     *
     *     Skipping the delete produces a SPURIOUS alert for a device that just
     *     checked in, because a registered timer fires unless explicitly deleted --
     *     even for a key with data in this very batch (new-data output is emitted,
     *     THEN timer output). b8 is the proof that this step works: D1's escalate
     *     timer is due in that batch and does not fire.
     *
     *  5. EMIT. The alert is a snapshot of state at the moment it is constructed, so
     *     build it after 3 and 4 -- otherwise pendingTimers reports a schedule that
     *     no longer exists.
     *
     * At most one row: RECOVERED if the device had a verdict, OPENED on first
     * sighting, nothing otherwise. A healthy heartbeat is not news.
     */
    override def handleInputRows(
                                  key: String,
                                  inputRows: Iterator[TimedBeat],
                                  timerValues: TimerValues): Iterator[DeviceAlert] = {

      // 1. PRIORS -- before any write.
      val prevSeen = if (_lastSeen.exists()) Some(_lastSeen.get()) else None
      val wasDown  = _deemedDownAt.exists()

      // 2. COMPUTE -- order-independent.
      val beats = inputRows.toList
      val maxT  = beats.map(_.t).max

      // 3. WRITE THE PIECES.
      beats.sortBy(_.t).foreach(b => appendReading(Reading(b.t, b.value)))
      _lastSeen.update(math.max(prevSeen.getOrElse(Long.MinValue), maxT.toLong))
      if (wasDown) _deemedDownAt.clear()          // recovered: the verdict is void

      // 4. RECONCILE THE SCHEDULE.
      clearTimers()
      getHandle.registerTimer(secToMs(maxT + downAfter))

      // 5. EMIT.
      val out =
        if (wasDown)
          Some(DeviceAlert(key, "RECOVERED", maxT,
            silentFor = maxT - prevSeen.get.toInt,
            bufferedReadings(), pendingTimerCount()))
        else if (prevSeen.isEmpty)
          Some(DeviceAlert(key, "OPENED", maxT, 0, bufferedReadings(), pendingTimerCount()))
        else None

      out.iterator
    }

    /**
     * THE TIMER DOOR -- invoked ONCE PER EXPIRED TIMER. Two timers due for one key in
     * one batch means two separate calls.
     *
     * This method is why C8's hasTimedOut is gone: the two ways in are now two
     * methods, each taking the parameters it actually needs. There are no input rows
     * here by definition, and the engine rejects any attempt to read them.
     *
     * WHICH TIMER FIRED -- expiredTimerInfo.getExpiryTimeInMs() returns the timestamp
     * the timer was REGISTERED FOR, read straight out of the timer store. NOT the
     * moment of firing: the watermark may have overshot it by an arbitrary amount.
     * Using the registered value is what makes the alert report the VERDICT rather
     * than an artifact of when data happened to arrive.
     *
     * THREE BRANCHES, dispatched on the verdict's existence plus the fired timestamp.
     * No state field encodes stage position -- that was C8's isDown.
     *
     * REGISTERING BOTH FOLLOW-UPS AT ONCE is what the timer list buys. The deadlines
     * are ORDERED, so C8's single slot could have reached them stage by stage; the
     * gain is that the whole schedule is declared where the decision is made, instead
     * of each stage having to remember to arm the next.
     *
     * A FIRED TIMER REMOVES ONLY THE TIMER, never your state. clear() is still your
     * job, and there is no key-level delete -- retirement means clearing every piece
     * individually. Miss one, and it survives as an orphan, so exists() reads true
     * when the device returns and the logic branches as though it were still down.
     */
    override def handleExpiredTimer(
                                     key: String,
                                     timerValues: TimerValues,
                                     expiredTimerInfo: ExpiredTimerInfo): Iterator[DeviceAlert] = {

      val firedForSec = (expiredTimerInfo.getExpiryTimeInMs() / 1000L).toInt
      val lastSeen    = _lastSeen.get().toInt
      val silentFor   = firedForSec - lastSeen

      val alert =
        if (!_deemedDownAt.exists()) {
          // The down-check fired. Record the verdict and declare BOTH follow-ups now.
          val deemedDownAt = lastSeen + downAfter
          _deemedDownAt.update(deemedDownAt.toLong)
          getHandle.registerTimer(secToMs(lastSeen + escalateAfter))
          getHandle.registerTimer(secToMs(lastSeen + retireAfter))
          DeviceAlert(key, "DOWN", deemedDownAt, downAfter,
            bufferedReadings(), pendingTimerCount())

        } else if (silentFor >= retireAfter) {
          // Retire. Every piece must be cleared by hand -- there is no key delete.
          _lastSeen.clear(); _deemedDownAt.clear()
          _readings.clear(); _readingCount.clear()
          clearTimers()
          DeviceAlert(key, "RETIRED", firedForSec, silentFor, Seq.empty, pendingTimerCount())

        } else {
          // Escalate. State untouched; the retire timer is still pending.
          DeviceAlert(key, "ESCALATED", firedForSec, silentFor,
            bufferedReadings(), pendingTimerCount())
        }

      Iterator(alert)
    }

    /**
     * No-op here -- nothing external was opened. Kept to document when it runs.
     *
     * Fires last in the batch's terminal CompletionIterator:
     *   doTtlCleanup() -> store.commit() -> setStoreMetrics/setOperatorMetrics
     *   -> closeStatefulProcessor() -> setHandle(null) -> CLOSED
     *
     * So close() runs AFTER the state store commit -- nothing state-dependent belongs
     * here. And since init/close bracket EVERY micro-batch per partition, this is not
     * a query-lifetime teardown hook.
     */
    override def close(): Unit = println(s"[close] ${java.time.Instant.now()}")
  }

  // ==========================================================================

  private val spark: SparkSession = SparkSession.builder()
    .appName("HeartbeatTransformWithStateDemo")
    .master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.sql.streaming.noDataMicroBatches.enabled", "false")
    .config("spark.sql.streaming.stateStore.providerClass",
      "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
    .getOrCreate()

  implicit val sqlCtx: SQLContext = spark.sqlContext
  import spark.implicits._

  private def start(queryName: String)
  : (MemoryStream[Heartbeat], StreamingQuery, TwsTraceCollector) = {

    val input = MemoryStream[Heartbeat]

    val timed: Dataset[TimedBeat] = input.toDF()
      .withColumn("eventTime", timestamp_seconds(col("t")))
      .withWatermark("eventTime", WatermarkDelay)
      .as[TimedBeat]

    val alerts: Dataset[DeviceAlert] = timed
      .groupByKey(_.deviceId)
      .transformWithState(
        new DowntimeDetector(DownAfterSec, EscalateAfterSec, RetireAfterSec, BufferSize),
        TimeMode.EventTime(),
        OutputMode.Append())

    val collector = new TwsTraceCollector
    spark.streams.addListener(collector)

    val q = alerts.writeStream
      .format("memory").queryName(queryName).outputMode("append").start()

    (input, q, collector)
  }

  private def trace(label: String, c: TwsTraceCollector): Unit = {
    println(s"\n--- $label ---")
    c.snapshot().takeRight(1).foreach(t => println("  " + t))
  }

  private def sink(qName: String): Unit = {
    val df = spark.read.table(qName)
    if (df.isEmpty) println("  [alerts] (none)")
    else df.orderBy("at", "deviceId").show(truncate = false)
  }

  def main(args: Array[String]): Unit = {
    spark.sparkContext.setLogLevel("ERROR")
    println(s"\n===== transformWithState demo: Spark ${spark.version} =====")

    val qName = "tws_heartbeat"
    val (in, q, c) = start(qName)
    try {
      // b0-b2: all three beat. D1's ring fills (slots 0,1,2).
      in.addData(Heartbeat("D1", 30, 1), Heartbeat("D2", 30, 2), Heartbeat("D3", 30, 3))
      q.processAllAvailable(); trace("b0  all@30", c)

      in.addData(Heartbeat("D1", 60, 4), Heartbeat("D2", 60, 5), Heartbeat("D3", 60, 6))
      q.processAllAvailable(); trace("b1  all@60", c)

      in.addData(Heartbeat("D1", 90, 7), Heartbeat("D2", 90, 8), Heartbeat("D3", 90, 9))
      q.processAllAvailable(); trace("b2  all@90   (D3's last beat)", c)

      // b3: D1's 4th reading WRAPS the ring -- slot 0, overwriting t=30.
      in.addData(Heartbeat("D1", 120, 10), Heartbeat("D2", 120, 11))
      q.processAllAvailable(); trace("b3  D1@120,D2@120  <- D1 ring wraps; D1's last beat", c)

      in.addData(Heartbeat("D2", 200, 12))
      q.processAllAvailable(); trace("b4  D2@200   (wm=119 < D3's 180)", c)

      // b5: wm=199 > 180 -> D3 DOWN. Registers escalate@270 and retire@390.
      in.addData(Heartbeat("D2", 260, 13))
      q.processAllAvailable(); trace("b5  D2@260   <- D3 DOWN (2 timers registered)", c); sink(qName)

      // b6: wm=259 > 210 -> D1 DOWN. Its alert should show [120,90,60] -- t=30 GONE.
      in.addData(Heartbeat("D2", 300, 14))
      q.processAllAvailable(); trace("b6  D2@300   <- D1 DOWN; ring wrapped, t=30 absent", c); sink(qName)

      // b7: wm=299 > 270 -> D3 ESCALATED. Its retire@390 stays pending.
      in.addData(Heartbeat("D2", 360, 15))
      q.processAllAvailable(); trace("b7  D2@360   <- D3 ESCALATED", c); sink(qName)

      // b8: wm=359 > D1's escalate@300 -- BUT D1 HAS DATA. New data runs first and
      // deletes both of D1's timers, so the escalate does NOT fire. D1 RECOVERS.
      in.addData(Heartbeat("D1", 400, 16), Heartbeat("D2", 400, 17))
      q.processAllAvailable(); trace("b8  D1@400,D2@400  <- D1 RECOVERED, escalate deleted not fired", c); sink(qName)

      // b9: wm=399 > 390 -> D3 RETIRED. D1 stays (re-armed at 490).
      in.addData(Heartbeat("D2", 450, 18))
      q.processAllAvailable(); trace("b9  D2@450   <- D3 RETIRED", c); sink(qName)

    } finally { q.stop(); spark.streams.removeListener(c) }
  }
}

/** Per-batch trace: batchId, input rows, inherited watermark, the operator's
 *  total/updated/removed counters, and transformWithState's custom timer/TTL
 *  metrics -- which live only in customMetrics. */
final case class TwsBatchTrace(
                                batchId: Long, numInputRows: Long, watermark: String,
                                ops: Seq[(String, Long, Long, Long)], custom: Seq[(String, Long)]) {
  override def toString: String = {
    val opStr = ops.map { case (n, tot, u, r) =>
      f"$n%-22s total=$tot%-3d upd=$u%-3d removed=$r%-3d" }.mkString("; ")
    val keep = Set("numRegisteredTimers", "numExpiredTimers", "numDeletedTimers",
      "numValuesRemovedDueToTTLExpiry", "numDeletedStateVars")
    val cStr = custom.filter { case (k, _) => keep.contains(k) }
      .map { case (k, v) => s"$k=$v" }.mkString(" ")
    f"batch $batchId%-2d | in=$numInputRows%-2d | wm=$watermark%-24s | $opStr" +
      (if (cStr.nonEmpty) s"\n                 $cStr" else "")
  }
}

final class TwsTraceCollector extends StreamingQueryListener {
  private val traces: ArrayBuffer[TwsBatchTrace] = ArrayBuffer.empty
  override def onQueryStarted(event: QueryStartedEvent): Unit = ()
  override def onQueryProgress(event: QueryProgressEvent): Unit = {
    val p = event.progress
    val wm = Option(p.eventTime.get("watermark")).getOrElse("-")
    val ops = p.stateOperators.toSeq.map(so =>
      (so.operatorName, so.numRowsTotal, so.numRowsUpdated, so.numRowsRemoved))
    val custom = p.stateOperators.headOption.toSeq.flatMap { so =>
      Option(so.customMetrics).map(_.asScala.toSeq.map { case (k, v) => (k, v.toLong) })
        .getOrElse(Nil)
    }.sortBy(_._1)
    traces.synchronized { traces += TwsBatchTrace(p.batchId, p.numInputRows, wm, ops, custom) }
  }
  override def onQueryIdle(event: QueryIdleEvent): Unit = ()
  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = ()
  def snapshot(): Seq[TwsBatchTrace] = traces.synchronized(traces.toList)
}