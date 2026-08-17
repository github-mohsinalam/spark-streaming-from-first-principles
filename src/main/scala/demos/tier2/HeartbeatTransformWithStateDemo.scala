package demos.tier2

import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.functions.{col, timestamp_seconds}
import org.apache.spark.sql.{Dataset, Encoders, SQLContext, SparkSession}
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryIdleEvent, QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

import java.time.Duration
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

/**
 * Concept 9 -- transformWithState. The SAME heartbeat problem as Concept 8,
 * rewritten with the Spark 4 API.
 *
 * ============================== PURPOSE ==============================
 *
 * FIVE claims, each one a thing the legacy API structurally could not do:
 *
 *  1. COMPOSITE STATE. Three independently-named, independently-serialized
 *     variables instead of one packed case class. Appending a reading does not
 *     rewrite _lastSeen or _deemedDownAt. (C8 break 1: "the cost of updating your
 *     cheapest field is set by the size of your largest".)
 *
 *  2. TWO CONCURRENT TIMERS PER KEY. When a device goes DOWN we register BOTH an
 *     ESCALATE deadline and a RETIRE deadline; they are live at the same time.
 *     GroupState had ONE slot and could hold only the earlier one -- C8's two
 *     stages worked only because they were SEQUENTIAL. (C8 break 3.)
 *
 *  3. expiredTimerInfo DISAMBIGUATE. Each firing reports the timestamp it was
 *     registered for, so ESCALATE and RETIRE are told apart with NO state field.
 *     In C8, isDown existed partly to discriminate stages -- that job is gone.
 *
 *  4. exists() REPLACES THE FLATTENED Option. A sealed ADT still does not encode
 *     (probed: Encoders.product[T] declares an upper bound T <: Product, and a
 *     sealed trait is a SUM type). But _deemedDownAt simply DOES NOT EXIST for an
 *     active device, instead of being a None the type permits alongside Active.
 *     The invariant moves from "a field that shouldn't be set" to "a variable that
 *     isn't there". Composite state does not fix sum types; it removes the
 *     pressure that made their absence painful.
 *
 *  5. TTL IS ENGINE-ENFORCED -- but NOT IN THIS PROCESSOR. See below.
 *
 * WHAT THIS DOES NOT DO: no handleInitialState, no schema evolution, no State
 * Data Source reader (Tier 4), no tie-break re-probe (C8 settled the ordering,
 * and the source shows the same `newDataProcessorIter ++ timeoutProcessorIter`).
 *
 * ================= TTL AND EVENT TIME ARE MUTUALLY EXCLUSIVE =================
 *
 * TimeMode picks ONE clock for the whole processor. Timers read it; TTL exists
 * only on the processing-time side, because expiry is computed as
 * batchTimestampMs + ttlDuration -- always processing time (TTLConfig docstring:
 * "Any state update resets the ttl to current processing time plus ttlDuration").
 *
 * Declaring a non-NONE TTL under TimeMode.EventTime is rejected per state
 * variable at declaration time, by StatefulProcessorHandleImpl.validateTTLConfig:
 *   STATEFUL_PROCESSOR_INCORRECT_TIME_MODE_TO_ASSIGN_TTL
 *
 * Consequence: event-time detection (requirements 1-3, which need event time for
 * replay determinism) and TTL-based expiry (requirement 7) CANNOT SHARE A
 * PROCESSOR. A pipeline needing both is split in two -- event-time detection
 * chained with a processing-time buffer owner.
 *
 * This demo is EventTime, so _readings uses TTLConfig.NONE and claims 1-4 are what
 * it proves. TTL semantics are established separately by TwsTtlProbe
 * (ProcessingTime mode): per-ELEMENT expiry on ListState, scoped to the VARIABLE
 * as declared, with a TTLConfig.NONE control variable surviving alongside.
 * ============================ THE SCENARIO ============================
 *
 * DOWN at 90s silent, ESCALATE at 180s, RETIRE at 300s -- all measured from
 * lastSeen, in EVENT time. Watermark delay 1s.
 *
 *   D1 -- goes down, escalates, then RETURNS. On recovery it must DELETE the
 *         still-pending RETIRE timer. Proof it worked: D1 is never retired.
 *   D2 -- healthy control. Beats often enough to stay under the threshold, and
 *         its data is what advances the watermark for everyone else.
 *   D3 -- goes down, escalates, and is RETIRED. Never returns.
 *
 * =================== READING THE TRACE (source-verified) ===================
 *
 * These surprised us on the first run; all four are confirmed in the Spark source.
 *
 * A. THE FIRING TIMER IS STILL IN THE STORE during handleExpiredTimer. In
 *    TransformWithStateExec.handleTimerRows, deleteTimer is a COMPLETION CALLBACK
 *    on the output iterator -- it runs AFTER your callback's rows are consumed:
 *        iteratorWithImplicitKeySet(keyObj, mappedIterator, () => {
 *          processorHandle.deleteTimer(expiryTimestampMs) })
 *    So listTimers() inside a timer callback returns {firing} u {pending}, and
 *    pendingTimers reads 3 at DOWN (2 just registered + the one firing), 2 at
 *    ESCALATED, 0 at RETIRED (we cleared them all).
 *
 * B. numDeletedTimers COUNTS BOTH our explicit deleteTimer calls AND the engine's
 *    removal of expired ones -- and DOUBLE-COUNTS when we delete a timer that is
 *    firing. b7: D2's clearTimers (1) + D3's clearTimers deleting the firing
 *    timer 360 (1) + the engine's own removal of 360 (1) = 3.
 *
 * C. THE COUNTERS USE DIFFERENT UNITS.
 *      numRowsTotal   -> user-visible physical store keys; a ListState of N
 *                        elements is ONE row (RocksDB merge operands).
 *      numRowsUpdated -> one per (key, variable) write; one per ELEMENT for
 *                        appendValue/put/appendList.
 *      numRowsRemoved -> LOGICAL entries: ListStateImpl.clear() does
 *                        incrementMetric("numRemovedStateRows", entryCount).
 *    Hence b7 removed=4 (lastSeen 1 + deemedDownAt 1 + readings 2) while total
 *    dropped by only 3. `removed` is NOT the inverse of `total`, and the gap
 *    scales with list length.
 *
 * D. NEW DATA IS PROCESSED BEFORE TIMERS, and the source says why -- not merely to
 *    refresh timers, but so timer processing SEES the state writes:
 *        "Late-bind the timeout processing iterator so it is created *after* the
 *         input is processed ... and the state updates are written into the state
 *         store. Otherwise the iterator may not see the updates."
 *
 * Also: getExpiryTimeInMs() returns the REGISTERED timestamp (read straight out of
 * the timer store), not the moment of firing -- b5 reports at=240 while the
 * inherited watermark is 259. That is why the alerts report the VERDICT instant.
 *
 * ======================= LIFECYCLE (source-verified) =======================
 *
 * init/close run TWICE on the DRIVER before any batch, then ONCE PER PARTITION PER
 * MICRO-BATCH on executors. The driver pairs come from getColFamilySchemas() and
 * getStateVariableInfos(), each calling getDriverProcessorHandle() then
 * closeProcessorHandle(): Spark cannot know the state schema without RUNNING your
 * init, so it runs it purely to observe what you register, then discards that
 * instance ("This instance of the stateful processor won't be used again").
 *
 * close() fires last in the batch's CompletionIterator, in this order:
 *     doTtlCleanup() -> store.commit() -> setStoreMetrics/setOperatorMetrics
 *     -> closeStatefulProcessor() -> setHandle(null) -> CLOSED
 * So close() runs AFTER the state store commit. Nothing state-dependent belongs
 * there. And because init runs every batch, it is NOT the place for expensive
 * setup (a connection would be opened per batch) -- its job is REGISTERING STATE.
 *
 * NOTE also: in TWS, allRemovalsTimeMs measures TTL removals ONLY. Time spent in
 * your own clear() calls is not counted -- a change from every earlier operator,
 * where that metric meant watermark eviction.
 *
 * =========================== THE deleteTimer FOOTGUN ===========================
 *
 * Timers are NOT auto-reset (GroupState reset its single slot on every
 * invocation). A registered timer fires unless explicitly deleted -- and per (D)
 * it fires even if the key received data in the same batch. listTimers() exists
 * because the stale timestamp was computed in a PREVIOUS invocation from data you
 * no longer hold.
 *
 * The failure mode INVERTED between the two APIs:
 *   C8  -- forget to re-arm  -> the key goes SILENT (an expected alert never comes)
 *   C9  -- forget to delete  -> a SPURIOUS alert fires for a recovered device
 * A missing alert is eventually noticed as a gap; a wrong one gets ACTED ON.
 */

object HeartbeatTransformWithStateDemo {

  private val DownAfterSec     = 90
  private val EscalateAfterSec = 180
  private val RetireAfterSec   = 300
  private val WatermarkDelay   = "1 second"

  /**
   * Raw input event: one heartbeat from one device.
   *
   * This is what the `MemoryStream` carries, before the event-time column is
   * derived. Times are plain integer seconds so the scenario reads as arithmetic
   * rather than timestamps.
   *
   * @param deviceId grouping key -- what `groupByKey` extracts
   * @param t        event time, in seconds
   * @param value    the sensor reading itself; carried only so the diagnostic
   *                 buffer has something worth keeping
   */
  final case class Heartbeat(deviceId: String, t: Int, value: Int)

  /**
   * A `Heartbeat` with a real `Timestamp` column added, so `withWatermark` has a
   * column to work on. This is the input type `I` of the processor.
   *
   * `t` is deliberately kept alongside `eventTime`: the processor reasons in
   * integer seconds, while the ENGINE reasons about lateness and timer firing
   * using `eventTime`. Two representations of the same instant, for two readers.
   *
   * @param deviceId  grouping key
   * @param t         event time in seconds -- what the processor's own logic uses
   * @param value     the sensor reading
   * @param eventTime `t` as a Timestamp -- what the watermark and event-time
   *                  timers are driven by
   */
  final case class TimedBeat(deviceId: String, t: Int, value: Int, eventTime: java.sql.Timestamp)


  /**
   * One entry in the per-device diagnostic buffer (`ListState[Reading]`).
   *
   * Deliberately narrower than `TimedBeat`: `deviceId` is the grouping key, so
   * storing it in every element would repeat it once per reading, and `eventTime`
   * is redundant with `t`. State is the one place where field-by-field frugality
   * pays -- every element is serialized into the state store and checkpointed.
   *
   * This is the variable that carries a TTL, so its elements are evicted by the
   * ENGINE. Nothing in the processor prunes it.
   *
   * @param t     event time of the reading, in seconds
   * @param value the sensor reading
   */
  final case class Reading(t: Int, value: Int)

  /**
   * Output row -- the alert emitted to ops. This is the output type `O`.
   *
   * FLAT BY NECESSITY. The domain-correct model is a sum type (`Down` carries a
   * verdict instant, `Recovered` carries a duration, and neither carries the
   * other's fields), but that does not encode: `Encoders.product[T]` declares an
   * upper bound `T <: Product`, and a sealed trait is a SUM type, not a product.
   * Probed and confirmed on Spark 4.1.2, same as Concept 8. So the discriminator
   * is a `String` field written by hand, and every row carries every field
   * whether it is meaningful for that alert kind.
   *
   * Composite state fixes this for STATE (each piece gets its own variable) but
   * not for OUTPUT, which is still a single Dataset row type.
   *
   * @param deviceId      the device this alert is about
   * @param alertType     hand-rolled discriminator:
   *                      OPENED | RECOVERED | DOWN | ESCALATED | RETIRED
   * @param at            the event-time second this verdict refers to. For DOWN
   *                      this is the VERDICT instant (`lastSeen + downAfter`),
   *                      not the watermark value at which the timer happened to
   *                      fire -- the two differ, and the verdict is the honest
   *                      one to report.
   * @param silentFor     seconds of event-time silence the alert is reporting.
   *                      0 for OPENED.
   * @param readings      how many buffered readings SURVIVED in state at emit
   *                      time. Non-zero means TTL has not evicted them yet; this
   *                      is the diagnostic payload ops would actually read.
   * @param pendingTimers timers still registered for this key AFTER this
   *                      invocation. The direct evidence for concurrent timers:
   *                      2 immediately after DOWN (escalate + retire), which a
   *                      single `GroupState` timeout slot could not represent.
   */
  final case class DeviceAlert(
                                deviceId:      String,
                                alertType:     String,
                                at:            Int,
                                silentFor:     Int,
                                readings:      Int,
                                pendingTimers: Int
                              )
  // ==========================================================================
  // THE PROCESSOR
  // ==========================================================================
  class DowntimeDetector(downAfter: Int, escalateAfter: Int, retireAfter: Int)
    extends StatefulProcessor[String, TimedBeat, DeviceAlert] {

    // Declared on the DRIVER, null. @transient keeps them out of serialization --
    // a ValueState handle is bound to a live state store and is meaningless off
    // the executor. NOTE the field type is ValueState[Long], a REFERENCE type, so
    // `= _` gives null (not 0L; the [Long] is what the state HOLDS).
    @transient private var _lastSeen: ValueState[Long] = _
    @transient private var _deemedDownAt: ValueState[Long] = _
    @transient private var _readings: ListState[Reading] = _

    /** Runs ONCE PER PARTITION on the executor, before any key is processed.
     *  getValueState is "create new OR RETURN EXISTING" -- a register-or-lookup
     *  against the state store BY NAME. The returned object is a thin handle: it
     *  holds no value, just (store, name, encoder, ttlConfig). The engine supplies
     *  the current grouping key around each invocation, which is why one handle
     *  serves every key in the partition. */
    override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
      println(s"Entered init method : ${java.time.Instant.now()}")
      _lastSeen = getHandle.getValueState[Long](
        "lastSeen", Encoders.scalaLong, TTLConfig.NONE)

      // exists() on this variable IS "the device is down". No isDown flag,
      // no Option, no invariant to maintain by hand. (claim 4)
      _deemedDownAt = getHandle.getValueState[Long](
        "deemedDownAt", Encoders.scalaLong, TTLConfig.NONE)

      // Engine-enforced expiry. Nothing in our code prunes this. (claim 5)
      _readings = getHandle.getListState[Reading](
        "readings", Encoders.product[Reading], TTLConfig.NONE)

      println(s"Exiting init method : ${java.time.Instant.now()}")
    }

    private def pendingTimerCount(): Int = getHandle.listTimers().size

    /** Delete EVERY pending timer for this key. Necessary because timers persist
     *  until deleted -- there is no auto-reset. listTimers() exists because the
     *  old timestamps were computed in a PREVIOUS invocation from data we no
     *  longer hold. */
    private def clearTimers(): Unit =
      getHandle.listTimers().foreach(ts => getHandle.deleteTimer(ts))

    private def secToMs(s: Int): Long = s.toLong * 1000L

    override def handleInputRows(
                                  key: String,
                                  inputRows: Iterator[TimedBeat],
                                  timerValues: TimerValues): Iterator[DeviceAlert] = {

      val beats = inputRows.toList
      // Iterator order is NOT guaranteed -> reduce with something order-independent.
      val maxT = beats.map(_.t).max

      // Claim 1: this appends to _readings WITHOUT touching _lastSeen or
      // _deemedDownAt. In C8 every one of these would have rewritten the whole
      // packed object, readings included.
      beats.sortBy(_.t).foreach(b => _readings.appendValue(Reading(b.t, b.value)))

      val prevSeen = if (_lastSeen.exists()) Some(_lastSeen.get()) else None
      _lastSeen.update(math.max(prevSeen.getOrElse(Long.MinValue), maxT.toLong))

      val wasDown  = _deemedDownAt.exists()          // claim 4

      // Any pending timer was computed from the OLD lastSeen -> all stale now.
      clearTimers()

      // Re-arm the down-check from the NEW lastSeen.
      //This is same structure as state.setTimeoutTimestamp ,
      getHandle.registerTimer(secToMs(maxT + downAfter))

      val out =
        if (wasDown) {
          _deemedDownAt.clear()                       // no longer down
          Some(DeviceAlert(key, "RECOVERED", maxT,
            silentFor = maxT - prevSeen.map(_.toInt).getOrElse(maxT),
            readings = _readings.get().size, pendingTimers = pendingTimerCount()))
        } else if (prevSeen.isEmpty) {  //first time entry
          Some(DeviceAlert(key, "OPENED", maxT, 0, _readings.get().size, pendingTimerCount()))
        } else None

      out.iterator
    }

    /* Called ONCE PER EXPIRED TIMER. expiredTimerInfo carries the timestamp the
     *  timer was registered for -- which is how ESCALATE and RETIRE are told
     *  apart with no state field. (claim 3)
     *
     *  A fired timer removes THE TIMER, not your state: "the new
     *  transformWithState doesn't manage your state". clear() is still our job. */
    override def handleExpiredTimer(
                                     key: String,
                                     timerValues: TimerValues,
                                     expiredTimerInfo: ExpiredTimerInfo): Iterator[DeviceAlert] = {

      val firedAtSec = (expiredTimerInfo.getExpiryTimeInMs() / 1000L).toInt
      val lastSeen   = _lastSeen.get().toInt
      val silentFor  = firedAtSec - lastSeen

      val alert =
        if (!_deemedDownAt.exists()) {
          // FIRST firing -> the down-check. Register BOTH follow-ups at once:
          // they are CONCURRENT, which is the thing GroupState could not express.
          val deemedDownAt = lastSeen + downAfter
          _deemedDownAt.update(deemedDownAt.toLong)
          getHandle.registerTimer(secToMs(lastSeen + escalateAfter))   // claim 2
          getHandle.registerTimer(secToMs(lastSeen + retireAfter))     // claim 2
          DeviceAlert(key, "DOWN", deemedDownAt, downAfter, _readings.get().size, pendingTimerCount())

        } else if (silentFor >= retireAfter) {
          // RETIRE. Wipe every variable for this key by hand.
          _lastSeen.clear(); _deemedDownAt.clear(); _readings.clear()
          clearTimers()
          DeviceAlert(key, "RETIRED", firedAtSec, silentFor, 0, pendingTimerCount())

        } else {
          // ESCALATE. State untouched; the RETIRE timer is still pending.
          DeviceAlert(key, "ESCALATED", firedAtSec, silentFor, _readings.get().size, pendingTimerCount())
        }

      Iterator(alert)
    }

    override def close(): Unit = {
      println(s"Inside close method : ${java.time.Instant.now()}")
    }
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

  private def start(queryName: String, ttl: Duration)
  : (MemoryStream[Heartbeat], StreamingQuery, TwsTraceCollector) = {

    val input = MemoryStream[Heartbeat]

    val timed: Dataset[TimedBeat] = input.toDF()
      .withColumn("eventTime", timestamp_seconds(col("t")))
      .withWatermark("eventTime", WatermarkDelay)
      .as[TimedBeat]

    val alerts: Dataset[DeviceAlert] = timed
      .groupByKey(_.deviceId)
      .transformWithState(
        new DowntimeDetector(DownAfterSec, EscalateAfterSec, RetireAfterSec),
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
    c.snapshot().foreach(t => println("  " + t))
  }

  private def sink(label: String, qName: String): Unit = {
    println(s"\n---[Alerts] $label ---")
    val df = spark.read.table(qName)
    if (df.isEmpty) println("  (none)")
    else df.orderBy("at", "deviceId").show(truncate = false)
  }

  /** MAIN SCENARIO -- event-time timers, TTL long enough that readings survive. */
  private def mainScenario(): Unit = {
    val qName = "tws_main"
    val (in, q, c) = start(qName, Duration.ofSeconds(30))
    try {
      in.addData(Heartbeat("D1", 30, 1), Heartbeat("D2", 30, 2), Heartbeat("D3", 30, 3))
      q.processAllAvailable(); trace("b0  all@30  (down-check armed 120)", c); sink("b0", qName)

      in.addData(Heartbeat("D1", 60, 4), Heartbeat("D2", 60, 5), Heartbeat("D3", 60, 6))
      q.processAllAvailable(); trace("b1  all@60  (re-armed 150; last beat D1,D3)", c); sink("b1", qName)

      in.addData(Heartbeat("D2", 130, 7))
      q.processAllAvailable(); trace("b2  D2@130  (wm=59)", c)

      in.addData(Heartbeat("D2", 200, 8))
      q.processAllAvailable(); trace("b3  D2@200  (wm=129 < 150)", c)

      // wm=199 > 150 -> D1,D3 DOWN. Each then registers TWO timers (240, 360).
      in.addData(Heartbeat("D2", 260, 9))
      q.processAllAvailable(); trace("b4  D2@260  <- D1,D3 DOWN; expect pendingTimers=2", c); sink("b4", qName)

      // wm=259 > 240 -> ESCALATED. The RETIRE timer (360) is still pending.
      in.addData(Heartbeat("D2", 300, 10))
      q.processAllAvailable(); trace("b5  D2@300  <- D1,D3 ESCALATED; retire still pending", c); sink("b5", qName)

      // D1 returns. It MUST delete the pending retire@360 or it fires later.
      in.addData(Heartbeat("D1", 400, 11), Heartbeat("D2", 400, 12))
      q.processAllAvailable(); trace("b6  D1@400,D2@400  <- D1 RECOVERED (deletes retire@360)", c); sink("b6", qName)

      // wm=399 > 360 -> D3 RETIRED. D1 must NOT be retired: proof the delete worked.
      in.addData(Heartbeat("D2", 450, 13))
      q.processAllAvailable(); trace("b7  D2@450  <- D3 RETIRED; D1 untouched", c); sink("b7", qName)

      println(
        s"""
           |=================== READ THE EVIDENCE (MAIN) ===================
           | 1. COMPOSITE STATE -- three named variables. Appending a reading does
           |    not rewrite lastSeen/deemedDownAt. In C8 one packed object meant
           |    every beat rewrote everything.
           |
           | 2. TWO CONCURRENT TIMERS -- at b4, DOWN registers escalate@240 AND
           |    retire@360. Watch pendingTimers=2. GroupState had ONE slot: C8's
           |    two stages only worked because they were SEQUENTIAL.
           |
           | 3. expiredTimerInfo -- b4 (DOWN), b5 (ESCALATED), b7 (RETIRED) are all
           |    the same door with an EMPTY iterator. They are told apart by the
           |    fired timestamp + exists(), with NO stage-discriminator field.
           |    C8 needed isDown for exactly this.
           |
           | 4. exists() -- _deemedDownAt does not exist for an active device.
           |    C8 had to flatten to (isDown, Option[Int]) with an unenforceable
           |    invariant and a defensive getOrElse.
           |
           | 5. deleteTimer -- D1 recovers at b6 with retire@360 pending. If the
           |    delete failed, D1 would be RETIRED at b7. It is not. Timers here
           |    do NOT auto-reset; forgetting the delete gives a SPURIOUS alert.
           |
           | TTL is NOT observable in this run: it is PROCESSING time (~2s of wall
           | clock here) while the scenario spans 450s of EVENT time. That two-clock
           | split is the finding, not a defect of the demo. See the TTL scenario.
           |
           | WATCH: numRowsUpdated with THREE variables per key -- does one
           | invocation writing two variables count 1 or 2? (unverified)
           |================================================================
           |""".stripMargin)
    } finally { q.stop(); spark.streams.removeListener(c) }
  }


  def main(args: Array[String]): Unit = {
    spark.sparkContext.setLogLevel("ERROR")
    println("\n===== transformWithState demo: Spark " + spark.version + " =====")
    mainScenario()
  }
}

/** Per-batch trace. Extends the Tier-2 harness with transformWithState's
 *  customMetrics -- the timer and TTL counters live only there. */
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
      Option(so.customMetrics).map(_.asScala.toSeq.map { case (k, v) => (k, v.toLong) }).getOrElse(Nil)
    }.sortBy(_._1)
    traces.synchronized { traces += TwsBatchTrace(p.batchId, p.numInputRows, wm, ops, custom) }
  }
  override def onQueryIdle(event: QueryIdleEvent): Unit = ()
  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = ()
  def snapshot(): Seq[TwsBatchTrace] = traces.synchronized(traces.toList)
}