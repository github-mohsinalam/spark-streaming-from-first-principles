package demos.tier2

import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.functions.{col, timestamp_seconds}
import org.apache.spark.sql.{Dataset, SQLContext, SparkSession}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, StreamingQuery, StreamingQueryListener}
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryIdleEvent, QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

import scala.collection.mutable.ArrayBuffer

/**
 * Concept 8, Part 2 -- Arbitrary Stateful Processing (legacy), WITH timeouts.
 * Companion to notes/tier-2/08-arbitrary-stateful-legacy-part2.md.
 *
 * Part 1 (HeartbeatNoTimeoutDemo) hit a ceiling: with NoTimeout the function is
 * invoked only for groups PRESENT in the batch, so an outage was detectable only
 * retrospectively, and a device that never came back was never detected at all.
 * EventTimeTimeout closes that gap.
 *
 * ========================= THE TWO-STAGE TIMER =========================
 *
 * There is exactly ONE timeout slot per key. A device lifecycle needs TWO
 * scheduled actions:
 *
 *      silent 90s   -> deem DOWN, alert
 *      silent 300s  -> deem decommissioned, REMOVE state
 *
 * So the single slot is used SEQUENTIALLY, and a state field tracks which stage
 * we are in. isDown is the STAGE DISCRIMINATOR: both firings arrive through the
 * same door, with the same empty iterator and the same hasTimedOut=true. Nothing
 * but persisted state can tell them apart.
 *
 *      timer fires, !isDown  -> STAGE 1: emit DOWN,    re-arm at lastSeen+300
 *      timer fires,  isDown  -> STAGE 2: emit RETIRED, state.remove()
 *
 * A device that recovers in between comes through the DATA door, which clears
 * isDown and re-arms normally -- so stage 2 never happens for it.
 *
 * THIS IS THE LIMITATION, DEMONSTRATED: one timer per key means a multi-stage
 * lifecycle must be MULTIPLEXED BY HAND through it, with state fields tracking
 * position. Add a third stage (escalate after an hour) and you are hand-rolling
 * a state machine over a single slot, where every transition must remember to
 * re-arm or the key silently drops out of the mechanism forever.
 * C9's transformWithState gives multiple NAMED timers per key plus declarative
 * per-value TTL, so "expire after N" becomes a property, not a stage you build.
 *
 * ============================ FOUR CLAIMS ============================
 *
 *  D1 -- goes down, then RETURNS. DOWN fires DURING the silence, then a paired
 *        RECOVERED. Requirements 1, 2, 3.
 *  D2 -- healthy control, beats every 30-70s. NEVER alerts: each invocation
 *        re-arms further into the future, so it is never due.
 *  D3 -- goes down and NEVER returns. DOWN fires (Part 1 could not do this),
 *        and later RETIRED + remove() -- the only batch where removed != 0.
 *  D4 -- TIE-BREAK probe. Due at b4 AND has data at b4. Per the operator's order
 *            processNewData(filteredIter) ++ processTimedOutState()
 *        ("the filtering for timeout occurs only after all the data has been
 *        processed ... to ensure that the timeout information of all the keys
 *        with data is updated before they are processed for timeouts")
 *        D4 must NOT fire -- it re-arms in processNewData first.
 *        NEW DATA WINS THE TIE.
 *
 * ============================ STATE SHAPE ============================
 *
 * Flat by necessity. The domain-correct sum type
 *     sealed trait DeviceState; case class Active(...); case class Down(...)
 * does NOT encode (probed, Spark 4.1.2: "Primitive types (Int, String, etc.) and
 * Product types (case classes) are supported"). Flattened by hand, carrying an
 * invariant the type cannot enforce:  deemedDownAt.isDefined <=> isDown.
 * The getOrElse in the recovery branch is the visible cost of that flattening.
 *
 * noDataMicroBatches.enabled=false: one batch per addData. It also forces the
 * watermark to be advanced by REAL DATA, which is honest about the mechanism --
 * an event-time timeout fires only when the watermark crosses it, and the
 * watermark only moves when data with later event times arrives. D2 does that
 * work here. NOTE: a no-data batch CANNOT advance the watermark (zero rows
 * contribute no event times); it only APPLIES an already-computed one. On a
 * totally silent stream an event-time timeout never fires, flag or no flag.
 *
 * All times are integer seconds; timestamp_seconds() converts to a Timestamp.
 */
object HeartbeatEventTimeTimeoutDemo {

  private val DownThresholdSec   = 90    // silent this long -> DOWN
  private val RetireAfterSec     = 300   // silent this long -> remove state
  private val WatermarkThreshold = "1 second"

  final case class Heartbeat(deviceId: String, t: Int)
  final case class TimedBeat(deviceId: String, t: Int, eventTime: java.sql.Timestamp)

  /** INVARIANT (unenforceable -- see header): deemedDownAt.isDefined <=> isDown */
  final case class DeviceState(lastSeen: Int, isDown: Boolean, deemedDownAt: Option[Int])

  /** silentFor is uniformly (at - lastSeen): 90 for DOWN, the gap for RECOVERED,
   *  300 for RETIRED. */
  final case class DeviceAlert(deviceId: String, alertType: String, at: Int, silentFor: Int)

  private val spark: SparkSession = SparkSession.builder()
    .appName("HeartbeatEventTimeTimeoutDemo")
    .master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.sql.streaming.noDataMicroBatches.enabled", "false")
    .getOrCreate()

  implicit val sqlCtx: SQLContext = spark.sqlContext
  import spark.implicits._

  /** setTimeoutTimestamp throws if the timestamp is below the current watermark. */
  private def armAt(state: GroupState[DeviceState], epochSec: Int): Unit =
    state.setTimeoutTimestamp(
      math.max(epochSec.toLong * 1000L, state.getCurrentWatermarkMs() + 1L))

  /**
   * TWO DOORS into this function: the timer (hasTimedOut) and arriving data.
   * The timeout invocation passes an EMPTY iterator, so hasTimedOut is the only
   * thing that disambiguate them.
   */
  def updateDevice(deviceId: String,
                   beats: Iterator[TimedBeat],
                   state: GroupState[DeviceState]): Iterator[DeviceAlert] = {

    if (state.hasTimedOut) {
      // ---- DOOR 1: the timer fired. beats is empty. State still EXISTS --
      // firing deletes nothing; we decide what happens.
      val prev = state.get

      if (!prev.isDown) {
        // STAGE 1 -- deem it down, alert once, and re-arm the SAME slot for
        // retirement. isDown=true is what makes the next firing stage 2.
        val deemedDownAt = prev.lastSeen + DownThresholdSec
        state.update(prev.copy(isDown = true, deemedDownAt = Some(deemedDownAt)))
        armAt(state, prev.lastSeen + RetireAfterSec)
        Iterator(DeviceAlert(deviceId, "DOWN", deemedDownAt, DownThresholdSec))
      } else {
        // STAGE 2 -- still down after RetireAfterSec. Retire it.
        // remove() is the ONLY way state leaves this operator: no watermark rule
        // applies to S. Deliberately NOT re-armed -- the key is gone.
        val retiredAt = prev.lastSeen + RetireAfterSec
        state.remove()
        Iterator(DeviceAlert(deviceId, "RETIRED", retiredAt, RetireAfterSec))
      }

    } else {
      // ---- DOOR 2: data arrived.
      val maxT = beats.map(_.t).max   // iterator order is NOT guaranteed

      val out = state.getOption match {
        case None =>
          state.update(DeviceState(maxT, isDown = false, deemedDownAt = None))
          Iterator.empty

        case Some(prev) if prev.isDown =>
          // We DID emit a DOWN -> emit the matching RECOVERED. getOrElse is
          // defensive against a state the DOMAIN forbids but the TYPE permits.
          val deemedDownAt = prev.deemedDownAt.getOrElse(prev.lastSeen + DownThresholdSec)
          state.update(DeviceState(maxT, isDown = false, deemedDownAt = None))
          Iterator(DeviceAlert(deviceId, "RECOVERED", maxT, maxT - prev.lastSeen))

        case Some(prev) =>
          // Never alerted DOWN -> emit nothing, even if the gap exceeds the
          // threshold. The isDown trade-off: no unpaired RECOVERED, at the cost
          // of silently missing an outage the timer never caught.
          state.update(prev.copy(lastSeen = math.max(prev.lastSeen, maxT)))
          Iterator.empty
      }

      // RE-ARM on every data invocation. The timeout is reset every time the
      // function is called, so this is not optional. It is also why a healthy
      // device never fires: it keeps pushing its own timeout forward.
      armAt(state, maxT + DownThresholdSec)
      out
    }
  }

  private def start(queryName: String)
  : (MemoryStream[Heartbeat], StreamingQuery, HbEtTraceCollector) = {

    val input = MemoryStream[Heartbeat]

    val timed: Dataset[TimedBeat] = input.toDF()
      .withColumn("eventTime", timestamp_seconds(col("t")))
      .withWatermark("eventTime", WatermarkThreshold)   // REQUIRED by EventTimeTimeout
      .as[TimedBeat]

    val alerts: Dataset[DeviceAlert] = timed
      .groupByKey(_.deviceId)
      .flatMapGroupsWithState(OutputMode.Append, GroupStateTimeout.EventTimeTimeout)(updateDevice)

    val collector = new HbEtTraceCollector
    spark.streams.addListener(collector)

    val query = alerts.writeStream
      .format("memory")
      .queryName(queryName)
      .outputMode("append")
      .start()

    (input, query, collector)
  }

  private def printTrace(label: String, collector: HbEtTraceCollector): Unit = {
    println(s"\n--- $label ---")
    collector.snapshot().foreach(t => println("  " + t))
  }

  private def sinkRows(label: String, queryName: String): Unit = {
    println(s"\n---[Alerts emitted so far] $label ---")
    val df = spark.read.table(queryName)
    if (df.isEmpty) println("  (none)") else df.orderBy("at", "deviceId").show(truncate = false)
  }

  def main(args: Array[String]): Unit = {
    spark.sparkContext.setLogLevel("ERROR")
    val qName = "hb_event_timeout"
    val (in, query, collector) = start(qName)
    try {
      // b0: four devices open. Each armed at t+90 = 120.
      in.addData(Heartbeat("D1", 30), Heartbeat("D2", 30), Heartbeat("D3", 30), Heartbeat("D4", 30))
      query.processAllAvailable()
      printTrace("b0  all@30   (armed 120)", collector); sinkRows("b0", qName)

      // b1: all beat again -> armed 150. LAST beat for D1 and D3.
      in.addData(Heartbeat("D1", 60), Heartbeat("D2", 60), Heartbeat("D3", 60), Heartbeat("D4", 60))
      query.processAllAvailable()
      printTrace("b1  all@60   (armed 150; last beat for D1,D3)", collector); sinkRows("b1", qName)

      in.addData(Heartbeat("D2", 130))
      query.processAllAvailable()
      printTrace("b2  D2@130   (wm=59, nothing due)", collector); sinkRows("b2", qName)

      in.addData(Heartbeat("D2", 200))
      query.processAllAvailable()
      printTrace("b3  D2@200   (wm=129 < 150, still nothing)", collector); sinkRows("b3", qName)

      // b4: wm=199 > 150 -> D1, D3, D4 all DUE. But D4 has DATA -> processNewData
      // re-arms it first, so D4 must NOT fire. D1, D3 go DOWN and re-arm at 360.
      in.addData(Heartbeat("D2", 260), Heartbeat("D4", 255))
      query.processAllAvailable()
      printTrace("b4  D2@260, D4@255  <- D1,D3 DOWN; D4 due but has data -> NOT down", collector)
      sinkRows("b4", qName)

      // b5: D1 returns -> RECOVERED (paired with b4's DOWN), isDown cleared,
      // re-armed at 390 -- so D1 never reaches stage 2.
      in.addData(Heartbeat("D1", 300), Heartbeat("D4", 300))
      query.processAllAvailable()
      printTrace("b5  D1@300, D4@300  <- D1 RECOVERED", collector); sinkRows("b5", qName)

      // b6: wm=299 < 360 -> D3 not yet due for retirement.
      in.addData(Heartbeat("D1", 400), Heartbeat("D2", 400), Heartbeat("D4", 400))
      query.processAllAvailable()
      printTrace("b6  D1@400, D2@400, D4@400  (wm=299 < 360, D3 not yet retired)", collector)
      sinkRows("b6", qName)

      // b7: wm=399 > 360 -> D3 STAGE 2: RETIRED + remove(). total drops 4 -> 3,
      // removed=1. No second DOWN for D3 -- isDown routed it to stage 2.
      in.addData(Heartbeat("D2", 450))
      query.processAllAvailable()
      printTrace("b7  D2@450   <- D3 RETIRED, state REMOVED", collector); sinkRows("b7", qName)

      println(
        s"""
           |=================== READ THE EVIDENCE ===================
           | 1. D3 -- silent forever, yet DOWN FIRES at b4 (during the outage).
           |    Part 1 could not detect this device at all. Requirement 1.
           |
           | 2. D4 -- TIE-BREAK. Due at b4 AND has data at b4 -> does NOT fire.
           |    processNewData runs before processTimedOutState and re-arms it.
           |    A DOWN for D4 at b4 would falsify the derivation.
           |
           | 3. D1 -- PAIRED. DOWN at b4, RECOVERED at b5 (silentFor=240).
           |    Recovery cleared isDown, so D1 never reaches stage 2.
           |
           | 4. D2 -- NEVER alerts. Every invocation pushes its timeout forward.
           |
           | 5. b7 -- THE TWO-STAGE TIMER. D3 fires a SECOND time, but as RETIRED,
           |    not DOWN: isDown routed it to stage 2. remove() runs ->
           |    removed=1 and total drops 4 -> 3. This is the ONLY way state ever
           |    leaves this operator -- no watermark rule applies to S.
           |
           | ALSO WATCH:
           |   - removed=0 in b0..b6, 1 in b7. Retirement is entirely OUR policy,
           |     scheduled through the SAME single timer slot as the DOWN alert.
           |   - upd in b4 = 4 with in=2: timeout invocations count as updated rows
           |     (D2,D4 via data; D1,D3 via timer). upd counts STATE WRITES.
           |   - does the stage-2 invocation (which removes rather than updates)
           |     count in upd? Watch b7. Unverified.
           |
           | (DOWN after ${DownThresholdSec}s, RETIRE after ${RetireAfterSec}s)
           |=========================================================
           |""".stripMargin)

    } finally {
      query.stop()
      spark.streams.removeListener(collector)
    }
  }
}

/** Per-batch trace: batchId, input rows, watermark in effect (inherited), and each
 *  stateful operator's total/updated/removed/dropped counters. */
final case class HbEtBatchTrace(
                                 batchId: Long,
                                 numInputRows: Long,
                                 watermark: String,
                                 ops: Seq[(String, Long, Long, Long, Long)]
                               ) {
  override def toString: String = {
    val opStr = ops.map { case (n, tot, u, r, d) =>
      f"$n%-26s total=$tot%-3d upd=$u%-3d removed=$r%-3d dropped=$d"
    }.mkString("\n                 ")
    f"batch $batchId%-2d | in=$numInputRows%-2d | wm=$watermark%-24s | $opStr"
  }
}

final class HbEtTraceCollector extends StreamingQueryListener {
  private val traces: ArrayBuffer[HbEtBatchTrace] = ArrayBuffer.empty
  override def onQueryStarted(event: QueryStartedEvent): Unit = ()
  override def onQueryProgress(event: QueryProgressEvent): Unit = {
    val p  = event.progress
    val wm = Option(p.eventTime.get("watermark")).getOrElse("-")
    val ops = p.stateOperators.toSeq.map { so =>
      (so.operatorName, so.numRowsTotal, so.numRowsUpdated, so.numRowsRemoved, so.numRowsDroppedByWatermark)
    }
    traces.synchronized { traces += HbEtBatchTrace(p.batchId, p.numInputRows, wm, ops) }
  }
  override def onQueryIdle(event: QueryIdleEvent): Unit = ()
  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = ()
  def snapshot(): Seq[HbEtBatchTrace] = traces.synchronized(traces.toList)
}