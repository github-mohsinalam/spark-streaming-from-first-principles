package demos.tier2

import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.functions.{col, timestamp_seconds}
import org.apache.spark.sql.{Dataset, SQLContext, SparkSession}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, StreamingQuery, StreamingQueryListener}
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryIdleEvent, QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

import scala.collection.mutable.ArrayBuffer

/**
 * Concept 8, Part 1 -- Arbitrary Stateful Processing (legacy), WITHOUT timeouts.
 *
 * ======================= A NAIVE FIRST ATTEMPT =======================
 *
 * The ops requirement (device heartbeat / downtime detection):
 *   1. a device is DOWN if no heartbeat for 90 event-time seconds
 *   2. emit ONE alert per outage
 *   3. on recovery, emit one alert carrying the outage duration
 *   4. the device roster is not known in advance
 *
 * This is a DELIBERATELY NAIVE attempt: arbitrary state only, with
 * `GroupStateTimeout.NoTimeout`. It is not a failed demo -- it is the half of the
 * problem this much machinery can solve, built properly, with its ceiling made
 * visible. Prep for the real implementation in Part 2.
 *
 * WHAT IT TEACHES:
 *
 *  1. ARBITRARY STATE. You define `S`, you write the update function, you decide
 *     what to emit. State persists across batches entirely under your control --
 *     contrast the fixed accumulator of an aggregation.
 *
 *  2. INVOCATION IS DATA-DRIVEN. The function is called once per group PRESENT
 *     IN THE BATCH, with that group's rows as an Iterator, and NOT AT ALL for a
 *     group that has state but no rows in this batch. Instrument: numRowsTotal vs
 *     numRowsUpdated -- the gap between them IS the set of uninvoked devices.
 *     This is the scaladoc line "the function will be called only [for] the
 *     groups present in the batch", rendered as a number.
 *
 *  3. NOTHING EVICTS (numRowsRemoved = 0, always). In aggregation,stream-stream join and de-dup eviction was a
 *     rule the engine DERIVED FROM THE WATERMARK PROMISE and applied for you
 *     (window.end <= wm; iT + interval < W; wm >= T + D). Here no such rule
 *     exists: the engine cannot know which field of your S is a time, or what
 *     "too old" would mean for it. State is retained until YOU call remove().
 *     There, eviction was a CORRECTNESS decision the engine could derive.
 *     Here, retirement is a BUSINESS decision only you can make.
 *
 *  4. THE CEILING. Detection is RETROSPECTIVE ONLY -- an outage is catchable at
 *     the moment it ENDS, never while it is happening. D1 recovers and reports a
 *     correct 180s outage, 180s late. D3 never returns and is NEVER DETECTED AT
 *     ALL -- and that is exactly the device ops most needs to hear about.
 *     Closing that gap is what timeoutConf exists for (Part 2).
 *
 * WHAT THIS DOES NOT DO: no timeouts, no eviction-boundary probe, no performance
 * measurement of the monolithic-S cost (derived, not benchmarked -- Tier 4).
 *
 * ============================ STATE SHAPE ============================
 *
 * `S` is just the last-seen event time. Nothing more is needed to answer "how long
 * was it silent?" at the moment a heartbeat returns.
 *
 * `withWatermark` is declared but NOTHING CONSUMES IT (`NoTimeout` ignores it). Kept
 * so Part 2 differs only by timeoutConf and the new branch. The trace still shows
 * it advancing -- WatermarkTracker is independent of operators (C4).
 *
 * All times are integer seconds; `timestamp_seconds()` converts to a Timestamp.
 * Fixture rule worth respecting: a device meant to look HEALTHY must beat more
 * often than the threshold, or the data contradicts the model under test.
 */
object HeartbeatNoTimeoutDemo {

  private val downThresholdSec   = 90
  private val watermarkThreshold = "1 second"

  final case class Heartbeat(deviceId: String, t: Int)
  final case class TimedBeat(deviceId: String, t: Int, eventTime: java.sql.Timestamp)

  /** STATE: the last event time seen for this device. All Part 1 needs. */
  final case class DeviceState(lastSeen: Int)

  /**
   * OUTPUT. Flat, with a hand-rolled alertType discriminator: a sealed DeviceAlert
   * hierarchy would fail to encode for the same reason S would (U carries the same
   * [U: Encoder] bound). Forward-compatible with Part 2's "DOWN".
   */
  final case class DeviceAlert(
                                deviceId:     String,
                                alertType:    String,      // "RECOVERED" here; "DOWN" arrives in Part 2
                                deemedDownAt: Int,         // when we CONCLUDE it was down = lastSeen + threshold
                                recoveredAt:  Int,
                                silentFor:    Int          // recoveredAt - lastSeen
                              )

  private val spark: SparkSession = SparkSession.builder()
    .appName("HeartbeatNoTimeoutDemo")
    .master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.sql.streaming.noDataMicroBatches.enabled", "false")
    .getOrCreate()

  implicit val sqlCtx: SQLContext = spark.sqlContext
  import spark.implicits._

  /**
   * The update function. Named (not a lambda) so it can also be unit-tested in
   * isolation via TestGroupState -- no running query required.
   *
   * The whole batch arrives as an Iterator and is reduced with max: iterator order
   * is NOT guaranteed, so the result must be order-independent. "last row wins"
   * would make the output depend on arrival order, and therefore on replay.
   */
  def updateDevice(deviceId: String,
                   beats: Iterator[TimedBeat],
                   state: GroupState[DeviceState]): Iterator[DeviceAlert] = {

    val maxT = beats.map(_.t).max

    state.getOption match {
      case None =>
        // First sighting of this device. Nothing to compare against.
        state.update(DeviceState(maxT))
        Iterator.empty

      case Some(prev) =>
        // Previous reading of this device exits in the state.
        val silentFor = maxT - prev.lastSeen
        // Records can arrive in out of order event-time , Math.max ensures, last seen
        // for a device never regresses
        state.update(DeviceState(math.max(prev.lastSeen, maxT)))

        if (silentFor > downThresholdSec)
          Iterator(DeviceAlert(
            deviceId,
            "RECOVERED",
            deemedDownAt = prev.lastSeen + downThresholdSec,
            recoveredAt  = maxT,
            silentFor    = silentFor)
          )
        else Iterator.empty
    }
  }

  private def start(queryName: String)
  : (MemoryStream[Heartbeat], StreamingQuery, HbTraceCollector) = {

    val input = MemoryStream[Heartbeat]

    val timed: Dataset[TimedBeat] = input.toDF()
      .withColumn("eventTime", timestamp_seconds(col("t")))
      .withWatermark("eventTime", watermarkThreshold)   // declared; unused by NoTimeout
      .as[TimedBeat]

    val alerts: Dataset[DeviceAlert] = timed
      .groupByKey(_.deviceId)
      .flatMapGroupsWithState(OutputMode.Append, GroupStateTimeout.NoTimeout)(updateDevice)

    val collector = new HbTraceCollector
    spark.streams.addListener(collector)

    val query = alerts.writeStream
      .format("memory")
      .queryName(queryName)
      .outputMode("append")
      .start()

    (input, query, collector)
  }

  private def printTrace(label: String, collector: HbTraceCollector): Unit = {
    println(s"\n--- $label ---")
    collector.snapshot().foreach(t => println("  " + t))
  }

  private def sinkRows(label: String, queryName: String): Unit = {
    println(s"\n---[Alerts emitted so far] $label ---")
    val df = spark.read.table(queryName)
    if (df.isEmpty) println("  (none)") else df.orderBy("recoveredAt", "deviceId").show(truncate = false)
  }

  /**
   * Three devices, three roles:
   *   D1 -- goes silent, then RETURNS      -> the retrospective catch
   *   D2 -- healthy control; beats every 30-60s, always under the threshold, so it
   *         keeps the engine demonstrably ALIVE and never alerts
   *   D3 -- goes silent and NEVER returns  -> never detected, ever
   */
  def main(args: Array[String]): Unit = {
    spark.sparkContext.setLogLevel("ERROR")
    val qName = "hb_no_timeout"
    val (in, query, collector) = start(qName)
    try {
      // b0: three devices open.
      in.addData(Heartbeat("D1", 30), Heartbeat("D2", 30), Heartbeat("D3", 30))
      query.processAllAvailable()
      printTrace("b0  D1@30, D2@30, D3@30", collector); sinkRows("b0", qName)

      // b1: ordinary updates, no output.
      in.addData(Heartbeat("D1", 60), Heartbeat("D2", 60), Heartbeat("D3", 60))
      query.processAllAvailable()
      printTrace("b1  D1@60, D2@60, D3@60", collector); sinkRows("b1", qName)

      // b2: last beat for BOTH D1 and D3. Two outages start here.
      in.addData(Heartbeat("D1", 90), Heartbeat("D3", 90))
      query.processAllAvailable()
      printTrace("b2  D1@90, D3@90  <- last beat for D1 and D3", collector); sinkRows("b2", qName)

      // b3...b6: only D2 beats. total=3 but upd=1 -> TWO uninvoked devices.
      in.addData(Heartbeat("D2", 120))
      query.processAllAvailable()
      printTrace("b3  D2@120   (D1,D3 silent 30s)", collector); sinkRows("b3", qName)

      in.addData(Heartbeat("D2", 180))
      query.processAllAvailable()
      printTrace("b4  D2@180   (D1,D3 silent 90s -- THRESHOLD CROSSED, nothing happens)", collector)
      sinkRows("b4", qName)

      in.addData(Heartbeat("D2", 210))
      query.processAllAvailable()
      printTrace("b5  D2@210   (D1,D3 silent 120s -- still nothing)", collector); sinkRows("b5", qName)

      in.addData(Heartbeat("D2", 240))
      query.processAllAvailable()
      printTrace("b6  D2@240   (D1,D3 silent 150s -- still nothing)", collector); sinkRows("b6", qName)

      // b7: D1 returns -> retrospective alert. D3 does not, and never will.
      in.addData(Heartbeat("D1", 270), Heartbeat("D2", 270))
      query.processAllAvailable()
      printTrace("b7  D1@270 (returns), D2@270", collector); sinkRows("b7", qName)

      println(
        s"""
           |=================== READ THE EVIDENCE ===================
           | 1. ARBITRARY STATE: S = DeviceState(lastSeen). We defined it, we
           |    updated it, we chose what to emit. No fixed accumulator.
           |
           | 2. DATA-DRIVEN INVOCATION: in b3..b6, total=3 but upd=1.
           |    Only D2 is invoked. D1 and D3 have state and are NOT touched.
           |    total - updated = the uninvoked devices.
           |
           | 3. NOTHING EVICTS: removed=0 in EVERY batch. No watermark rule applies
           |    to S -- contrast C4 (window.end <= wm), C6 (iT+interval < W),
           |    C7 (wm >= T + D), where the engine evicted for you.
           |    State is retained until WE call remove().
           |
           | 4. THE CEILING -- retrospective only:
           |    - D1 recovers at b7: correct alert, silentFor=180, deemedDownAt=180.
           |      But it arrives 180s AFTER the outage began. Every fact needed to
           |      fire it sat in state the whole time.
           |    - D3 never returns: ZERO alerts, ever. Never detected at all --
           |      precisely the device ops most needs to hear about.
           |
           |    Requirements 1 and 2 are untouched; 3 fires only late. Closing that
           |    gap needs the engine to invoke us WITHOUT data -> Part 2, timeouts.
           |
           | (threshold = ${downThresholdSec}s)
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
final case class HbBatchTrace(
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

final class HbTraceCollector extends StreamingQueryListener {
  private val traces: ArrayBuffer[HbBatchTrace] = ArrayBuffer.empty
  override def onQueryStarted(event: QueryStartedEvent): Unit = ()
  override def onQueryProgress(event: QueryProgressEvent): Unit = {
    val p  = event.progress
    val wm = Option(p.eventTime.get("watermark")).getOrElse("-")
    val ops = p.stateOperators.toSeq.map { so =>
      (so.operatorName, so.numRowsTotal, so.numRowsUpdated, so.numRowsRemoved, so.numRowsDroppedByWatermark)
    }
    traces.synchronized { traces += HbBatchTrace(p.batchId, p.numInputRows, wm, ops) }
  }
  override def onQueryIdle(event: QueryIdleEvent): Unit = ()
  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = ()
  def snapshot(): Seq[HbBatchTrace] = traces.synchronized(traces.toList)
}