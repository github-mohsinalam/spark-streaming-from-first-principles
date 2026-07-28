package demos.tier2

import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.functions.{col, expr, timestamp_seconds}
import org.apache.spark.sql.{Encoder, Encoders, SQLContext, SparkSession}
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryListener}
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryIdleEvent, QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

import scala.collection.mutable.ArrayBuffer

/**
 * Stream-stream INNER join demo. Reproduces the hand-traced scenario exactly.
 *
 *   - Left  (impressions):  Impression(adId, iT)   iT = impressionTime (seconds)
 *   - Right (clicks):       Click(adId, cT)        cT = clickTime       (seconds)
 *
 *   Join condition:  adId = adId  AND  cT >= iT  AND  cT <= iT + 10
 *
 *   Watermark threshold: 5s on both streams.  Global W = min(leftWm, rightWm).
 *   Derived state watermarks (from the hand derivation):
 *     impressions evicted when  iT <= W - 10   (bounded by cT <= iT + 10)
 *     clicks      evicted when  cT <= W         (bounded by cT >= iT)
 *
 * The Int in each event is the event time in seconds; timestamp_seconds converts.
 * We use a TYPED MemoryStream so each event carries BOTH adId and time. That needs
 * an `Encoder[Impression]` / `Encoder[Click]` -- provided by `import spark.implicits._`
 * via the case classes below.
 *
 * WHAT THIS PROVES (observable claims from the hand-trace):
 *   b0  clean match (A,10,13)
 *   b1  click D@25 buffered with NO impression yet (out-of-order setup) -> no match
 *   b2  impression D@22 arrives, matches the buffered D@25 -> (D,22,25) emitted
 *   b3  click F@8 DROPPED on admission (8 <= W=17) -> numRowsDroppedByWatermark>0
 *   b4  E: adId matches but cT=45 > iT+10=40 -> NO match (time-bound failure)
 *   b5  impression B@12 ages out having never matched (inner: vanishes silently)
 *
 *   THE ASSUMPTION UNDER TEST: a new row is dropped by the SAME state-watermark
 *   predicate that evicts old state (carried over from Concept 4). Watch
 *   numRowsDroppedByWatermark in b3.
 *
 * CANNOT DIRECTLY SHOW: numRowsTotal is buffered rows across BOTH sides combined
 * (SPARK-35896), not split left/right. Per-side inspection needs the State Data
 * Source reader (Tier 4). We verify totals, drops, removals, matches, watermark.
 */
object StreamStreamJoinDemo {

  private final case class Impression(adId: String, iT: Int)
  private final case class Click(adId: String, cT: Int)

  private val WatermarkThreshold = "5 seconds"

  private val spark: SparkSession = SparkSession.builder()
    .appName("StreamStreamJoinDemo")
    .master("local[3]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "1")
    // 1 batch per addData; no eager no-data batches, so every batch is attributable.
    .config("spark.sql.streaming.noDataMicroBatches.enabled", "false")
    .getOrCreate()

  //Needed for MemoryStream
  import spark.implicits._
  implicit val sqlContext: SQLContext = spark.sqlContext

  private def buildAndStart(queryName: String)
  : (MemoryStream[Impression], MemoryStream[Click], StreamingQuery, JoinTraceCollector) = {

    val impressions = MemoryStream[Impression]
    val clicks      = MemoryStream[Click]

    val impDF = impressions.toDF()
      .withColumnRenamed("adId", "impAdId")
      .withColumn("impressionTime", timestamp_seconds(col("iT")))
      .withWatermark("impressionTime", WatermarkThreshold)

    val clkDF = clicks.toDF()
      .withColumnRenamed("adId", "clkAdId")
      .withColumn("clickTime", timestamp_seconds(col("cT")))
      .withWatermark("clickTime", WatermarkThreshold)

    val joined = impDF.join(
      clkDF,
      expr(
        """
          impAdId = clkAdId AND
          clickTime >= impressionTime AND
          clickTime <= impressionTime + interval 10 seconds
        """),
      joinType = "inner"
    ).select(
      col("impAdId").as("adId"),
      col("iT"),
      col("cT")
    )

    val collector = new JoinTraceCollector
    spark.streams.addListener(collector)

    val query = joined.writeStream
      .format("memory")
      .queryName(queryName)
      .outputMode("append") // stream-stream inner join emits appended matched rows
      .start()

    (impressions, clicks, query, collector)
  }

  private def printTrace(label: String, collector: JoinTraceCollector): Unit = {
    println(s"\n--- $label ---")
    collector.snapshot().foreach(t => println("  " + t))
  }

  private def sinkRows(label: String, queryName: String): Unit = {
    println(s"\n---[Matched pairs so far] $label ---")
    spark.read.table(queryName)
      .select(col("adId"), col("iT"), col("cT"))
      .orderBy("adId", "iT")
      .show(truncate = false)
  }

  def main(args: Array[String]): Unit = {
    val qName = "ss_inner_join"
    val (imp, clk, query, collector) = buildAndStart(qName)
    try {
      // impressions A@10, B@12 ; click A@13   -> match (A,10,13)
      imp.addData(Impression("A", 10), Impression("B", 12))
      clk.addData(Click("A", 13))
      query.processAllAvailable()
      printTrace("After impressions A@10, B@12 ; click A@13", collector); sinkRows("after b0", qName)
      //  click D@25, NO impression yet (out-of-order setup)
      clk.addData(Click("D", 25))
      query.processAllAvailable()
      printTrace("After click D@25", collector); sinkRows("after b1", qName)
      // impression D@22 -> matches buffered D@25 -> (D,22,25)
      imp.addData(Impression("D", 22))
      query.processAllAvailable()
      printTrace("After impression D@22", collector); sinkRows("after b2", qName)
      // impression E@25 ; click F@8 (too late, dropped on admission)
      imp.addData(Impression("E", 25))
      clk.addData(Click("F", 8))
      query.processAllAvailable()
      printTrace("after impression E@25 ; click F@8", collector); sinkRows("after b3", qName)
      //  click E@45 -> same adId as E@30 but 45 > 30+10 -> NO match
      clk.addData(Click("E", 45))
      query.processAllAvailable()
      printTrace("after click E@45", collector); sinkRows("after b4", qName)
      // impression G@60 -> nothing to match; B@12 ages out unmatched
      imp.addData(Impression("G", 60))
      query.processAllAvailable()
      printTrace("after impression G@60", collector); sinkRows("after b5", qName)
    } finally {
      query.stop()
      spark.streams.removeListener(collector)
    }
  }
}

/** Per-batch trace. For joins, stateOperators may contain >1 entry, so we print
 * each operator's name and metrics. */
final case class JoinBatchTrace(
                                 batchId: Long,
                                 numInputRows: Long,
                                 watermark: String,
                                 ops: Seq[(String, Long, Long, Long, Long)] // (name, total, updated, removed, dropped)
                               ) {
  override def toString: String = {
    val opStr = ops.map { case (name, total, upd, rem, drop) =>
      f"$name%-14s total=$total%-3d upd=$upd%-3d removed=$rem%-3d dropped=$drop"
    }.mkString("\n                 ")
    f"batch $batchId%-2d | in=$numInputRows%-2d | wm=$watermark%-24s | $opStr"
  }
}

final class JoinTraceCollector extends StreamingQueryListener {
  private val traces: ArrayBuffer[JoinBatchTrace] = ArrayBuffer.empty

  override def onQueryStarted(event: QueryStartedEvent): Unit = ()

  override def onQueryProgress(event: QueryProgressEvent): Unit = {
    val p = event.progress
    val wm = Option(p.eventTime.get("watermark")).getOrElse("-")
    val ops = p.stateOperators.toSeq.map { so =>
      (so.operatorName, so.numRowsTotal, so.numRowsUpdated, so.numRowsRemoved, so.numRowsDroppedByWatermark)
    }
    traces.synchronized {
      traces += JoinBatchTrace(p.batchId, p.numInputRows, wm, ops)
    }
  }

  override def onQueryIdle(event: QueryIdleEvent): Unit = ()
  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = ()

  def snapshot(): Seq[JoinBatchTrace] = traces.synchronized(traces.toList)
}