package demos.tier2

import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.functions.{col, timestamp_seconds}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryListener}
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryIdleEvent, QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

import scala.collection.mutable.ArrayBuffer

/**
 * dropDuplicatesWithinWatermark demo (Spark 3.5+ API). Companion to
 * notes/tier-2/06-de-duplication.md -- see the .md for the mechanism, eviction
 * rule, and the annotated per-batch trace. This file just drives the scenario.
 *
 * Dedup key = id ONLY; watermark threshold D = 10s. Scale in integer seconds.
 */
object DropDuplicatesWithinWatermarkDemo {

  final case class Event(id: String, t: Int)

  private val WatermarkThreshold = "10 seconds"

  private val spark: SparkSession = SparkSession.builder()
    .appName("DropDuplicatesWithinWatermarkDemo")
    .master("local[3]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.sql.streaming.noDataMicroBatches.enabled", "false")
    .getOrCreate()

  implicit val sqlCtx: SQLContext    = spark.sqlContext
  import spark.implicits._

  private def start(queryName: String)
  : (MemoryStream[Event], StreamingQuery, TraceCollectorWW) = {

    val input = MemoryStream[Event]

    val deduped = input.toDF()
      .withColumn("eventTime", timestamp_seconds(col("t")))
      .withWatermark("eventTime", WatermarkThreshold)
      .dropDuplicatesWithinWatermark("id")   // id only; timestamp NOT in the key
      .select(col("id"), col("t"))

    val collector = new TraceCollectorWW
    spark.streams.addListener(collector)

    val query = deduped.writeStream
      .format("memory")
      .queryName(queryName)
      .outputMode("append")
      .start()

    (input, query, collector)
  }

  private def printTrace(label: String, collector: TraceCollectorWW): Unit = {
    println(s"\n--- $label ---")
    collector.snapshot().foreach(t => println("  " + t))
  }

  private def sinkRows(label: String, queryName: String): Unit = {
    println(s"\n---[Deduped output so far] $label ---")
    spark.read.table(queryName).orderBy("t", "id").show(truncate = false)
  }

  def main(args: Array[String]): Unit = {
    val qName = "ddww"
    val (in, query, collector) = start(qName)
    try {
      in.addData(Event("A", 5), Event("A", 5))          // same-batch duplicate
      query.processAllAvailable()
      printTrace("after (A@5, A@5)", collector); sinkRows("", qName)

      in.addData(Event("B", 12), Event("C", 13))          // two new ids
      query.processAllAvailable()
      printTrace("after (B@12, C@13)", collector); sinkRows("", qName)

      in.addData(Event("C", 15))                          // diff-timestamp dup of C
      query.processAllAvailable()
      printTrace("after (C@15)", collector); sinkRows("", qName)

      in.addData(Event("B", 12))                          // exact dup of B
      query.processAllAvailable()
      printTrace("after (B@12)", collector); sinkRows("", qName)

      in.addData(Event("E", 25))
      query.processAllAvailable()
      printTrace("after (E@25)", collector); sinkRows("", qName)

      in.addData(Event("A", 5))
      query.processAllAvailable()
      printTrace("after (A@5", collector); sinkRows("", qName)

    } finally {
      query.stop()
      spark.streams.removeListener(collector)
    }
  }
}

/** Per-batch trace; handles >=1 state operators, prints each operator's name. */
final case class DedupBatchTrace(
                                  batchId: Long,
                                  numInputRows: Long,
                                  watermark: String,
                                  ops: Seq[(String, Long, Long, Long, Long)] // (name, total, upd, removed, dropped)
                                ) {
  override def toString: String = {
    val opStr = ops.map { case (n, tot, u, r, d) =>
      f"$n%-20s total=$tot%-3d upd=$u%-3d removed=$r%-3d dropped=$d"
    }.mkString("\n                 ")
    f"batch $batchId%-2d | in=$numInputRows%-2d | wm=$watermark%-24s | $opStr"
  }
}

final class TraceCollectorWW extends StreamingQueryListener {
  private val traces: ArrayBuffer[DedupBatchTrace] = ArrayBuffer.empty
  override def onQueryStarted(event: QueryStartedEvent): Unit = ()
  override def onQueryProgress(event: QueryProgressEvent): Unit = {
    val p = event.progress
    val wm = Option(p.eventTime.get("watermark")).getOrElse("-")
    val ops = p.stateOperators.toSeq.map { so =>
      (so.operatorName, so.numRowsTotal, so.numRowsUpdated, so.numRowsRemoved, so.numRowsDroppedByWatermark)
    }
    traces.synchronized { traces += DedupBatchTrace(p.batchId, p.numInputRows, wm, ops) }
  }
  override def onQueryIdle(event: QueryIdleEvent): Unit = ()
  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = ()
  def snapshot(): Seq[DedupBatchTrace] = traces.synchronized(traces.toList)
}