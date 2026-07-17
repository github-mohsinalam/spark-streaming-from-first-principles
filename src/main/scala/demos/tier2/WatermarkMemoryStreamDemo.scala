package demos.tier2

import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.functions.{col, count, timestamp_seconds, window}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryListener}
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryIdleEvent, QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

import scala.collection.mutable.ArrayBuffer

object WatermarkMemoryStreamDemo {

  private val WatermarkThreshold = "10 seconds"
  private val WindowDuration     = "5 seconds"

  private val spark: SparkSession = SparkSession.builder()
    .appName("WatermarkMemoryStreamSpec")
    .master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.sql.streaming.noDataMicroBatches.enabled", "false")
    .getOrCreate()

  //Needed for MemoryStream
  implicit val sqlContext: SQLContext = spark.sqlContext
  import spark.implicits._

  private def startWatermarkedQuery(queryName : String)
  : (MemoryStream[Int], StreamingQuery, TraceCollector) = {
    val input = MemoryStream[Int]

    // The Int IS the event time in seconds. timestamp_seconds does the conversion.
    val counts = input.toDF()
      .withColumn("eventTime", timestamp_seconds(col("value")))
      .withWatermark("eventTime", WatermarkThreshold)
      .groupBy(window(col("eventTime"), WindowDuration).as("w"))
      .agg(count("*").as("cnt"))
      // Project the window start back to a plain Long (seconds) so outputs read
      // in the same units we fed in.
      .select(
        col("w.start").cast("long").as("winStart"),
        col("w.end").cast("long").as("winEnd"),
        col("cnt")
      )

    //Register a listener to track the progress of the streaming query
    val collector = new TraceCollector
    spark.streams.addListener(collector)

    val query = counts.writeStream
      .format("memory")
      .queryName(queryName)  //we can access data in it by using spark.table(queryName)
      .outputMode("append")
      .start()

    (input, query, collector)

  }

  private def printTrace(label: String, collector: TraceCollector): Unit = {
    println(s"\n--- $label ---")
    collector.snapshot().foreach(t => println("  " + t))
  }

  /** Current contents of the MemorySink as (winStart, cnt) pairs. */
  private def sinkRows(label :  String , queryName: String): Unit = {
    println(s"\n---[Sink State] $label ---\n")
    spark.read.table(queryName)
      .select(col("winStart"), col("winEnd"), col("cnt"))
      .show(truncate = false)
  }

  /*
    This demo demonstrates :
    - A window which satisfies the watermark >= end is evicted.
    - UTC epoch is the starting watermark value.
    - Watermark calculated in batch N-1 is used in batch N .
        * Watermark at the end of batch 0 is calculated , 25-10 = 15
        * But, it did not cause eviction , see the trace for batch 0 , evicted = 0
        * In batch 1 , eviction was done and the window was emitted to the sink, see the trace for batch=1 , evicted=1
    - After batch 2 is processed, max event time seen so far is 30 , so watermark will advance to 30-10 = 20, and will
      be used for eviction in next batch. Since we don't add any batch , the window remains in the state. This further
      solidifies the fact that max event time of a batch N-1, impacts batch N.
      Try running the demo with : spark.sql.streaming.noDataMicroBatches.enabled = true and notice Spark adds a zero
      row batch, and does eviction. This is called eager state maintenance.
   */
  private def demoEvictionBoundary(queryName :  String): Unit = {

    val (input, query, collector) = startWatermarkedQuery(queryName)

    try{

      input.addData(10,11,16,15,21,20,25)
      query.processAllAvailable()
      printTrace("[Query progress trace]: demoEvictionBoundary", collector)
      sinkRows("After first addData", queryName)

      input.addData(24,23,26)
      query.processAllAvailable()
      printTrace("[Query progress trace]: demoEvictionBoundary", collector)
      sinkRows("After second addData", queryName)

      input.addData(26,22,30)
      query.processAllAvailable()
      printTrace("[Query progress trace]: demoEvictionBoundary", collector)
      sinkRows("After third addData", queryName)

    } finally {
      query.stop()
      spark.streams.removeListener(collector)
    }
  }

  /*
    This demo demonstrates :
    - Watermark is not an event-time filter on input records.
    - A below-watermark record still updates an open window
    - Number of rows dropped by watermark:  is not always same as the count of late input rows.
      this is represented by "dropped" in query progress trace
   */
  private def demoNotAFilter(queryName :  String): Unit = {

    val (input, query, collector) = startWatermarkedQuery(queryName)

    try {
      input.addData(103, 107)
      query.processAllAvailable()
      printTrace("[Query progress trace]: demoNotAFilter", collector)
      sinkRows("After first addData(103, 107)", queryName)


      input.addData(114, 102)      //102 is out of order record , wm = 107-10= 97
      query.processAllAvailable()
      printTrace("[Query progress trace]: demoNotAFilter", collector)
      sinkRows("After second addData(114, 102)", queryName)

      /*
        90,92, 103 is out of order record, wm = 114-10=104,
        90, 92 belongs to [90, 95) window for which , window end <= watermark and thus will be dropped, see the trace
        for batch=2
        103 belongs to [100, 105) window, for which ,window end > watermark , and thus it  updates the state, see the
        trace , upd=2 , one for the window [120,125) and one for [100,105). Note that this record is not dropped even
        though it being less than watermark.
       */
      input.addData(121, 103, 90, 92)
      query.processAllAvailable()
      printTrace("[Query progress trace]: demoNotAFilter", collector)
      sinkRows("After third addData(121, 103, 90, 92)", queryName)

      input.addData(122)
      query.processAllAvailable()
      printTrace("[Query progress trace]: demoNotAFilter", collector)
      sinkRows("After fourth addData(122)", queryName)

      input.addData(121, 123, 126)
      query.processAllAvailable()
      printTrace("[Query progress trace]: demoNotAFilter", collector)
      sinkRows("After fifth addData(121, 123, 126)", queryName)

      input.addData(127, 128, 129,130) //wm = 126-10=116  , eviction of [110, 115)
      query.processAllAvailable()
      printTrace("[Query progress trace]: demoNotAFilter", collector)
      sinkRows("After sixth addData(121, 123, 126)", queryName)

    } finally {
      query.stop()
      spark.streams.removeListener(collector)
    }

  }

  def main(args: Array[String]): Unit = {
    demoEvictionBoundary("wm_boundary")

    //demoNotAFilter("wm_not_a_filter")
  }


}


//Per-batch trace captured from the listener
private case class BatchTrace(
                               batchId: Long,
                               numInputRows: Long,
                               watermark: String,        // the watermark IN EFFECT for this batch (inherited)
                               numRowsTotal: Long,
                               numRowsUpdated: Long,
                               numRowsRemoved: Long,
                               numRowsDroppedByWatermark: Long) {

  override def toString: String =
    f"batch $batchId%-2d | in=$numInputRows%-2d | wm=$watermark%-24s | " +
      f"total=$numRowsTotal%-2d upd=$numRowsUpdated%-2d evicted=$numRowsRemoved%-2d " +
      f"dropped=$numRowsDroppedByWatermark"
}

private class TraceCollector extends StreamingQueryListener {
  private val traces: ArrayBuffer[BatchTrace] = ArrayBuffer.empty

  override def onQueryStarted(event: QueryStartedEvent): Unit = ()

  override def onQueryProgress(event: QueryProgressEvent): Unit = {
    val p = event.progress
    val wm = Option(p.eventTime.get("watermark")).getOrElse("-")
    val so = p.stateOperators.headOption
    traces.synchronized {
      traces += BatchTrace(
        batchId                   = p.batchId,
        numInputRows              = p.numInputRows,
        watermark                 = wm,
        numRowsTotal              = so.map(_.numRowsTotal).getOrElse(0L),
        numRowsUpdated            = so.map(_.numRowsUpdated).getOrElse(0L),
        numRowsRemoved            = so.map(_.numRowsRemoved).getOrElse(0L),
        numRowsDroppedByWatermark = so.map(_.numRowsDroppedByWatermark).getOrElse(0L)
      )
    }
  }

  override def onQueryIdle(event: QueryIdleEvent): Unit = ()
  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = ()

  //gives the snapshot of the progress of the streaming query
  def snapshot(): Seq[BatchTrace] = traces.synchronized(traces.toList)
}