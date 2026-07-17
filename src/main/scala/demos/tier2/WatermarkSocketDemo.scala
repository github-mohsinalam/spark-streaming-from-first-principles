package demos.tier2

import java.sql.Timestamp
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryListener, Trigger}
import org.apache.spark.sql.streaming.StreamingQueryListener._

import scala.concurrent.duration._

/**
 * Companion to `WatermarkDataSender`. Reads the socket stream, runs a watermarked
 * windowed COUNT in append mode, and installs a StreamingQueryListener that
 * surfaces, per batch:
 *   - eventTime map: max / min / avg / watermark
 *   - stateOperators(0): numRowsTotal (state rows retained),
 *                        numRowsUpdated, numRowsRemoved,
 *                        numRowsDroppedByWatermark
 *   - numInputRows (to VERIFY socket batch alignment)
 *
 * These fields are the instrument that makes the invisible visible. Field names
 * verified against:
 *   - StateOperatorProgress ScalaDoc: numRowsTotal, numRowsUpdated,
 *     numRowsRemoved, numRowsDroppedByWatermark are Long fields.
 *   - Spark Structured Streaming Programming Guide: read
 *     "numRowsDroppedByWatermark" from "stateOperators" in the progress event;
 *     it is NOT an exact late-input-row count (streaming aggregation pre-aggregates
 *     before the lateness check), so interpret it as zero vs non-zero.
 *
 * SCALE: 10-second watermark, 5-second windows (compressed from the .md's
 * 10-min / 5-min; mechanics are unit-invariant).
 *
 * OUTPUT FORMAT NOTE: the eventTime map values (max/min/avg/watermark) print as
 * ISO-8601 UTC strings, not bare numbers, because Spark's ProgressReporter formats
 * them with ZoneId "Z". With our compact origin, e.g. epoch 543000 ms prints as
 * 1970-01-01T00:09:03Z and a watermark of 544000 ms as ...00:09:04Z. Read the
 * mm:ss part. (Source: apache/spark ProgressReporter.formatTimestamp uses
 * Instant.ofEpochMilli(...).atZone(ZoneId.of("Z")).)
 *
 * TIME ZONE: session tz set to UTC so window boundaries computed on the
 * GMT-relative Timestamp(long) values are not shifted by the machine's local
 * zone. (new Timestamp(long) is "milliseconds since 1970-01-01 00:00:00 GMT" per
 * the java.sql.Timestamp Javadoc; window() bucketing uses the session time zone.)
 *
 * RUN ORDER: start WatermarkDataSender FIRST, then this.
 */
object WatermarkSocketDemo {

  private val WatermarkThreshold = "10 seconds"
  private val WindowDuration     = "5 seconds"
  private val TriggerInterval    = 2.seconds
  private val Host               = "localhost"
  private val Port               = 12345

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("WatermarkDemo")
      .master("local[4]")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.shuffle.partitions", "1") // small state; keep the demo readable
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(new WatermarkProgressListener)

    import spark.implicits._

    // Socket source: one line per record, "key,epochMillis".
    val raw: DataFrame = spark.readStream
      .format("socket")
      .option("host", Host)
      .option("port", Port)
      .load()

    // Parse "key,epochMillis" -> (key: String, eventTime: Timestamp).
    // `value` is the single string column produced by the socket source.
    // millisToTs is lifted to a UDF because new Timestamp(long) is a Java
    // constructor, not a Spark SQL expression. (A pure-SQL alternative is
    // `timestamp_millis(col("ms"))` in Spark 3.1+, or `(col("ms")/1000).cast(
    // "timestamp")` which truncates sub-second; the UDF keeps the millis mapping
    // explicit and matches Daniel's `new Timestamp(...)` approach.)
    val millisToTs = udf((ms: Long) => new Timestamp(ms))

    val parsed: DataFrame = raw
      .select(
        split(col("value"), ",").getItem(0).as("key"),
        split(col("value"), ",").getItem(1).cast("long").as("ms")
      )
      .where(col("key").isNotNull && col("ms").isNotNull)
      .withColumn("eventTime", millisToTs(col("ms")))

    // Watermarked, windowed count. withWatermark references the SAME column used
    // in window(): required for the watermark to bind to the aggregation.
    val counts: DataFrame = parsed
      .withWatermark("eventTime", WatermarkThreshold)
      .groupBy(
        window(col("eventTime"), WindowDuration),
        col("key")
      )
      .count()
      .select(
        col("window.start").as("winStart"),
        col("window.end").as("winEnd"),
        col("key"),
        col("count")
      )

    val query :  StreamingQuery  = counts.writeStream
      .outputMode("append") // requires watermark; emits a window once, finalized
      .format("console")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime(TriggerInterval))
      .option("checkpointLocation", s"/tmp/watermark-demo/_checkpoint")
      .start()

    query.awaitTermination()
  }

  /**
   * Prints the watermark and state metrics after every batch. This is where the
   * two simulations get verified: watch the `watermark` value advance one batch
   * behind `max`, watch `[540000,545000)` emit one trigger after the watermark
   * crosses 545000, and watch numRowsDroppedByWatermark stay 0 for the
   * below-watermark-but-open record (F) yet go non-zero for the evicted-window
   * record (G).
   */
  private class WatermarkProgressListener extends StreamingQueryListener {
    override def onQueryStarted(event: QueryStartedEvent): Unit =
      println(s"[listener] query started: ${event.id}")

    override def onQueryProgress(event: QueryProgressEvent): Unit = {
      val p = event.progress
      val batchId = p.batchId
      val inputRows = p.numInputRows

      // eventTime is a java.util.Map[String, String] with keys max/min/avg/watermark.
      val et = p.eventTime
      def etv(k: String): String = Option(et.get(k)).getOrElse("-")

      val sb = new StringBuilder
      sb.append(s"\n[listener] ===== batch $batchId =====\n")
      sb.append(s"[listener] inputRows=$inputRows  (verify: should match the sender group size)\n")
      sb.append(s"[listener] eventTime: max=${etv("max")}  min=${etv("min")}  " +
        s"avg=${etv("avg")}  watermark=${etv("watermark")}\n")

      if (p.stateOperators.nonEmpty) {
        val so = p.stateOperators(0)
        sb.append(s"[listener] state: numRowsTotal=${so.numRowsTotal}  " +
          s"numRowsUpdated=${so.numRowsUpdated}  " +
          s"numRowsRemoved=${so.numRowsRemoved}  " +
          s"numRowsDroppedByWatermark=${so.numRowsDroppedByWatermark}\n")
        sb.append("[listener] (note: numRowsDroppedByWatermark is zero-vs-nonzero, " +
          "not an exact late-row count)\n")
      } else {
        sb.append("[listener] state: <no stateful operators reported this batch>\n")
      }
      print(sb.toString())
    }

    override def onQueryIdle(event: QueryIdleEvent): Unit =
      println(s"[listener] query idle (no data this tick)")

    override def onQueryTerminated(event: QueryTerminatedEvent): Unit =
      println(s"[listener] query terminated: ${event.id}")
  }
}