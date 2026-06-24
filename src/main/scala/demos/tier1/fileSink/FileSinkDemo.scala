package demos.tier1.fileSink


import common.{payloadSchema, startProgressThread}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

/**
 * Demo — File sink with `_spark_metadata` inspection
 *
 * What this demo proves:
 *   The file sink achieves exactly-once-effect for append-only writes via an
 *   atomic per-batch manifest file at `_spark_metadata/<batchId>`. Each
 *   manifest is a file listing the data files committed by that batch with
 *   `"action": "add"`. The manifest is what makes the sink
 *   idempotent on retry; data files are written directly to their final
 *   paths with no staging directory.
 *
 * What to do while it runs:
 *   1. Watch `/tmp/file-sink/` — `part-*.json` files appear directly in the
 *      output directory each batch, with UUID-suffixed names. No
 *      `_temporary/` staging dir is created.
 *   2. Inspect `/tmp/file-sink/_spark_metadata/` — one file per batch named
 *      by `batchId` (`0`, `1`, `2`, ...). `cat` any of them to see the
 *      manifest format: `v1` header line, then one JSON action entry per
 *      data file produced by that batch.
 *   3. Compare against `/tmp/file-sink/_checkpoint/offsets/` and
 *      `/tmp/file-sink/_checkpoint/commits/` — three logs (offset, commit,
 *      manifest) line up per `batchId`.
 *
 *
 * To run:
 *   1. Start the Kafka sensor producer in a terminal
 *   2. Then this demo in another terminal
 *   3. Ctrl-C to stop. Inspect /tmp/file-sink/ and /tmp/file-sink/_spark_metadata/.
 */

object FileSinkDemo {

  System.setProperty("hadoop.home.dir", "C:\\Program Files\\hadoop")

  private val tablePath      = "/tmp/file-sink/"
  private val checkpointPath = "/tmp/file-sink/_checkpoint"

  private val kafkaBootstrap = "localhost:9092"
  private val kafkaTopic     = "sensor-events"



  val spark: SparkSession = SparkSession.builder()
    .appName("DemoFileSink")
    .master("local[3]")
    // Quieter logs so the demo output is readable.
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val rawKafka = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrap)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", "earliest")
      .load()


    val processedDf = rawKafka
      .select(
        from_json(col("value").cast(StringType), payloadSchema).as("events")
      ).select(
        col("events.*"),
        current_timestamp().as("ingestionTime")
      )

    val query = processedDf.writeStream
      .format("json")
      .outputMode("append")
      .option("checkpointLocation",checkpointPath)
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start(tablePath)

    println(s"Streaming query : ${query.id} started....")

    startProgressThread(query)

    query.awaitTermination()

  }



}
