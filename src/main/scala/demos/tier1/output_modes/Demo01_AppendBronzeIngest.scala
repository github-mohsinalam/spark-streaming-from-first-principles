package demos.tier1.output_modes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, current_timestamp}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

/**
 * Demo 01 — Kafka → bronze Delta in `append` mode.
 *
 * Reads sensor JSON events from the `sensor-events` topic, parses them
 * with an explicit schema, adds an ingestion timestamp for traceability,
 * and writes to a streaming Delta table in `append` mode.
 *
 * Why `append`:
 *   Raw sensor readings are immutable facts. Each input row produces
 *   exactly one output row, and that row will never be revised. This is
 *   the canonical `append`-mode use case from Concept 5.
 *
 * What you should observe:
 *   - The bronze Delta table grows continuously as the producer sends.
 *   - Each commit in _delta_log contains a `txn(appId, batchId)` action
 *     — same mechanism as Concept 4.5's Demo 01, just with Kafka as the
 *     source instead of `rate`.
 *
 * To run:
 *   1. Start the Kafka producer in a separate terminal:
 *        runMain demos.tier1.output_modes.KafkaSensorProducer
 *   2. Then in another terminal:
 *        runMain demos.tier1.output_modes.Demo01_AppendBronzeIngest
 */
object Demo01_AppendBronzeIngest {
  System.setProperty("hadoop.home.dir", "C:\\Program Files\\hadoop")

  private val kafkaBootstrap = "localhost:9092"
  private val kafkaTopic     = "sensor-events"

  private val bronzePath     = "/tmp/output-modes/bronze/sensor_events"
  private val checkpointPath = "/tmp/output-modes/bronze/sensor_events/_checkpoint"

  // The payload schema — what's INSIDE Kafka's `value` field. This is the
  // contract between the producer (which writes JSON shaped this way) and
  // this consumer (which parses JSON shaped this way). The case class
  // `SensorReading` is the Scala-side mirror of this schema, but at the
  // Catalyst plan level we declare it as a `StructType` here.
  private val payloadSchema: StructType = StructType(Array(
    StructField("sensorId",    StringType,    nullable = false),
    StructField("roomId",      StringType,    nullable = false),
    StructField("buildingId",  StringType,    nullable = false),
    StructField("eventTime",   TimestampType, nullable = false),
    StructField("temperature", DoubleType,    nullable = false),
    StructField("humidity",    DoubleType,    nullable = false),
    StructField("occupied",    BooleanType,   nullable = false)
  ))

  def main(args: Array[String]): Unit = {
    val spark = buildSpark()

    println(s"Reading from Kafka topic: $kafkaTopic")
    println(s"Writing to bronze Delta:  $bronzePath")
    println(s"Checkpoint:               $checkpointPath\n")

    // ------------------------------------------------------------------
    // Phase 1: read Kafka, parse the JSON payload into typed columns.
    // ------------------------------------------------------------------
    val rawKafka = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrap)
      .option("subscribe", kafkaTopic)
      // `latest` means: when there's no checkpoint, start from now. With a
      // checkpoint present, this is ignored (the checkpoint dictates where
      // to resume). For a fresh demo, `latest` keeps us from re-consuming
      // any stale records that may have been produced earlier in testing.
      // Switch to `earliest` if you want to backfill the full topic.
      .option("startingOffsets", "latest")
      .load()

    // At this point `rawKafka` has the standard Kafka envelope schema:
    //   key (binary), value (binary), topic, partition, offset,
    //   timestamp, timestampType, headers.
    // The `value` column contains the JSON payload — cast it to string,
    // then parse against our declared schema. Concept 2 in action:
    // Kafka defers the payload schema to the downstream parser.
    val parsed = rawKafka
      .select(
        from_json(col("value").cast("string"), payloadSchema).as("event")
      )
      .select("event.*")  // flatten the struct
      .withColumn("ingestionTime", current_timestamp())

    // ------------------------------------------------------------------
    // Phase 2: write to bronze Delta in `append` mode.
    // ------------------------------------------------------------------
    val query = parsed.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", checkpointPath)
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start(bronzePath)

    println(s"Streaming started. Query id: ${query.id}")
    println("Running for ~60 seconds. Ctrl-C to stop earlier.\n")

    // Print progress every few seconds so the demo isn't silent.
    val progressThread = new Thread(() => {
      try {
        while (query.isActive) {
          Thread.sleep(10000)
          val lastProgress = query.lastProgress
          if (lastProgress != null) {
            println(s"[progress] batchId=${lastProgress.batchId} " +
              s"inputRows=${lastProgress.numInputRows} " +
              s"rate=${"%.1f".format(lastProgress.processedRowsPerSecond)} rows/sec")
          }
        }
      } catch {
        case _: InterruptedException => // graceful stop
      }
    })
    progressThread.setDaemon(true)
    progressThread.start()

    query.awaitTermination(60000)
    query.stop()

    println("\nQuery stopped. Inspecting the bronze table:\n")

    val bronze = spark.read.format("delta").load(bronzePath)
    val rowCount = bronze.count()
    println(s"Total rows in bronze: $rowCount")

    println("\nSample rows:")
    bronze.show(5, truncate = false)
  }

  private def buildSpark(): SparkSession = {
    val spark = SparkSession.builder()
      .appName("Demo01_AppendBronzeIngest")
      .master("local[2]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.ui.showConsoleProgress", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }
}