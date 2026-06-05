package demos.tier1.output_modes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count}
import org.apache.spark.sql.streaming.Trigger

/**
 * Demo 02 — Complete-mode "dashboard" of occupied rooms.
 *
 * Reads the bronze Delta table that Demo 01 writes into, aggregates
 * to one row per occupied room (avg temperature, avg humidity), and
 * writes to the console sink in `complete` output mode.
 *
 * The product question this answers:
 *   "Show maintenance team the current conditions in occupied rooms,
 *    refreshed every few seconds."
 *
 * Why `complete`:
 *   - The result table is small and bounded (≤20 rows — one per room).
 *   - The downstream consumer (the dashboard / console) wants a fresh
 *     snapshot each refresh, not a changelog.
 *   - Re-emitting the whole table per trigger is cheap at this key
 *     cardinality.
 *
 *   For a key space that grows unboundedly (e.g. per-sensor instead of
 *   per-room, ~60 sensors), `complete` would still be feasible but
 *   wasteful. For a key space that grows truly large (millions of users),
 *   `complete` is a memory/network bomb and `update` is the right call.
 *   That contrast is what Demo 03 demonstrates.
 *
 * Two simplifications worth flagging in the implementation:
 *   1. The aggregation has no watermark, so its state grows in
 *      principle. For our 20-room key space that's negligible.
 *   2. The "occupied" filter happens BEFORE the aggregation, so a
 *      room appears if it has EVER had at least one occupied reading.
 *      A semantically tighter version would use the latest reading per
 *      room — that requires stateful logic from Tier 2.
 *
 * To run:
 *   1. Producer running in terminal 1.
 *   2. Demo 01 (the bronze writer) running in terminal 2.
 *   3. This demo in terminal 3.
 *
 * Or, after Demo 01 has written some data, this demo can run
 * standalone — it will read whatever is in bronze, plus any new
 * data that arrives while it's running.
 */
object Demo02_CompleteDashboard {
  System.setProperty("hadoop.home.dir", "C:\\Program Files\\hadoop")

  private val bronzePath     = "/tmp/output-modes/bronze/sensor_events"
  private val checkpointPath = "/tmp/output-modes/dashboard/sensor_events/_checkpoint"

  def main(args: Array[String]): Unit = {
    val spark = buildSpark()

    println(s"Reading bronze Delta as a streaming source: $bronzePath")
    println(s"Checkpoint: $checkpointPath")
    println("Output: console sink in `complete` mode\n")

    // Read bronze as a STREAMING source. Delta tables are first-class
    // streaming sources — every new commit (every batch Demo 01 writes)
    // becomes a new micro-batch here.
    val bronze = spark.readStream
      .format("delta")
      .load(bronzePath)

    // The aggregation. No watermark, no window — the result table is a
    // running average per occupied room over all history.
    val dashboard = bronze
      .filter(col("occupied") === true)
      .groupBy(col("roomId"))
      .agg(
        avg("temperature").as("avgTemperature"),
        avg("humidity").as("avgHumidity"),
        count("*").as("readingCount")
      )

    // The console sink in `complete` mode. Each trigger reprints the
    // ENTIRE current result table.
    val query = dashboard.writeStream
      .format("console")
      .outputMode("complete")
      .option("truncate", "false")
      .option("numRows", "30")  // show enough rows for our 20-room max
      .option("checkpointLocation", checkpointPath)
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    println(s"Streaming started. Query id: ${query.id}")
    println("Running for ~90 seconds. Each batch prints the full snapshot.")
    println("Ctrl-C to stop earlier.\n")

    query.awaitTermination(90000)
    query.stop()
  }

  private def buildSpark(): SparkSession = {
    val spark = SparkSession.builder()
      .appName("Demo02_CompleteDashboard")
      .master("local[2]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.ui.showConsoleProgress", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }
}