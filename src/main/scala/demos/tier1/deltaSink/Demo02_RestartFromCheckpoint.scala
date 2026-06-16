package demos.tier1.deltaSink

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.functions._
import scala.reflect.io.Directory
import java.io.File

/**
 * Demo 02 — Restart from checkpoint.
 *
 * What this proves:
 *   On a clean stop-and-restart of the same query against the same
 *   checkpoint + Delta table, the table grows contiguously from where
 *   it left off. No duplicates, no data loss. This is the end-to-end
 *   correctness story of (replayable source) + (Spark checkpoint) +
 *   (streaming Delta sink) working together.
 *
 * What this does NOT directly prove:
 *   That the `txn` action specifically prevents a duplicate write.
 *   A clean restart goes through Spark's normal checkpoint-resume
 *   path: Spark's commit log records batch N as done, so on restart
 *   Spark skips to batch N+1 without invoking the Delta sink for N.
 *   The `txn` is never consulted on this happy path.
 *
 *   The `txn`-actually-saves-the-day code path fires only in the
 *   narrow failure window where Delta committed but Spark's commit
 *   log did not. Reproducing that race in a demo is brittle; we
 *   demonstrate `txn` idempotency more rigorously in Demo 04 by
 *   driving the Delta writer directly with the same batchId twice.
 *
 * What to look for in the output:
 *   - Phase 1 commits a few batches, then stops. We print the row
 *     count and the latest Delta version.
 *   - Phase 2 restarts and runs a few more batches. The row count
 *     grows by roughly the same amount; the `value` column shows
 *     a single contiguous sequence with no gaps or repeats; new
 *     Delta versions are appended on top of the existing ones.
 */
object Demo02_RestartFromCheckpoint {
  System.setProperty("hadoop.home.dir", "C:\\Program Files\\hadoop")

  private val tablePath      = "/tmp/delta-demos/02-restart"
  private val checkpointPath = "/tmp/delta-demos/02-restart/_checkpoint"

  def main(args: Array[String]): Unit = {

    run_demo()


  }

  /**
   * Starts the query, lets it run for `durationMs`, then stops cleanly.
   * Each call uses the same checkpoint and table path — that's the
   * restart behaviour we're testing.
   */
  private def runFor(spark: SparkSession, durationMs: Long): Unit = {
    val source = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 10)
      .load()

    val query: StreamingQuery = source.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", checkpointPath)
      .trigger(Trigger.ProcessingTime("3 seconds"))
      .start(tablePath)

    println(s"Query started. Running for ${durationMs / 1000} seconds.")
    query.awaitTermination(durationMs)
    query.stop()
    println("Query stopped.")
  }

  private def reportState(spark: SparkSession, label: String): Unit = {
    val df = spark.read.format("delta").load(tablePath)
    val rowCount = df.count()
    val minValue = df.agg(min("value")).first().getLong(0)
    val maxValue = df.agg(max("value")).first().getLong(0)

    val deltaLogDir = new File(s"$tablePath/_delta_log")
    val commitCount = Option(deltaLogDir.listFiles())
      .map(_.count(_.getName.matches("\\d{20}\\.json")))
      .getOrElse(0)

    println(s"$label: rows=$rowCount, value range=[$minValue, $maxValue], commits=$commitCount")
  }

  /**
   * Verifies that the `value` column is a contiguous sequence from
   * min to max. If a restart had caused duplicates or gaps, this check
   * would catch it.
   */
  private def contiguityCheck(spark: SparkSession): Unit = {
    val df = spark.read.format("delta").load(tablePath)
    val rowCount = df.count()
    val distinctCount = df.select("value").distinct().count()
    val minValue = df.agg(min("value")).first().getLong(0)
    val maxValue = df.agg(max("value")).first().getLong(0)
    val expectedCount = maxValue - minValue + 1

    println(s"rows=$rowCount  distinct values=$distinctCount  expected (max-min+1)=$expectedCount")

    if (rowCount == distinctCount && rowCount == expectedCount) {
      println("PASS — no duplicates, no gaps. Restart preserved exactly-once.")
    } else if (rowCount != distinctCount) {
      println("FAIL — duplicates present. Restart re-wrote committed batches.")
    } else {
      println("FAIL — gaps present. Restart skipped some offsets.")
    }
  }

  private def buildSpark(): SparkSession = {
    val spark = SparkSession.builder()
      .appName("Demo02_RestartFromCheckpoint")
      .master("local[3]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.ui.showConsoleProgress", "false")
      .getOrCreate()
    spark
  }

  private def cleanUp(path: String): Unit = {
    val dir = new Directory(new File(path))
    if (dir.exists) {
      dir.deleteRecursively()
      println(s"Cleaned previous run at $path")
    }
  }

  private def run_demo(): Unit = {
    // Clean slate for the demo as a whole, but we deliberately keep
    // state BETWEEN phase 1 and phase 2 — that's the point.
    cleanUp(tablePath)

    val spark = buildSpark()

    println("\n===== PHASE 1: initial run =====\n")
    runFor(spark, durationMs = 12000)

    println("\n--- After Phase 1 ---")
    reportState(spark, label = "Phase 1 final")

    println("\n===== PHASE 2: restart from same checkpoint =====\n")
    runFor(spark, durationMs = 12000)

    println("\n--- After Phase 2 ---")
    reportState(spark, label = "Phase 2 final")

    println("\n=== Checkpoint / Delta alignment ===")
    CheckpointInspector.inspect(checkpointPath, tablePath)

    println("\n=== Contiguity check ===")
    contiguityCheck(spark)
  }
}