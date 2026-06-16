package demos.tier1.deltaSink

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

import java.nio.file.{Files, Paths}
import scala.reflect.io.Directory
import java.io.File
import scala.concurrent.duration._

/**
 * Demo 01 — Streaming append to a Delta table.
 *
 * Proves that `writeStream.format("delta").outputMode("append")` writes a
 * `txn` action to the Delta log for every micro-batch. This is the
 * mechanism that makes streaming Delta writes idempotent on retry, with
 * zero application code.
 *
 * What to look for in the output: each commit file (one per micro-batch)
 * contains a `txn(appId, batchId)` action alongside the `add` actions
 * for the data files.
 */
object Demo01_StreamingAppendDelta {
  System.setProperty("hadoop.home.dir", "C:\\Program Files\\hadoop")

  private val tablePath      = "/tmp/delta-demos/01-streaming-append"
  private val checkpointPath = "/tmp/delta-demos/01-streaming-append/_checkpoint"

  val spark: SparkSession = SparkSession.builder()
    .appName("Demo01_StreamingAppendDelta")
    .master("local[3]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    // Quieter logs so the demo output is readable.
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()

  private def cleanUp(path: String): Unit = {
    val dir = new Directory(new File(path))
    if (dir.exists) {
      dir.deleteRecursively()
      println(s"Cleaned previous run at $path")
    }
  }

  private def run_demo(sparkSession: SparkSession): Unit = {
    // Clean slate: previous runs of this demo would leave a checkpoint
    // and table behind, which would interfere with what we're trying to
    // show. Here we want a virgin table every time.
    cleanUp(tablePath)

    // Synthetic source: 10 rows/second, schema is (timestamp, value).
    val source = sparkSession.readStream
      .format("rate")
      .option("rowsPerSecond", 10)
      .load()

    // Streaming append to Delta. This is THE path that auto-writes `txn`.
    val query = source.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", checkpointPath)
      // Trigger every 3 seconds so we get a small handful of commits
      // in our 15-second window — easier to inspect than dozens.
      .trigger(Trigger.ProcessingTime(5.seconds))
      .start(tablePath)

    println(s"Streaming query started. Writing to: $tablePath")
    println("Running for ~15 seconds, then stopping.\n")

    query.awaitTermination(15000)
    query.stop()

    println("\n=== Row count in the table ===")
    val finalCount = sparkSession.read.format("delta").load(tablePath).count()
    println(s"$finalCount rows written.")

    /*
      Go in the directory where the table was written .
      Inside _delta_log directory, look for json file.
      Inside each json file, you will find txn block .
      On windows , /tmp/ is C:/tmp/
     */

  }

  def main(args: Array[String]): Unit = {

    run_demo(spark)

    //See the data in table
    val df = spark.read.format("delta").load(tablePath)
    df.show()




  }

}