package demos.tier3

import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryIdleEvent, QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.streaming.{StreamingQueryListener, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

/**
 * Tier 3 - Concept 1 (part 1): who owns the Kafka read position.
 *
 * Everything Tier 2 proved was provable inside one JVM with a MemoryStream,
 * because the failure mode was "wrong output". Here the failure mode is
 * "wrong AFTER A RESTART", so the demo has to span process lifetimes: it runs
 * as separate invocations against ONE topic and ONE checkpoint.
 *
 *   sbt "runMain demos.tier3.KafkaOffsetOwnershipDemo stage1"
 *   sbt "runMain demos.tier3.KafkaOffsetOwnershipDemo stage2a"
 *   sbt "runMain demos.tier3.KafkaOffsetOwnershipDemo stage2b"
 *   sbt "runMain demos.tier3.KafkaOffsetOwnershipDemo stage3"
 *
 * Run them in order. stage1 resets the topic and the checkpoint.
 *
 * ---------------------------------------------------------------------------
 * WHAT THIS PROVES
 *
 *  1. offsets/N is written BEFORE the batch runs; commits/N after it finishes.
 *     Observed as: after the injected failure, offsets/1 exists and commits/1
 *     does not.
 *
 *  2. THE PLAN IS BINDING. On restart the engine re-executes batch 1 with
 *     byte-identical end offsets -- even though far more data is available by
 *     then. It does not replan. This is the headline: it is why no config
 *     change rescues a batch that is already too big for the cluster.
 *
 *  3. startingOffsets is read ONLY at query birth. stage2a restarts with
 *     "latest" and the query still starts from where it left off.
 *
 *  4. That initialization lives in the SOURCE's own metadata log
 *     (sources/0/0), not in offsets/. Printed so its independence is visible.
 *
 *  5. Spark commits NOTHING back to Kafka. No consumer group with the
 *     "spark-kafka-source" prefix holds committed offsets.
 *
 *  6. Consequently, lag must come from : the per-source metrics
 *     min/max/avgOffsetsBehindLatest on StreamingQueryProgress.
 *
 *  7. Partitions added to a live topic start from EARLIEST regardless of
 *     startingOffsets. stage3 grows the topic and shows the new partitions'
 *     records arriving under startingOffsets="latest".
 *
 * Also, visible in passing: batch 0 reads 48 records, not 50, because
 * maxOffsetsPerTrigger is split proportionally across partitions and each
 * share is floored -- 50 * (100/300) = 16.67 -> 16, three times.
 *
 * ---------------------------------------------------------------------------
 * WHAT THIS DELIBERATELY DOES NOT PROVE
 *
 *  - failOnDataLoss / retention-driven loss. Reproducing it means fighting
 *    retention.ms and segment.ms locally and yields a flaky demo.
 *  - Event-time skew across partitions dropping rows at the watermark. Real,
 *    but needs a stateful query; separate demo.
 *  - Memory pressure from the wide-topic rate-limit overshoot. The record-count
 *    overshoot is measured in KafkaRateLimitProbe; the OOM is a sizing argument.
 *
 * ---------------------------------------------------------------------------
 * HOW THE CRASH IS SIMULATED
 *
 * Not `kill -9`: that is realistic but non-deterministic -- you cannot choose
 * which batch dies, nor reliably land before the commit-log write. Instead, a
 * chosen batchId throws from inside foreachBatch. The checkpoint state left
 * behind is identical at the only boundary that matters: offsets/N written,
 * batch not completed, commits/N absent.
 */
object KafkaOffsetOwnershipDemo {

  private val Topic          = "t3c1-ownership"
  private val Checkpoint     = "/tmp/spark-streaming/tier3/c1/ownership/_checkpoint"
  private val InitialParts   = 3
  private val GrownParts     = 6
  private val PerPartition   = 100
  private val MaxOffsets     = 50
  private val SparkGroupPrefix = "spark-kafka-source"

  // --------------------------------------------------------------- spark ---

  private lazy val spark: SparkSession = SparkSession.builder()
    .appName("KafkaOffsetOwnershipDemo")
    .master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.streaming.noDataMicroBatches.enabled", "false")
    .getOrCreate()

  // ------------------------------------------------------------- listener ---

  private final case class BatchTrace(
                                       batchId: Long,
                                       numInputRows: Long,
                                       startOffset: String,
                                       endOffset: String,
                                       latestOffset: String,
                                       minBehind: String,
                                       maxBehind: String,
                                       avgBehind: String) {
    override def toString: String =
      f"  batch $batchId%-3d in=$numInputRows%-4d\n" +
        f"      start  = $startOffset\n" +
        f"      end    = $endOffset\n" +
        f"      latest = $latestOffset\n" +
        f"      behindLatest: min=$minBehind max=$maxBehind avg=$avgBehind"
  }

  private final class Collector extends StreamingQueryListener {
    private val traces = ArrayBuffer.empty[BatchTrace]
    override def onQueryStarted(e: QueryStartedEvent): Unit = ()
    override def onQueryIdle(e: QueryIdleEvent): Unit = ()
    override def onQueryTerminated(e: QueryTerminatedEvent): Unit = ()
    override def onQueryProgress(e: QueryProgressEvent): Unit = {
      val p = e.progress
      p.sources.headOption.foreach { s =>
        val m = Option(s.metrics).map(_.asScala).getOrElse(collection.mutable.Map.empty[String, String])
        traces.synchronized {
          traces += BatchTrace(
            p.batchId, p.numInputRows,
            Option(s.startOffset).getOrElse("-"),
            Option(s.endOffset).getOrElse("-"),
            Option(s.latestOffset).getOrElse("-"),
            m.getOrElse("minOffsetsBehindLatest", "-"),
            m.getOrElse("maxOffsetsBehindLatest", "-"),
            m.getOrElse("avgOffsetsBehindLatest", "-"))
        }
      }
    }
    def snapshot(): Seq[BatchTrace] = traces.synchronized(traces.toList)
  }

  // --------------------------------------------------------------- query ---

  /**
   * @param startingOffsets what we ASK for. Whether it is honoured is the point.
   * @param failAtBatch     inject a failure when this batchId is reached.
   */
  private def runQuery(
                        startingOffsets: String,
                        failAtBatch: Option[Long],
                        awaitAllAvailable: Boolean): (Seq[BatchTrace], Seq[(Int, Long)]) = {

    val collector = new Collector
    spark.streams.addListener(collector)

    val received = collection.mutable.Map.empty[Int, Long]

    val src: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KafkaTestOps.Bootstrap)
      .option("subscribe", Topic)
      .option("startingOffsets", startingOffsets)
      .option("maxOffsetsPerTrigger", MaxOffsets)
      .load()
      .selectExpr("CAST(value AS STRING) AS value", "partition", "offset")

    val query = src.writeStream
      .foreachBatch { (df: DataFrame, batchId: Long) =>
        val byPart = df.groupBy("partition").count().collect()
          .map(r => r.getInt(0) -> r.getLong(1)).sortBy(_._1)
        val n = byPart.map(_._2).sum
        println(s"  [foreachBatch] batchId=$batchId rows=$n  " +
          byPart.map { case (p, c) => s"p$p=$c" }.mkString(" "))
        byPart.foreach { case (p, c) => received.update(p, received.getOrElse(p, 0L) + c) }

        if (failAtBatch.contains(batchId)) {
          println(s"  [foreachBatch] *** injecting failure on batchId=$batchId ***")
          throw new RuntimeException(s"simulated crash during batch $batchId")
        }
      }
      .option("checkpointLocation", Checkpoint)
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .start()

    try {
      if (awaitAllAvailable) {
        query.processAllAvailable()
        query.stop()
      } else {
        query.awaitTermination()
      }
    } catch {
      case t: Throwable =>
        val rc = rootCause(t)
        val isMetricsNpe = rc.isInstanceOf[NullPointerException] &&
          Option(rc.getMessage).exists(_.contains("Option.get()"))
        if (isMetricsNpe) {
          println("  [query] KafkaMicroBatchStream.metrics NPE during progress reporting.")
          println("          latestPartitionOffsets is only assigned by latestOffset(start, limit),")
          println("          i.e. during PLANNING -- which a replayed batch skips. metrics() then")
          println("          builds Some(null) and dereferences it. The BATCH ITSELF SUCCEEDED and")
          println("          its commit is already durable (commitLog.add runs inside runBatch,")
          println("          before finishTrigger). Only observability failed.")
        } else {
          println(s"  [query] terminated with: ${rc.getMessage}")
        }
    } finally {
      spark.streams.removeListener(collector)
    }

    (collector.snapshot(), received.toSeq.sortBy(_._1))
  }

  @tailrec
  private def rootCause(t: Throwable): Throwable =
    if (t.getCause == null || t.getCause == t) t else rootCause(t.getCause)

  // ---------------------------------------------------------------- check ---

  private val results = ArrayBuffer.empty[(String, Boolean)]

  private def check(label: String, cond: Boolean): Unit = {
    results += (label -> cond)
    println(s"  [${if (cond) "PASS" else "FAIL"}] $label")
  }

  private def summary(): Unit = {
    println("\n--- checks ---")
    results.foreach { case (l, ok) => println(s"  ${if (ok) "PASS" else "FAIL"}  $l") }
  }

  private def banner(s: String): Unit = println(s"\n${"=" * 78}\n$s\n${"=" * 78}")

  private def fmtMap(m: Map[Int, Long]): String =
    m.toSeq.sortBy(_._1).map { case (p, v) => s"p$p=$v" }.mkString(" ")

  // --------------------------------------------------------------- stages ---

  /**
   * Fresh topic, fresh checkpoint, 100 records per partition.
   * Run with startingOffsets=earliest and crash on batch 1.
   */
  private def stage1(): Unit = {
    banner("STAGE 1 -- plan is written ahead; crash leaves offsets/1 without commits/1")

    CheckpointInspector.deleteRecursively(Checkpoint)
    KafkaTestOps.resetTopic(Topic, InitialParts)
    KafkaTestOps.produceEvenly(Topic, PerPartition)
    KafkaTestOps.printEndOffsets("after seeding", Topic)

    println("\n-- running query: startingOffsets=earliest, maxOffsetsPerTrigger=" +
      s"$MaxOffsets, failing on batch 1 --")
    val (traces, _) = runQuery("earliest", failAtBatch = Some(1L), awaitAllAvailable = false)
    traces.foreach(println)

    CheckpointInspector.report(Checkpoint, "after simulated crash")

    check("offsets/0 exists and commits/0 exists (batch 0 completed)",
      CheckpointInspector.plannedEndOffsets(Checkpoint, 0).nonEmpty &&
        CheckpointInspector.commitExists(Checkpoint, 0))

    check("offsets/1 exists (plan written ahead of execution)",
      CheckpointInspector.plannedEndOffsets(Checkpoint, 1).nonEmpty)

    check("commits/1 ABSENT (batch planned, never finished)",
      !CheckpointInspector.commitExists(Checkpoint, 1))

    check("sources/0/0 holds the resolved initial offsets, all zero for 'earliest'",
      CheckpointInspector.sourceInitialOffsets(Checkpoint)
        .get(Topic).exists(_.values.forall(_ == 0L)))

    // Proportional split: 50 * (100/300) = 16.67 -> floor 16, three times.
    val b0 = CheckpointInspector.plannedEndOffsets(Checkpoint, 0).getOrElse(Topic, Map.empty)
    println(s"\n  batch 0 planned ends: ${b0.toSeq.sortBy(_._1).map { case (p, o) => s"p$p=$o" }.mkString(" ")}" +
      s"   (total ${b0.values.sum} records, cap $MaxOffsets)")
    check("batch 0 split the cap proportionally: 16 per partition, 48 total (not 50)",
      b0.values.toSet == Set(16L) && b0.values.sum == 48L)

    println("\nNEXT: run stage2. It produces MORE data first, so if the engine replanned " +
      "batch 1 the range would grow.")
    summary()
  }

  /**
   * Restart onto the uncommitted batch. Everything here is read from the
   * checkpoint and from foreachBatch, neither of which depends on progress
   * reporting -- which is what dies. Claims 2, 3, 4 land here.
   */
  private def stage2a(): Unit = {
    banner("STAGE 2a -- restart: the plan is binding, startingOffsets is ignored")

    val plannedB0 = CheckpointInspector.plannedEndOffsets(Checkpoint, 0).getOrElse(Topic, Map.empty)
    val plannedB1 = CheckpointInspector.plannedEndOffsets(Checkpoint, 1).getOrElse(Topic, Map.empty)
    require(plannedB1.nonEmpty, "offsets/1 not found -- run stage1 first")
    println("  offsets/0 (committed): " + fmtMap(plannedB0))
    println("  offsets/1 (planned, never committed): " + fmtMap(plannedB1))

    // Rows batch 1 must read on replay = its recorded end minus batch 0's end.
    val expectedRows = plannedB1.map { case (p, end) => p -> (end - plannedB0.getOrElse(p, 0L)) }
    println("  => replay must read exactly: " + fmtMap(expectedRows))

    KafkaTestOps.produceEvenly(Topic, PerPartition, startId = PerPartition)
    KafkaTestOps.printEndOffsets("after producing more", Topic)
    println("  ^ 'latest' is now far past the recorded plan. A replan would be visible.")

    println("\n-- restarting SAME checkpoint with startingOffsets=latest --")
    println("   Expect the query to die AFTER the batch, in progress reporting (known NPE).")
    val (_, received) = runQuery("latest", failAtBatch = None, awaitAllAvailable = false)

    check("replayed batch read EXACTLY the recorded range, not a replan against new data",
      received.toMap == expectedRows)

    check("offsets/1 unchanged on disk -- the plan was not rewritten",
      CheckpointInspector.plannedEndOffsets(Checkpoint, 1).getOrElse(Topic, Map.empty) == plannedB1)

    check("startingOffsets=latest IGNORED -- resumed from the checkpoint, did not skip ahead",
      received.map(_._2).sum == expectedRows.values.sum)

    check("sources/0/0 still the ORIGINAL zeros -- startingOffsets never re-resolved",
      CheckpointInspector.sourceInitialOffsets(Checkpoint)
        .get(Topic).exists(_.values.forall(_ == 0L)))

    check("commits/1 now present -- commit is durable before progress reporting runs",
      CheckpointInspector.commitExists(Checkpoint, 1))

    CheckpointInspector.report(Checkpoint, "after replaying batch 1")
    println("\nNEXT: stage2b. Batch 1 is committed, so every later batch is PLANNED, " +
      "latestPartitionOffsets is populated, and metrics work.")
    summary()
  }

  /** Drain normally. Claims 5 and 6 land here, once planning happens again. */
  private def stage2b(): Unit = {
    banner("STAGE 2b -- drain the backlog; lag comes from the driver, not from Kafka")

    require(CheckpointInspector.commitExists(Checkpoint, 1),
      "batch 1 not committed -- run stage2a first")

    val (traces, received) = runQuery("latest", failAtBatch = None, awaitAllAvailable = true)
    traces.foreach(println)

    check("batches after the replayed one report progress normally", traces.nonEmpty)

    check("driver reports lag: offsetsBehindLatest present on at least one batch",
      traces.exists(_.maxBehind != "-"))

    val allGroups = KafkaTestOps.allGroupIds()
    println(s"\n  consumer groups visible to the cluster: " +
      (if (allGroups.isEmpty) "NONE" else allGroups.mkString(", ")))
    check(s"no group with prefix '$SparkGroupPrefix' has even FORMED " +
      "(executors assign(), they do not subscribe())",
      !allGroups.exists(_.startsWith(SparkGroupPrefix)))

    val anyGroup = KafkaTestOps.anyGroupWithOffsetsFor(Topic)
    println(s"  groups holding committed offsets for $Topic: " +
      (if (anyGroup.isEmpty) "NONE" else anyGroup.keys.mkString(", ")))
    check(s"NO consumer group anywhere holds committed offsets for $Topic " +
      "-- group-based lag monitoring cannot work by construction",
      anyGroup.isEmpty)

    CheckpointInspector.report(Checkpoint, "after catch-up")
    println(s"\n  received this run: ${fmtMap(received.toMap)}")
    summary()
  }
  /** Grow the topic; new partitions must be read from earliest. */
  private def stage3(): Unit = {
    banner("STAGE 3 -- partitions added mid-life start from EARLIEST, not from startingOffsets")

    val before = KafkaTestOps.partitionCount(Topic)
    KafkaTestOps.printEndOffsets("before growing topic", Topic)
    KafkaTestOps.addPartitions(Topic, GrownParts)
    val newParts = (before until GrownParts).toList
    newParts.foreach(p => KafkaTestOps.produceTo(Topic, p, 20, startId = 900))
    KafkaTestOps.printEndOffsets("after growing topic", Topic)

    println("\n-- restarting SAME checkpoint with startingOffsets=latest --")
    val (traces, received) = runQuery("latest", failAtBatch = None, awaitAllAvailable = true)
    traces.foreach(println)

    val newPartRows = received.filter { case (p, _) => newParts.contains(p) }
    println(s"\n  rows received from NEW partitions: " +
      (if (newPartRows.isEmpty) "none" else newPartRows.map { case (p, c) => s"p$p=$c" }.mkString("  ")))

    check("records from newly added partitions were read despite startingOffsets=latest",
      newParts.forall(p => received.toMap.getOrElse(p, 0L) > 0L))

    check("all 20 records per new partition arrived (started at offset 0, not latest)",
      newParts.forall(p => received.toMap.getOrElse(p, 0L) == 20L))

    CheckpointInspector.report(Checkpoint, "after partition growth")
    summary()
  }

  def main(args: Array[String]): Unit = {
    val stage = args.headOption.getOrElse("stage1")
    try {
      stage match {
        case "stage1" => stage1()
        case "stage2a" => stage2a()
        case "stage2b" => stage2b()
        case "stage3" => stage3()
        case other    => println(s"unknown stage '$other' -- use stage1 | stage2a | stage2b | stage3")
      }
    }
  }
}