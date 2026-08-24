package demos.tier3

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
 * Tier 3 - Concept 1 (part 1): what `maxOffsetsPerTrigger` actually plans.
 *
 * A probe, not a demo: no restarts, no crash, no checkpoint semantics. It asks
 * one question -- given a known per-partition backlog and a known cap, what
 * end offsets does the driver write to offsets/N? -- and answers it by reading
 * the offset log after the fact.
 *
 *   sbt "runMain demos.tier3.KafkaRateLimitProbe"
 *
 * Each case recreates its topic and deletes its checkpoint, so runs are
 * independent and repeatable. Trigger.AvailableNow drains a FIXED backlog in
 * bounded batches and terminates -- the right shape here because we want the
 * ceiling frozen at start (nothing is being produced concurrently) and we want
 * the run to end on its own.
 *
 * ---------------------------------------------------------------------------
 * CASE A -- proportional allocation, with exact predicted numbers
 *
 * Backlog 9000 / 900 / 10 on partitions 0 / 1 / 2; cap 1000. Total 9910, so
 * this batch can take ~10% of what is waiting, and each partition gets 10% OF
 * ITS OWN backlog (in other words, each partition will get same sare of the cap as
 * the share of backlog they hold):
 *
 *     p0: 1000 * 9000/9910 = 908.17 -> floor -> 908
 *     p1: 1000 *  900/9910 =  90.82 -> floor ->  90
 *     p2: 1000 *   10/9910 =   1.01 -> floor ->   1
 *
 * The topic is fresh, so offsets start at 0 and the planned END offsets ARE
 * those counts. PREDICTION: offsets/0 == {p0:908, p1:90, p2:1}, 999 records --
 * not 1000, and not 333 each.
 *
 * Second prediction, the one that justifies proportional allocation: because
 * every partition surrenders the same FRACTION per batch, all three should
 * drain in roughly the same number of batches rather than the small ones
 * finishing early and wasting budget.
 *
 * Note how narrowly p2 survives: at a backlog of 9 instead of 10 its share
 * would be 0.908, below 1, and only the ceil-branch starvation guard would
 * keep it moving at all.
 *
 * ---------------------------------------------------------------------------
 * CASE B -- the cap is not a hard cap
 *
 * 20 partitions, 3 records each (60 total), cap 5. Every partition computes
 * 5 * 3/60 = 0.25, which is below 1, so the starvation guard rounds each UP to
 * 1. PREDICTION: every batch reads 20 records -- FOUR TIMES the cap -- and
 * offsets/0 is 1 on every partition.
 *
 * The guard only ever rounds up, so the overshoot is bounded by the number of
 * partitions holding unread data. Which is why this is invisible in steady
 * state (few partitions have a backlog at any instant) and appears only while
 * draining one -- that is, when the pipeline is already degraded.
 */
object KafkaRateLimitProbe {

  private val TopicA      = "t3c1-ratelimit-a"
  private val TopicB      = "t3c1-ratelimit-b"
  private val CkptA       = "/tmp/spark-streaming/tier3/c1/ratelimit-a/_checkpoint"
  private val CkptB       = "/tmp/spark-streaming/tier3/c1/ratelimit-b/_checkpoint"

  private lazy val spark: SparkSession = SparkSession.builder()
    .appName("KafkaRateLimitProbe")
    .master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()

  private val results = ArrayBuffer.empty[(String, Boolean)]

  private def check(label: String, cond: Boolean): Unit = {
    results += (label -> cond)
    println(s"  [${if (cond) "PASS" else "FAIL"}] $label")
  }

  private def banner(s: String): Unit = println(s"\n${"=" * 78}\n$s\n${"=" * 78}")

  private def fmt(m: Map[Int, Long]): String =
    m.toSeq.sortBy(_._1).map { case (p, v) => s"p$p=$v" }.mkString(" ")

  /**
   * Drain `topic` with the given cap and return, per batch id, the PLANNED end
   * offsets read back out of offsets/N. The offset log is the artifact we care
   * about: it is the plan, written before any record moved.
   */
  private def drain(topic: String, checkpoint: String, cap: Int): Seq[(Long, Map[Int, Long])] = {
    val df: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KafkaTestOps.Bootstrap)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", cap)
      .load()
      .selectExpr("partition", "offset")

    val query = df.writeStream
      .foreachBatch { (b: DataFrame, batchId: Long) =>
        val n = b.count()
        println(f"  [foreachBatch] batchId=$batchId%-3d rows=$n%-5d (cap $cap)")
      }
      .option("checkpointLocation", checkpoint)
      .trigger(Trigger.AvailableNow())
      .start()

    query.awaitTermination()

    var b = 0L
    val out = ArrayBuffer.empty[(Long, Map[Int, Long])]
    var more = true
    while (more) {
      val ends = CheckpointInspector.plannedEndOffsets(checkpoint, b).getOrElse(topic, Map.empty)
      if (ends.isEmpty) more = false else { out += (b -> ends); b += 1 }
    }
    out.toSeq
  }

  /** Per-batch record counts, derived from consecutive planned end offsets. */
  private def perBatchCounts(plans: Seq[(Long, Map[Int, Long])]): Seq[(Long, Map[Int, Long], Long)] = {
    var prev = Map.empty[Int, Long].withDefaultValue(0L)
    plans.map { case (b, ends) =>
      val delta = ends.map { case (p, e) => p -> (e - prev(p)) }
      prev = ends.withDefaultValue(0L)
      (b, delta, delta.values.sum)
    }
  }

  private def printPlans(plans: Seq[(Long, Map[Int, Long])], cap: Int): Unit = {
    println(f"\n  ${"batch"}%-6s ${"records"}%-9s ${"vs cap"}%-8s planned end offsets")
    perBatchCounts(plans).zip(plans).foreach { case ((b, delta, total), (_, ends)) =>
      val ratio = f"${total.toDouble / cap}%.2fx"
      println(f"  $b%-6d $total%-9d $ratio%-8s ${fmt(ends)}")
    }
  }

  // ------------------------------------------------------------------ A ---

  private def caseA(): Unit = {
    banner("CASE A -- proportional split: 9000/900/10 backlog, cap 1000")

    CheckpointInspector.deleteRecursively(CkptA)
    KafkaTestOps.resetTopic(TopicA, 3)
    KafkaTestOps.produceTo(TopicA, 0, 9000)
    KafkaTestOps.produceTo(TopicA, 1, 900)
    KafkaTestOps.produceTo(TopicA, 2, 10)
    KafkaTestOps.printEndOffsets("backlog", TopicA)

    println("\n  predicted offsets/0: p0=908 p1=90 p2=1  (999 records, not 1000)")

    val plans = drain(TopicA, CkptA, cap = 1000)
    printPlans(plans, cap = 1000)

    val b0 = plans.head._2
    check("offsets/0 == {p0:908, p1:90, p2:1} -- proportional, floored",
      b0 == Map(0 -> 908L, 1 -> 90L, 2 -> 1L))

    check("batch 0 planned 999 records, under the cap of 1000",
      b0.values.sum == 999L)

    // Batches each partition needed to finish: the first batch whose planned
    // end reaches that partition's full backlog.
    val backlog = Map(0 -> 9000L, 1 -> 900L, 2 -> 10L)
    val finishedAt = backlog.map { case (p, total) =>
      p -> plans.find { case (_, ends) => ends.getOrElse(p, 0L) >= total }.map(_._1).getOrElse(-1L)
    }
    println(s"\n  batch at which each partition finished draining: ${fmt(finishedAt)}")
    check("all partitions drained within 1 batch of each other (proportional keeps them aligned)",
      finishedAt.values.max - finishedAt.values.min <= 1L)

    check("every batch stayed at or under the cap (no partition count pressure here)",
      perBatchCounts(plans).forall(_._3 <= 1000L))
  }

  // ------------------------------------------------------------------ B ---

  private def caseB(): Unit = {
    banner("CASE B -- the cap is not a hard cap: 20 partitions x 3 records, cap 5")

    CheckpointInspector.deleteRecursively(CkptB)
    KafkaTestOps.resetTopic(TopicB, 20)
    KafkaTestOps.produceEvenly(TopicB, 3)
    KafkaTestOps.printEndOffsets("backlog", TopicB)

    println("\n  predicted: 5 * 3/60 = 0.25 per partition -> below 1 -> rounded UP to 1")
    println("  predicted: every batch reads 20 records = 4x the cap of 5")

    val plans = drain(TopicB, CkptB, cap = 5)
    printPlans(plans, cap = 5)

    val b0 = plans.head._2
    check("offsets/0 == 1 on every partition (starvation guard rounded each share up)",
      b0.values.toSet == Set(1L) && b0.size == 20)

    val counts = perBatchCounts(plans)
    check("batch 0 read 20 records against a cap of 5 -- a 4x overshoot",
      counts.head._3 == 20L)

    check("EVERY batch overshot the cap",
      counts.forall(_._3 > 5L))

    check("overshoot is bounded by the partition count, not unbounded",
      counts.forall(_._3 <= 20L))

    println("\n  => The effective floor on batch size is the number of partitions holding")
    println("     unread data. No value of maxOffsetsPerTrigger can go below it.")
  }

  def main(args: Array[String]): Unit = {
    try {
      //caseA()
      caseB()
    }
  }
}