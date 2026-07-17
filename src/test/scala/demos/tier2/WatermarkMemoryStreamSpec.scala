package demos.tier2

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryListener}
import org.apache.spark.sql.streaming.StreamingQueryListener._

// VERSION NOTE (confirmed empirically on Spark 4.1.2):
//   Spark <= 3.5.x : org.apache.spark.sql.execution.streaming.MemoryStream
//   Spark 4.1.x    : org.apache.spark.sql.execution.streaming.runtime.MemoryStream
// The package was relocated to `.runtime` in the 4.x line. Compiling against 4.1.2
// with the old path fails with:
//   "object MemoryStream is not a member of package org.apache.spark.sql.execution.streaming"
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Deterministic watermark experiments using `MemoryStream`.
 *
 * -- Why MemoryStream ----------------------------------------------------------
 * The socket demo (WatermarkDemo) could only *illustrate* watermark behaviour:
 * with a socket source, the engine decides which records land in which
 * micro-batch, so batch boundaries were a race between the sender's sleeps and
 * the trigger clock. Any observation was contingent on that race going our way.
 *
 * `MemoryStream[T]` inverts the control relationship. It is a streaming source
 * backed by an in-memory buffer that YOU drive:
 *
 *   - `addData(...)`            appends a new batch of records and bumps the offset
 *   - `query.processAllAvailable()`  blocks until everything added so far has been
 *                              processed and committed to the sink
 *
 * Together these give *deterministic batch boundaries*: one `addData` +
 * `processAllAvailable` pair == a known, reproducible unit of engine progress.
 * That is what turns "I watched it happen once" into "I can assert this in CI",
 * which is why MemoryStream is the foundation for testing streaming pipelines
 * (roadmap Tier 5), not just for this one experiment.
 *
 * Because it is an offset-addressable, replayable in-memory buffer, MemoryStream
 * also satisfies the replayability properties from Tier 1 Concept 3 - unlike the
 * socket source, which is at-most-once by construction.
 *
 * -- New APIs introduced here --------------------------------------------------
 *   MemoryStream[Int]        - typed in-memory streaming source. Needs an implicit
 *                              Encoder[Int] (from `import spark.implicits._`) and
 *                              an implicit SQLContext.
 *   .addData(a, b, ...)      - append records; returns the new Offset.
 *   query.processAllAvailable() - block until all added data is processed.
 *   timestamp_seconds(col)   - Spark SQL fn: Long seconds since epoch -> Timestamp.
 *                              (Modelled on Spark's own StreamingAggregationSuite,
 *                              which uses exactly this for watermark tests.)
 *   format("memory")         - MemorySink: results land in a queryable temp table,
 *                              readable via spark.table(queryName).
 *
 * -- Convention for these tests ------------------------------------------------
 * The Int flowing through the stream IS THE EVENT TIME IN SECONDS.
 * `timestamp_seconds` converts it. Watermark = 10s, tumbling window = 5s.
 * So an event with value 12 lands in window [10, 15).
 */
class WatermarkMemoryStreamSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private val WatermarkThreshold = "10 seconds"
  private val WindowDuration     = "5 seconds"

  private lazy val spark: SparkSession = {
    val s = SparkSession.builder()
      .appName("WatermarkMemoryStreamSpec")
      .master("local[2]")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.shuffle.partitions", "1") // determinism + speed
      .getOrCreate()
    s.sparkContext.setLogLevel("ERROR")
    s
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  // -- Per-batch trace captured from the listener ------------------------------
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
        f"total=$numRowsTotal%-2d upd=$numRowsUpdated%-2d removed=$numRowsRemoved%-2d " +
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

  /**
   * Builds the standard watermarked windowed-count query over a MemoryStream,
   * writing to a MemorySink so results are queryable as a table.
   *
   * Returns (input stream, running query, listener trace collector).
   */
  private def startWatermarkedQuery(queryName: String)
  : (MemoryStream[Int], StreamingQuery, TraceCollector) = {

    implicit val sqlCtx: org.apache.spark.sql.SQLContext = spark.sqlContext
    import spark.implicits._

    val input = MemoryStream[Int]

    // The Int IS the event time in seconds. timestamp_seconds does the conversion.
    val counts = input.toDF()
      .withColumn("eventTime", timestamp_seconds(col("value")))
      .withWatermark("eventTime", WatermarkThreshold)
      .groupBy(window(col("eventTime"), WindowDuration).as("w"))
      .agg(count("*").as("cnt"))
      // Project the window start back to a plain Long (seconds) so assertions read
      // in the same units we fed in.
      .select(
        col("w.start").cast("long").as("winStart"),
        col("w.end").cast("long").as("winEnd"),
        col("cnt")
      )

    val collector = new TraceCollector
    spark.streams.addListener(collector)

    val query = counts.writeStream
      .format("memory")
      .queryName(queryName)
      .outputMode("append") // watermark required; window emits once, finalized
      .start()

    (input, query, collector)
  }

  /** Current contents of the MemorySink as (winStart, cnt) pairs. */
  private def sinkRows(queryName: String): Set[(Long, Long)] = {
    import spark.implicits._
    spark.table(queryName)
      .select(col("winStart"), col("cnt"))
      .as[(Long, Long)]
      .collect()
      .toSet
  }

  private def printTrace(label: String, collector: TraceCollector): Unit = {
    println(s"\n--- $label ---")
    collector.snapshot().foreach(t => println("  " + t))
  }

  // ===========================================================================
  // EXPERIMENT 1 - the eviction boundary: strict `>` or non-strict `>=`?
  //
  // This is the one thing the socket demo could NOT settle: every comparison
  // there was strictly greater either way (wm 09:11 vs end 09:05 / 09:10).
  //
  // Construction: make the INHERITED watermark land EXACTLY on a window's end.
  //   - Target window [10, 15). Its end is 15.
  //   - watermark = maxEventTime - 10s, so we need maxEventTime = 25 for wm = 15.
  //
  //   step 1: addData(10, 25)
  //             -> event 10 opens window [10,15); event 25 opens [25,30)
  //             -> at END of this batch: watermark = 25 - 10 = 15
  //   step 2: addData(25)          <- does NOT move max (still 25), so the
  //             INHERITED watermark for this batch is EXACTLY 15.
  //             Eviction check for [10,15): is  wm(15) > end(15)  ... or  >= ?
  //               strict `>`  : 15 > 15  is FALSE -> window SURVIVES, no output
  //               non-strict  : 15 >= 15 is TRUE  -> window EVICTED, row emitted
  //   step 3: addData(30)          -> max=30, wm=20 -> 20 > 15 either way, so the
  //             window MUST emit eventually. This is the control: it proves the
  //             window was capable of emitting and step 2 wasn't a false negative.
  //
  // HYPOTHESIS (from the Spark programming guide's phrasing, which states, state is
  // retained until `(max event time - late threshold) > T`): STRICT `>`, so window
  // [10,15) should NOT appear after step 2.
  // If this assertion fails, the real rule is `>=` and the .md must be corrected.
  // ===========================================================================
  test("eviction boundary: is it `watermark > windowEnd` (strict) or `>=`?") {
    val qName = "wm_boundary"
    val (input, query, collector) = startWatermarkedQuery(qName)

    try {
      // step 1 - seed target window [10,15) and push max to exactly 25
      input.addData(10, 25)
      query.processAllAvailable()
      val afterStep1 = sinkRows(qName)
      println(s"\n[boundary] after step 1 (addData 10, 25): sink = $afterStep1")

      // step 2 - a DATA batch whose inherited watermark is EXACTLY 15 (max unchanged)
      input.addData(25)
      query.processAllAvailable()
      val afterStep2 = sinkRows(qName)
      println(s"[boundary] after step 2 (addData 25; inherited wm == 15 == window end): " +
        s"sink = $afterStep2")

      printTrace("boundary trace (steps 1-2)", collector)

      val windowTenEmittedAtExactBoundary = afterStep2.exists(_._1 == 10L)

      println(
        s"""
           |[boundary] ===== VERDICT =====
           |[boundary] window [10,15) emitted while inherited watermark == 15 (its exact end)?
           |[boundary]   observed: $windowTenEmittedAtExactBoundary
           |[boundary]   false => rule is STRICT  ( evict when watermark >  windowEnd )
           |[boundary]   true  => rule is NON-STRICT ( evict when watermark >= windowEnd )
           |""".stripMargin)

      // step 3 - control: push watermark clearly past the end; window MUST emit now.
      input.addData(30)
      query.processAllAvailable()
      val afterStep3 = sinkRows(qName)
      println(s"[boundary] after step 3 (addData 30; wm advances to 20 > 15): sink = $afterStep3")
      printTrace("boundary trace (all steps)", collector)

      withClue("CONTROL FAILED: window [10,15) never emitted even once the watermark " +
        "was clearly past its end. The experiment setup is wrong, not the engine. ") {
        afterStep3.exists(_._1 == 10L) shouldBe true
      }

      // The actual hypothesis. If this fails, the boundary is `>=`, not `>`.
      withClue("Window [10,15) WAS emitted while the inherited watermark equalled its end " +
        "exactly (15). That means eviction fires on `watermark >= windowEnd`, NOT the " +
        "strict `>` implied by the Spark programming guide. Update 04-watermarks-part1.md. ") {
        windowTenEmittedAtExactBoundary shouldBe false
      }

    } finally {
      query.stop()
      spark.streams.removeListener(collector)
    }
  }

  // ===========================================================================
  // EXPERIMENT 2 - the watermark is NOT an input filter.
  //
  // Deterministic replay of Simulation 2 from 04-watermarks-part1.md.
  // A record whose event time is BELOW the inherited watermark is STILL aggregated,
  // provided its window is still in the state store. A record whose window was
  // never opened / already evicted is dropped.
  //
  //   batch 1: 103, 107      -> windows [100,105) & [105,110); max=107, wm_out=97
  //   batch 2: 114, 102      -> 102 (>97) joins [100,105); max=114, wm_out=104
  //   batch 3: 121, 103, 90  -> inherited wm = 104
  //                              * 103 is BELOW 104 but [100,105) is STILL OPEN
  //                                  => ACCEPTED (this is the whole point)
  //                              * 90 targets [90,95): never opened, far below wm
  //                                  => DROPPED
  //                              * 121 opens [120,125)
  //                              max=121, wm_out=111
  //   batch 4: 122           -> inherited wm = 111 -> evicts [100,105) and [105,110)
  //
  // If the watermark were an input filter, event 103 in batch 3 would have been
  // discarded and window [100,105) would emit count 2. It emits count 3.
  // ===========================================================================
  test("watermark is not an input filter: a below-watermark record still updates an open window") {
    val qName = "wm_not_a_filter"
    val (input, query, collector) = startWatermarkedQuery(qName)

    try {
      input.addData(103, 107)      // batch 1
      query.processAllAvailable()

      input.addData(114, 102)      // batch 2 - 102 is late but above wm(97)
      query.processAllAvailable()

      input.addData(121, 103, 90)  // batch 3 - 103 is BELOW wm(104); 90 has no open window
      query.processAllAvailable()
      val traceAfterB3 = collector.snapshot()

      input.addData(122)           // batch 4 - inherited wm(111) evicts the two windows
      query.processAllAvailable()

      val rows = sinkRows(qName)
      printTrace("not-a-filter trace", collector)
      println(s"\n[filter] final sink = $rows")

      // Window [100,105) must contain events 103 (b1), 102 (b2) AND 103 (b3) = 3.
      // A filtering model would give 2.
      withClue("Window [100,105) did not emit count 3. If it emitted 2, the below-watermark " +
        "record (103 in batch 3) was filtered out at input - which would contradict the " +
        "not-a-filter model. ") {
        rows should contain((100L, 3L))
      }

      // Window [105,110) holds only event 107.
      rows should contain((105L, 1L))

      // Event 90 must have been dropped: some batch reports a non-zero drop count.
      // (Per the Spark guide, numRowsDroppedByWatermark is a zero-vs-non-zero signal,
      //  not an exact late-row tally - so we only assert non-zero.)
      val droppedSomewhere = traceAfterB3.exists(_.numRowsDroppedByWatermark > 0)
      withClue("Expected the far-late record (90, whose window was never open) to be " +
        "counted in numRowsDroppedByWatermark. ") {
        droppedSomewhere shouldBe true
      }

    } finally {
      query.stop()
      spark.streams.removeListener(collector)
    }
  }

  // ===========================================================================
  // EXPERIMENT 3 - eviction uses the INHERITED watermark (the one-trigger lag).
  //
  // The watermark computed at the end of batch N is NOT used to evict in batch N.
  // It is inherited by batch N+1, which does the eviction. Consequence: the batch
  // in which rows are REMOVED from state is strictly LATER than the batch whose
  // data first pushed the watermark past the window's end.
  //
  // Assertion: find the batch with numRowsRemoved > 0. The watermark reported for
  // THAT batch must already be past the evicted window's end - i.e. the removal
  // batch inherited a watermark computed earlier. Meanwhile the batch that first
  // reported that watermark value carried numRowsRemoved == 0.
  // ===========================================================================
  test("eviction happens one trigger after the watermark advances past the window end") {
    val qName = "wm_ordering"
    val (input, query, collector) = startWatermarkedQuery(qName)

    try {
      input.addData(103, 107)   // opens [100,105), [105,110); wm_out = 97
      query.processAllAvailable()

      input.addData(121)        // max=121 -> wm_out = 111 (now PAST end 105 and 110)
      query.processAllAvailable()
      val afterAdvance = sinkRows(qName)

      input.addData(122)        // this batch INHERITS wm=111 and should do the eviction
      query.processAllAvailable()
      val afterInherit = sinkRows(qName)

      printTrace("ordering trace", collector)
      println(s"\n[ordering] sink right after the watermark advanced past 105/110: $afterAdvance")
      println(s"[ordering] sink after the NEXT batch inherited that watermark:     $afterInherit")

      val trace = collector.snapshot()
      val removalBatches = trace.filter(_.numRowsRemoved > 0).map(_.batchId)
      println(s"[ordering] batches that removed state rows: $removalBatches")

      // The windows must eventually emit.
      afterInherit should contain((100L, 1L))
      afterInherit should contain((105L, 1L))

      // The key ordering claim: the batch that *computed* the advancing watermark
      // (the one fed 121) did not itself evict. Eviction shows up in a LATER batch.
      // We assert this structurally: no batch both (a) is the first to report a
      // watermark past 110 and (b) removes rows in that same batch.
      withClue("Expected at least one batch to have removed state rows. ") {
        removalBatches should not be empty
      }

      // Note: this is deliberately a *structural* check rather than an exact batchId
      // assertion, because Spark may interleave no-data batches (inputRows == 0) that
      // also inherit the watermark and perform the eviction. Either way, the removal
      // must NOT occur in the same batch that first saw event 121.
      val batchThatSaw121 = trace.find(t => t.numInputRows == 1 && t.numRowsUpdated > 0)
      batchThatSaw121.foreach { b =>
        withClue(s"Batch ${b.batchId} both ingested the watermark-advancing record AND " +
          s"removed state rows in the same batch. That would contradict the " +
          s"'advance at end of batch N, evict in batch N+1' ordering. ") {
          b.numRowsRemoved shouldBe 0L
        }
      }

    } finally {
      query.stop()
      spark.streams.removeListener(collector)
    }
  }
}