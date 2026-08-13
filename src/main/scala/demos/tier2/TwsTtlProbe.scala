package demos.tier2

import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.functions.{col, timestamp_seconds}
import org.apache.spark.sql.{Encoders, SQLContext, SparkSession}
import org.apache.spark.sql.streaming._

import java.time.Duration
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/**
 * PROBE (throwaway -- not the Concept 9 demo).
 *
 * Three open questions that decide the design of the real processor. Each is
 * measured, not reasoned about.
 *
 *   P1  TTL on ListState: PER-ELEMENT or PER-VARIABLE?
 *       Requirement 7 wants "readings older than an hour are gone" -- a sliding
 *       window. If TTL applies to the whole variable, the entire buffer expires
 *       together and we would be rebuilding from nothing instead of sliding.
 *       This changes how the readings buffer must be built.
 *
 *   P2  What does the TTL duration measure FROM, and against WHICH clock?
 *       Candidates: last write / last read / creation. Clock: processing time,
 *       event time, or follows TimeMode. Decides whether a 1h TTL on a device
 *       that stops reporting expires 1h after its last beat (what we want) or
 *       never/immediately.
 *
 *   P3  Does a sealed-trait ADT encode as a state variable?
 *       PREDICTION: NO -- same Spark SQL encoders as C8, where a sealed trait
 *       failed at compile time ("Primitive types (Int, String, etc) and Product
 *       types (case classes) are supported"). If it compiles, my model is wrong.
 *       P3 is COMMENTED OUT because a failure here is a COMPILE error, which
 *       would stop P1/P2 from running. Uncomment it on its own.
 *
 * Deliberately ProcessingTime mode with real sleeps and SHORT ttls (5s), because
 * that is the only way to observe expiry without guessing at event-time
 * semantics. P2 tells us whether an event-time rewrite is even needed.
 */
object TwsTtlProbe {

  final case class Reading(deviceId: String, t: Int, value: Int)
  final case class Probe(deviceId: String, note: String)

  private val TtlSeconds = 5L

  private val spark: SparkSession = SparkSession.builder()
    .appName("TwsTtlProbe")
    .master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.sql.streaming.noDataMicroBatches.enabled", "false")
    // transformWithState requires the RocksDB state store provider.
    .config("spark.sql.streaming.stateStore.providerClass",
      "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
    .getOrCreate()

  implicit val sqlCtx: SQLContext = spark.sqlContext
  import spark.implicits._

  // ==========================================================================
  // P1 + P2 processor. Appends every incoming reading to a TTL'd ListState and
  // reports what is still visible, so we can watch elements leave one at a time
  // (per-element) or all at once (per-variable).
  // ==========================================================================
  class TtlListProcessor extends StatefulProcessor[String, Reading, Probe] {

    @transient private var _readings: ListState[Reading] = _
    @transient private var _beats: ValueState[Long] = _   // no TTL: control variable

    override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
      _readings = getHandle.getListState[Reading](
        "readings", Encoders.product[Reading], TTLConfig(Duration.ofSeconds(TtlSeconds)))
      // Control: same key, NO ttl. If this survives while readings expire, TTL is
      // per-VARIABLE (as declared), not per-KEY.
      _beats = getHandle.getValueState[Long](
        "beats", Encoders.scalaLong, TTLConfig.NONE)
    }

    override def handleInputRows(
                                  key: String,
                                  inputRows: Iterator[Reading],
                                  timerValues: TimerValues): Iterator[Probe] = {

      val incoming = inputRows.toList
      incoming.foreach(r => _readings.appendValue(r))

      val n = if (_beats.exists()) _beats.get() else 0L
      _beats.update(n + incoming.size)

      // Read back what is STILL THERE. This is the measurement.
      val alive = Try(_readings.get().toList).getOrElse(Nil)
      val aliveTs = alive.map(_.t).sorted.mkString(",")

      Iterator(Probe(key,
        f"added=[${incoming.map(_.t).mkString(",")}%s] " +
          f"aliveNow=${alive.size}%-2d ts=[$aliveTs%s] beatsEver=${n + incoming.size}"))
    }

    override def close(): Unit = ()
  }

  // ==========================================================================
  // P3 -- COMMENTED OUT: a failure here is a COMPILE error and would block P1/P2.
  // Uncomment this block ALONE to test it.
  // ==========================================================================

  /*sealed trait DevStatus
  case class Active(lastSeen: Int)                  extends DevStatus
  case class Down(lastSeen: Int, deemedDownAt: Int) extends DevStatus

  class AdtProcessor extends StatefulProcessor[String, Reading, Probe] {
    @transient private var _status: ValueState[DevStatus] = _

    override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
      // Does an Encoder[DevStatus] resolve? C8 says no -- sum type, not a Product.
      _status = getHandle.getValueState[DevStatus](
        "status", Encoders.product[DevStatus], TTLConfig.NONE)
    }

    override def handleInputRows(
        key: String, inputRows: Iterator[Reading], timerValues: TimerValues): Iterator[Probe] = {
      val maxT = inputRows.map(_.t).max
      val next: DevStatus = if (_status.exists()) {
        _status.get() match {
          case Active(ls)  => Down(ls, ls + 90)
          case Down(ls, _) => Active(math.max(ls, maxT))
        }
      } else Active(maxT)
      _status.update(next)
      Iterator(Probe(key, next.toString))
    }
    override def close(): Unit = ()
  }
   */


  private def run(): Unit = {
    val input = MemoryStream[Reading]

    val out = input.toDS()
      .groupByKey(_.deviceId)
      .transformWithState(
        new TtlListProcessor(),
        TimeMode.ProcessingTime(),
        OutputMode.Update())

    val q = out.writeStream
      .format("memory")
      .queryName("tws_ttl_probe")
      .outputMode("update")
      .start()

    def show(label: String): Unit = {
      println(s"\n--- $label ---")
      spark.read.table("tws_ttl_probe").collect().foreach(r => println("  " + r.mkString(" | ")))
    }

    try {
      // t=0: three readings land together.
      input.addData(Reading("D1", 1, 10), Reading("D1", 2, 20), Reading("D1", 3, 30))
      q.processAllAvailable()
      show("b0  added 1,2,3 -- expect aliveNow=3")

      // ~3s later: still inside the 5s TTL. One more reading.
      Thread.sleep(3000)
      input.addData(Reading("D1", 4, 40))
      q.processAllAvailable()
      show("b1  +3s, added 4 -- expect aliveNow=4 (nothing expired yet)")

      // ~7s after b0, ~4s after b1. THE DECIDING BATCH:
      //   PER-ELEMENT  -> 1,2,3 gone (7s old), 4 alive (4s old), plus 5  => aliveNow=2 ts=[4,5]
      //   PER-VARIABLE -> whole list gone, only 5                        => aliveNow=1 ts=[5]
      //   (if TTL refreshes on WRITE, the b1 append may have reset the whole
      //    variable's clock -> aliveNow=5, which answers P2)
      Thread.sleep(4000)
      input.addData(Reading("D1", 5, 50))
      q.processAllAvailable()
      show("b2  +7s from b0 -- P1 DECIDES HERE")

      // ~13s after b0. Everything from b0/b1 is well past TTL.
      Thread.sleep(6000)
      input.addData(Reading("D1", 6, 60))
      q.processAllAvailable()
      show("b3  +13s from b0 -- expect only recent elements")

      println(
        s"""
           |=================== HOW TO READ THIS ===================
           | TTL = ${TtlSeconds}s, TimeMode.ProcessingTime, real sleeps.
           |
           | P1 -- at b2 (7s after b0, 4s after b1):
           |    aliveNow=2 ts=[4,5]  -> TTL is PER-ELEMENT (sliding window; what
           |                            requirement 7 wants -- buffer works as-is)
           |    aliveNow=1 ts=[5]    -> TTL is PER-VARIABLE (whole list expires
           |                            together; the readings buffer needs a
           |                            different design)
           |    aliveNow=5           -> the b1 append RESET the variable's TTL
           |                            clock -> TTL measures from LAST WRITE to
           |                            the VARIABLE (answers P2 too)
           |
           | P2 -- compare b2 and b3. If elements leave exactly TtlSeconds after
           |    the batch that appended them, TTL measures from ELEMENT WRITE.
           |    If nothing ever expires while we keep appending, it measures from
           |    last write to the VARIABLE.
           |
           | CONTROL -- beatsEver must keep climbing (1..6). _beats has TTLConfig
           |    .NONE, so if it survives while readings expire, TTL is scoped to
           |    the VARIABLE as declared, not to the key.
           |
           | P3 -- separate run; see the commented block above.
           |========================================================
           |""".stripMargin)
    } finally {
      q.stop()
    }
  }

  def main(args: Array[String]): Unit = {
    spark.sparkContext.setLogLevel("ERROR")
    println("\n=== transformWithState TTL probe: Spark " + spark.version + " ===")
    Try(run()) match {
      case Success(_) => println("\n=== probe completed ===")
      case Failure(e) =>
        println(s"\n[FAIL] ${e.getClass.getName}")
        println("       " + Option(e.getMessage).getOrElse("(no message)")
          .linesIterator.take(8).mkString("\n       "))
    }
    //spark.stop()
  }
}