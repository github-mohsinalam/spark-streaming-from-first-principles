package demos.tier2

import java.io.PrintStream
import java.net.ServerSocket

/**
 * Socket server that feeds the two watermark simulations from
 * notes/tier-2/03-watermarks-part1.md into `WatermarkDemo`.
 *
 *   - Events are `key,epochMillis` lines. The long is epoch MILLISECONDS, fed
 *     to `new Timestamp(long)` on the Spark side, which per the java.sql.Timestamp
 *     Javadoc interprets its argument as "milliseconds since January 1, 1970,
 *     00:00:00 GMT". Source: java.sql.Timestamp(long) Javadoc (JDK 17).
 *   - SCALE IS COMPRESSED TO SECONDS. The .md simulations use a 10-min watermark
 *     and 5-min windows; here we use a 10-SECOND watermark and 5-SECOND windows.
 *     The mechanics (watermark = maxEventTime - threshold; evict when
 *     watermark > windowEnd) are unit-invariant. Event times are chosen so that,
 *     divided by 1000, they read as seconds-past-a-small-origin and line up with
 *     the .md batches. We anchor the .md "09:00" to epoch second 540 (=540000 ms)
 *     purely so log timestamps are compact; the origin is arbitrary.
 *
 *   .md time  ->  epoch seconds  ->  epochMillis (what we send)
 *   09:00     ->  540            ->  540000
 *   09:01     ->  541            ->  541000
 *   09:02     ->  542            ->  542000
 *   09:03     ->  543            ->  543000
 *   09:04     ->  544            ->  544000  (not sent; = a watermark value)
 *   09:07     ->  547            ->  547000
 *   09:14     ->  554            ->  554000
 *   09:21     ->  561            ->  561000
 *   08:50     ->  530            ->  530000
 *   09:22     ->  562            ->  562000
 *   10:02..10:17 similarly (see sim1 below)
 *
 * BATCH ALIGNMENT (honest caveat): a socket source + ProcessingTime trigger does
 * NOT give exact control over which records land in which micro-batch. We PACE
 * the sender (sleep > trigger interval between simulation-batches) so each group
 * is *expected* to be consumed by its own trigger. This is best-effort. The
 * listener in WatermarkDemo prints input-rows-per-batch so you can VERIFY the
 * alignment held; if a group bled across triggers, treat that run as void and
 * rely on the MemoryStream version (which gives exact batch control) as the
 * authoritative check.
 *
 * RUN ORDER: start THIS object first (it blocks on accept()), then start
 * WatermarkDemo so Spark's socket source dials in.
 */
object WatermarkSocketDataSender {

  // Must exceed WatermarkDemo's trigger interval so each group is its own batch.
  private val BatchGapMillis = 4000L
  private val Port           = 12345

  /** One simulation-batch: a label (for console) and its events. */
  private case class SimBatch(label: String, events: Seq[(String, Long)])

  /**
   * Simulation 1 - the simple, in-order case (see .md).
   * Compact origin: .md "10:00" -> epoch second 600 (=600000 ms).
   *   10:02->602000  10:04->604000  10:07->607000  10:09->609000
   *   10:16->616000  10:17->617000
   * Windows are 5s: [600000,605000), [605000,610000), [615000,620000).
   * Expected: [600000,605000) {P,Q}=2 is emitted one trigger AFTER the watermark
   * (maxEventTime-10000) first exceeds its end 605000 — i.e. after H-equivalent
   * pushes max to 616000 (wm 606000 > 605000) at end of batch3, evicted in batch4.
   */
  private val sim1: Seq[SimBatch] = Seq(
    SimBatch("batch1", Seq("P" -> 602000L, "Q" -> 604000L)),          // 10:02, 10:04
    SimBatch("batch2", Seq("R" -> 607000L, "S" -> 609000L)),          // 10:07, 10:09
    SimBatch("batch3", Seq("T" -> 616000L)),                          // 10:16
    SimBatch("batch4", Seq("U" -> 617000L))                           // 10:17
  )

  /** Simulation 2 - the below-watermark-but-still-open case (see .md). */
  private val sim2: Seq[SimBatch] = Seq(
    SimBatch("batch1", Seq("A" -> 543000L, "B" -> 547000L)),          // 09:03, 09:07
    SimBatch("batch2", Seq("C" -> 554000L, "D" -> 542000L)),          // 09:14, 09:02 (D late, window open)
    SimBatch("batch3", Seq("E" -> 561000L, "F" -> 543000L, "G" -> 530000L)), // 09:21, 09:03 (below wm, open), 08:50 (dropped)
    SimBatch("batch4", Seq("H" -> 562000L))                           // 09:22 (advances wm -> evicts [540000,545000) & [545000,550000))
  )

  def main(args: Array[String]): Unit = {
    val which = args.headOption.getOrElse("sim2")
    val batches = which match {
      case "sim1" => sim1
      case _      => sim2
    }

    println(s"[sender] starting ServerSocket on port $Port, waiting for Spark to connect...")
    val serverSocket = new ServerSocket(Port)
    val socket = serverSocket.accept() // blocks until the socket source connects
    println(s"[sender] Spark connected from ${socket.getInetAddress}. Sending '$which'.")

    val out = new PrintStream(socket.getOutputStream)
    try {
      batches.foreach { b =>
        println(s"[sender] --- ${b.label} ---")
        b.events.foreach { case (key, millis) =>
          val line = s"$key,$millis"
          out.println(line)          // newline-delimited: socket source reads per line
          out.flush()
          println(s"[sender]   sent $line  (eventTime ${millis / 1000}s)")
        }
        Thread.sleep(BatchGapMillis) // let this group be consumed as its own trigger
      }
      // Keep a couple of empty trailing "ticks" so the final eviction batch fires.
      println("[sender] all batches sent; holding connection so trailing triggers run...")
      Thread.sleep(BatchGapMillis * 3)
    } finally {
      out.close()
      socket.close()
      serverSocket.close()
      println("[sender] done.")
    }
  }
}