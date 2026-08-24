package demos.tier3

import org.json4s.{DefaultFormats, Formats}
import org.json4s.jackson.JsonMethods

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.util.Try

/**
 * Reads a Structured Streaming checkpoint directory off local disk.
 *
 * Distinct from the Tier 1 inspector, which was oriented at the Delta
 * transaction log. This one is about POSITION: who recorded what, and when.
 * Three artifacts matter here, and each has its own on-disk format --
 * all three verified against Spark v4.1.2 source rather than inferred:
 *
 *   offsets/N     -- the PLAN for batch N, written BEFORE the batch runs.
 *                    Line 1: "v1". Line 2: OffsetSeqMetadata JSON (watermark,
 *                    batch timestamp, captured SQL conf). Then ONE LINE PER
 *                    SOURCE, in the query's source order, holding that
 *                    source's end offsets; "-" if a source has no offset.
 *                    (OffsetSeqLog.serialize / its class doc.)
 *
 *   commits/N     -- batch N COMPLETED, written after. Line 1: "v1"
 *                    (the version is the state-store checkpoint format
 *                    version). Line 2: CommitMetadata JSON, carrying
 *                    nextBatchWatermarkMs -- which is how the watermark
 *                    survives a restart. (CommitLog.serialize.)
 *
 *   sources/<i>/0 -- the Kafka source's OWN metadata log: the offsets
 *                    `startingOffsets` resolved to, written ONCE at query
 *                    birth and consulted forever after. Format is unusual:
 *                    a single ZERO BYTE (a Spark 2.1 compatibility artifact,
 *                    SPARK-19517), then "v1\n", then the offset JSON.
 *                    (KafkaSourceInitialOffsetWriter.serialize.)
 *
 * A batch id present in offsets/ but absent from commits/ is a batch that was
 * planned and did not finish -- the state a crash leaves behind.
 */
object CheckpointInspector {

  private implicit val fmt: Formats = DefaultFormats

  private def batchIds(dir: File): Seq[Long] =
    if (!dir.isDirectory) Nil
    else dir.listFiles()
      .filter(f => f.isFile && f.getName.forall(_.isDigit))
      .map(_.getName.toLong).sorted.toSeq

  private def read(f: File): String = new String(Files.readAllBytes(f.toPath), StandardCharsets.UTF_8)

  // ------------------------------------------------------------ offsets ---

  /** Raw source-offset lines of offsets/N, one entry per source. */
  def offsetLogSourceLines(checkpoint: String, batchId: Long): Seq[String] = {
    val f = new File(s"$checkpoint/offsets/$batchId")
    if (!f.exists()) Nil
    else read(f).split("\n").toSeq.drop(2).map(_.trim).filter(_.nonEmpty)
  }

  /**
   * End offsets planned for batch N, for the first source, as
   * topic -> (partition -> offset). Remember these are EXCLUSIVE ends: the
   * next offset to read, not the last one read.
   */
  def plannedEndOffsets(checkpoint: String, batchId: Long): Map[String, Map[Int, Long]] =
    offsetLogSourceLines(checkpoint, batchId).headOption
      .filter(_ != "-")
      .flatMap(json => Try(parseTopicOffsets(json)).toOption)
      .getOrElse(Map.empty)

  private def parseTopicOffsets(json: String): Map[String, Map[Int, Long]] =
    JsonMethods.parse(json).extract[Map[String, Map[String, BigInt]]]
      .map { case (t, m) => t -> m.map { case (p, o) => p.toInt -> o.toLong } }

  def offsetLogMetadata(checkpoint: String, batchId: Long): Option[String] = {
    val f = new File(s"$checkpoint/offsets/$batchId")
    if (!f.exists()) None else read(f).split("\n").lift(1).map(_.trim)
  }

  // ------------------------------------------------------------ commits ---

  def commitExists(checkpoint: String, batchId: Long): Boolean =
    new File(s"$checkpoint/commits/$batchId").exists()

  def commitMetadata(checkpoint: String, batchId: Long): Option[String] = {
    val f = new File(s"$checkpoint/commits/$batchId")
    if (!f.exists()) None else read(f).split("\n").lift(1).map(_.trim)
  }

  // --------------------------------------------- source initial offsets ---

  /** Raw JSON of sources/<idx>/0, skipping the leading zero byte and "v1" line. */
  def sourceInitialOffsetsRaw(checkpoint: String, sourceIdx: Int = 0): Option[String] = {
    val f = new File(s"$checkpoint/sources/$sourceIdx/0")
    if (!f.exists()) None
    else {
      val bytes = Files.readAllBytes(f.toPath)
      val body = new String(bytes.drop(1), StandardCharsets.UTF_8) // drop the 0 byte
      val nl = body.indexOf('\n')
      if (nl > 0) Some(body.substring(nl + 1).trim) else Some(body.trim)
    }
  }

  def sourceInitialOffsets(checkpoint: String, sourceIdx: Int = 0): Map[String, Map[Int, Long]] =
    sourceInitialOffsetsRaw(checkpoint, sourceIdx)
      .flatMap(j => Try(parseTopicOffsets(j)).toOption).getOrElse(Map.empty)

  // ------------------------------------------------------------- report ---

  private def fmtOffsets(m: Map[String, Map[Int, Long]]): String =
    if (m.isEmpty) "-"
    else m.toSeq.sortBy(_._1).map { case (t, parts) =>
      t + " " + parts.toSeq.sortBy(_._1).map { case (p, o) => s"p$p=$o" }.mkString(" ")
    }.mkString(" | ")

  /** The headline view: every planned batch, and whether it ever completed. */
  def report(checkpoint: String, label: String = ""): Unit = {
    println(s"\n=== checkpoint: $checkpoint ${if (label.nonEmpty) s"($label)" else ""} ===")

    sourceInitialOffsetsRaw(checkpoint) match {
      case Some(j) => println(s"  sources/0/0 (initial offsets, written once at birth):\n     $j")
      case None    => println("  sources/0/0 : ABSENT")
    }

    val planned = batchIds(new File(s"$checkpoint/offsets"))
    val done = batchIds(new File(s"$checkpoint/commits")).toSet
    if (planned.isEmpty) { println("  no batches planned yet"); return }

    println(f"\n  ${"batch"}%-6s ${"committed"}%-10s planned end offsets (exclusive)")
    planned.foreach { b =>
      val mark = if (done.contains(b)) "yes" else "NO  <-- planned, never finished"
      println(f"  $b%-6d $mark%-10s ${fmtOffsets(plannedEndOffsets(checkpoint, b))}")
    }

    val orphans = planned.filterNot(done.contains)
    if (orphans.nonEmpty)
      println(s"\n  => batch(es) ${orphans.mkString(",")} will be RE-EXECUTED verbatim on restart.")
  }

  def deleteRecursively(path: String): Unit = {
    val f = new File(path)
    def go(x: File): Unit = {
      if (x.isDirectory) Option(x.listFiles()).foreach(_.foreach(go))
      x.delete()
    }
    if (f.exists()) { go(f); println(s"[fs] deleted $path") }
  }
}