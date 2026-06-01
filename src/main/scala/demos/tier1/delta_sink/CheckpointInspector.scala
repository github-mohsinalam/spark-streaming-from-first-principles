package demos.tier1.delta_sink

import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters._
import com.fasterxml.jackson.databind.ObjectMapper

/**
 * Walks a Spark streaming checkpoint directory and lines up its
 * contents with the Delta transaction log of the table the checkpoint
 * writes to. For each batchId, reports:
 *
 *   - offset log:  what input range the engine intended to process in the current batch
 *   - commit log:  whether the engine recorded the batch as committed
 *   - Delta log:   which Delta version this batchId wrote, and the
 *                  appId from the txn action
 *
 * The three indices line up to the same batchId in the happy path.
 * An offsets/N file without a matching commits/N file is the visible
 * crash-window signature — the batch was planned but not committed,
 * and on restart would be replayed.
 *
 * No Spark APIs used here. We read raw files so what we report is
 * what's physically on disk, not what a higher-level API summarises.
 */
object CheckpointInspector {

  private val mapper = new ObjectMapper()

  def inspect(checkpointPath: String, tablePath: String): Unit = {
    val ckptDir = Paths.get(checkpointPath)
    if (!Files.exists(ckptDir)) {
      println(s"No checkpoint directory at $checkpointPath")
      return
    }

    val offsetIds  = listBatchIds(ckptDir.resolve("offsets"))
    val commitIds  = listBatchIds(ckptDir.resolve("commits")).toSet
    val deltaTxns  = readDeltaTxns(tablePath)  // Map[batchId, (deltaVersion, appId)]

    println(s"\n=== Checkpoint / Delta log alignment ===")
    println(s"checkpoint: $checkpointPath")
    println(s"table:      $tablePath\n")

    if (offsetIds.isEmpty) {
      println("No batches planned yet.")
      return
    }

    offsetIds.foreach { batchId =>
      val offsetSummary = summariseOffset(ckptDir.resolve(s"offsets/$batchId"))
      val committed     = commitIds.contains(batchId)
      val deltaInfo     = deltaTxns.get(batchId)

      val commitStr = if (committed) "committed" else "MISSING (planned but not committed)"
      val deltaStr  = deltaInfo match {
        case Some((version, appId)) =>
          s"Delta v$version, txn(appId=${appId.take(8)}…, batchId=$batchId)"
        case None =>
          "no Delta version (sink never wrote, or txn absent)"
      }

      val aligned = committed && deltaInfo.isDefined
      val marker  = if (aligned) "  " else ">>"

      println(s"$marker Batch $batchId")
      println(s"     offset log: $offsetSummary")
      println(s"     commit log: $commitStr")
      println(s"     delta log:  $deltaStr")

      if (!aligned) {
        println(s"     ^^^ ALIGNMENT BROKEN — this is the crash-window signature.")
      }
      println()
    }
  }

  /**
   * Lists batchIds present as files in the given directory (offsets/
   * or commits/). Filenames are bare integers, no padding, no extension.
   */
  private def listBatchIds(dir: Path): List[Long] = {
    if (!Files.exists(dir)) return Nil
    Files.list(dir).iterator().asScala
      .map(_.getFileName.toString)
      .filter(_.forall(_.isDigit))
      .map(_.toLong)
      .toList
      .sorted
  }

  /**
   * The offset log file has three lines:
   *   line 1: version marker (e.g. "v1")
   *   line 2: JSON metadata (watermark, timestamp, configs)
   *   line 3+: source-specific offset representation
   *
   * For the rate source, line 3 is a single integer: the elapsed-seconds
   * counter. We don't try to interpret other source formats here —
   * just print whatever is there.
   */
  private def summariseOffset(file: Path): String = {
    if (!Files.exists(file)) return "(missing)"
    val lines = Files.readAllLines(file).asScala.toList
    lines match {
      case _ :: _ :: rest if rest.nonEmpty =>
        s"ends at source offset ${rest.mkString(" / ")}"
      case _ =>
        s"(unexpected format: ${lines.mkString(" | ")})"
    }
  }

  /**
   * Walks the Delta log and builds a map from batchId (in the `txn`
   * action's `version` field) to (deltaVersion, appId). Batches that
   * went through foreachBatch + plain (non-txn) writes will NOT appear
   * in this map — that's the whole point of demos 03 and 04.
   */
  private def readDeltaTxns(tablePath: String): Map[Long, (Long, String)] = {
    val logDir = Paths.get(tablePath, "_delta_log")
    if (!Files.exists(logDir)) return Map.empty

    val commitFiles = Files.list(logDir).iterator().asScala
      .filter(_.getFileName.toString.matches("\\d{20}\\.json"))
      .toList

    commitFiles.flatMap { file =>
      val deltaVersion = file.getFileName.toString.stripSuffix(".json").toLong
      Files.readAllLines(file).asScala.flatMap { line =>
        val node = mapper.readTree(line)
        if (node.has("txn")) {
          val txn = node.get("txn")
          val batchId = txn.get("version").asLong()
          val appId   = txn.get("appId").asText()
          Some(batchId -> (deltaVersion, appId))
        } else None
      }
    }.toMap
  }
}