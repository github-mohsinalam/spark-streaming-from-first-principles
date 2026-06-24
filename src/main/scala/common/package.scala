import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types._

package object common {

  val payloadSchema: StructType = StructType(Array(
    StructField("sensorId",    StringType,    nullable = false),
    StructField("roomId",      StringType,    nullable = false),
    StructField("buildingId",  StringType,    nullable = false),
    StructField("eventTime",   TimestampType, nullable = false),
    StructField("temperature", DoubleType,    nullable = false),
    StructField("humidity",    DoubleType,    nullable = false),
    StructField("occupied",    BooleanType,   nullable = false)
  ))

  def startProgressThread(query : StreamingQuery): Unit = {
    val progressThread = new Thread(() => {
      try {
        while (query.isActive) {
          Thread.sleep(10000)
          val lastProgress = query.lastProgress
          if (lastProgress != null) {
            println(s"[progress] batchId=${lastProgress.batchId} " +
              s"inputRows=${lastProgress.numInputRows} " +
              s"rate=${"%.1f".format(lastProgress.processedRowsPerSecond)} rows/sec")
          }
        }
      } catch {
        case _: InterruptedException => // graceful stop
      }
    })
    progressThread.setDaemon(true)
    progressThread.start()
    print(s"A progress thread started to track the progress of query : ${query.id}")

  }
}
