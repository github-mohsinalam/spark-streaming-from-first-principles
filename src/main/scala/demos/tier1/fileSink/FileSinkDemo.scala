package demos.tier1.fileSink


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object FileSinkDemo {

  System.setProperty("hadoop.home.dir", "C:\\Program Files\\hadoop")

  private val tablePath      = "/tmp/file-sink/"
  private val checkpointPath = "/tmp/file-sink/_checkpoint"

  private val kafkaBootstrap = "localhost:9092"
  private val kafkaTopic     = "sensor-events"

  private val payloadSchema: StructType = StructType(Array(
    StructField("sensorId",    StringType,    nullable = false),
    StructField("roomId",      StringType,    nullable = false),
    StructField("buildingId",  StringType,    nullable = false),
    StructField("eventTime",   TimestampType, nullable = false),
    StructField("temperature", DoubleType,    nullable = false),
    StructField("humidity",    DoubleType,    nullable = false),
    StructField("occupied",    BooleanType,   nullable = false)
  ))

  val spark: SparkSession = SparkSession.builder()
    .appName("DemoFileSink")
    .master("local[3]")
    // Quieter logs so the demo output is readable.
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val rawKafka = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrap)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", "earliest")
      .load()


    val processedDf = rawKafka
      .select(
        from_json(col("value").cast(StringType), payloadSchema).as("events")
      ).select(
        col("events.*"),
        current_timestamp().as("ingestionTime")
      )

    val query = processedDf.writeStream
      .format("json")
      .outputMode("append")
      .option("checkpointLocation",checkpointPath)
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start(tablePath)

    //Write a progress thread

  }



}
