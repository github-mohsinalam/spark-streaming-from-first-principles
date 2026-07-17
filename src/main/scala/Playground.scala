import common.{payloadSchema,startProgressThread}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._

object Playground{
  System.setProperty("hadoop.home.dir", "C:\\Program Files\\hadoop")

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("a Test App")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  /*import spark.implicits._
  val data = Seq(
    (1, "One"),
    (2,"Two"),
    (3,"Three")
  )

  val df = data.toDF("Id","Numbers")*/

  def readFromSocket() : Unit ={
    val socketDF = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port", 12345)
      .load()

    val query = socketDF
      .writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    query.awaitTermination()
    query.stop()
  }

  def readFromKafka() : Unit = {
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "sensor-events")
      .option("startingOffsets","latest")
      .load()

    val parsedDF = df.select(
      from_json(col("value").cast("string"), payloadSchema).as("events")
    ).select(
      col("events.*")
    )

    val deduplicate = parsedDF
      .dropDuplicates("sensorId","roomId","buildingId","eventTime")
      .writeStream
      .outputMode("append")
      .format("delta")
      .option("checkpointLocation", "/tmp/output-modes/bronze/drop_duplicate/_checkpoint")
      .start("/tmp/output-modes/bronze/drop_duplicate")

    print(s"Query started :  ${deduplicate.id}")
    startProgressThread(deduplicate)

    deduplicate.awaitTermination()


  }

  def main(args: Array[String]): Unit = {
    // df.show
    //readFromSocket()

    val df = spark.read
      .json("/tmp/file-sink/")

    print(df.count)
    //readFromKafka()





  }



}
