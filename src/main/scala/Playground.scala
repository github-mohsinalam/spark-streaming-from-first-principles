import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object Playground{
  System.setProperty("hadoop.home.dir", "C:\\Program Files\\hadoop")

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("a Test App")
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

  def main(args: Array[String]): Unit = {
    // df.show
    readFromSocket()



  }



}
