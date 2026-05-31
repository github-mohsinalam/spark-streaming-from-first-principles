import org.apache.spark.sql.SparkSession

object Playground{

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("a Test App")
    .getOrCreate()

  import spark.implicits._
  val data = Seq(
    (1, "One"),
    (2,"Two"),
    (3,"Three")
  )

  val df = data.toDF("Id","Numbers")

  def main(args: Array[String]): Unit = {
    // df.show



  }



}
