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
}
