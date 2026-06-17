package common

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.time.Duration
import java.util.Properties

/**
 * Synthetic sensor fleet → Kafka.
 *
 * Initialises a deterministic fleet of sensors and produces readings
 * to a Kafka topic at a steady rate. Runs until you Ctrl-C it.
 *
 * Run separately from the demos:
 *   sbt> runMain demos.tier1.output_modes.KafkaSensorProducer
 *
 * Configuration (compile-time constants — adjust here if needed):
 *   - bootstrapServers: localhost:9092 (the Docker Kafka from your setup)
 *   - topic: sensor-events
 *   - building: bldg-A
 *   - 20 rooms × 3 sensors = 60 sensors total
 *   - one batch every 5 seconds (so each sensor reports once per 5s)
 */
object KafkaSensorProducer {

  private val bootstrapServers = "localhost:9092"
  private val topic            = "sensor-events"
  private val buildingId       = "bldg-A"
  private val numRooms         = 20
  private val sensorsPerRoom   = 3
  private val tickIntervalMs   = 5000L

  def main(args: Array[String]): Unit = {
    val fleet  = SensorSimulator.buildFleet(buildingId, numRooms, sensorsPerRoom)
    val mapper = buildJsonMapper()
    val producer = buildKafkaProducer()

    println(s"Producer starting. Fleet size: ${fleet.size} sensors across $numRooms rooms.")
    println(s"Topic: $topic. Bootstrap: $bootstrapServers. Tick: ${tickIntervalMs}ms.")
    println("Ctrl-C to stop.\n")

    // Graceful shutdown so Kafka's pending sends flush on Ctrl-C.
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      println("\nShutting down producer…")
      producer.flush()
      producer.close(Duration.ofSeconds(5))
      println("Producer closed.")
    }))

    var tick = 0L
    while (true) {
      //Defining a fleet simulates installing sensors in each room
      //calling nextReading on a sensor simulates an event generated from sensor
      //nextReading uses a local Random Number generator inside the method def
      //allowing us to produce different event on each call.
      val readings = fleet.map(_.nextReading())
      readings.foreach { reading =>
        val json = mapper.writeValueAsString(reading)
        // Key by roomId so all readings for the same room land on the
        // same Kafka partition — this matters if/when we add multiple
        // partitions and want ordering guarantees per room. For our
        // single-partition demo it's a no-op, but it's the right shape.
        val record = new ProducerRecord[String, String](topic, reading.roomId, json)
        producer.send(record)
      }

      tick += 1
      if (tick % 4 == 0) {  // log every 20 seconds (4 ticks at 5s each)
        val sample = readings.head
        println(s"[tick $tick] sent ${readings.size} readings. " +
          s"Sample: ${sample.sensorId} temp=${sample.temperature} occupied=${sample.occupied}")
      }

      Thread.sleep(tickIntervalMs)
    }
  }

  private def buildJsonMapper(): ObjectMapper = {
    val mapper = new ObjectMapper()

    // Teach Jackson about Scala case classes — without this, Scala fields
    // are invisible to Jackson's Java-bean introspection, and you get
    // empty `{}` objects. The module is already on the classpath via
    // Spark's transitive dependencies.
    mapper.registerModule(DefaultScalaModule)

    // Required so Jackson knows how to serialize java.time.Instant.
    mapper.registerModule(new JavaTimeModule())

    // Without this, Jackson writes Instants as a giant integer (epoch nanos).
    // We want ISO-8601 strings — readable, and Spark's from_json parses them
    // natively when the target schema field is TimestampType.
    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

    mapper
  }

  private def buildKafkaProducer(): KafkaProducer[String, String] = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.serializer",    classOf[StringSerializer].getName)
    props.put("value.serializer",  classOf[StringSerializer].getName)
    /* Tuning:
     acks=1 is the right default for a demo producer.
     acks=0: fire-and-forget, fastest, can lose data on broker restart
     acks=1: leader confirms
     acks=all: leader + ISRs confirm

     */
    props.put("acks", "1")
    props.put("linger.ms", "50")     // batch sends within 50ms windows
    props.put("compression.type", "snappy")
    new KafkaProducer[String, String](props)
  }
}