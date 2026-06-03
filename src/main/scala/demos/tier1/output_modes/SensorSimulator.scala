package demos.tier1.output_modes

import java.time.Instant
import scala.util.Random

/**
 * A stateful per-sensor simulator. Holds the sensor's last reading and
 * produces the next one with a small random drift, so consecutive readings
 * look like real sensor output rather than uncorrelated noise.
 *
 * Not thread-safe — each sensor instance is updated from a single producer
 * loop, which is fine for our purposes.
 */
class SensorSimulator(
                       val sensorId:   String,
                       val roomId:     String,
                       val buildingId: String,
                       initialTemp:    Double,
                       initialHumid:   Double,
                       initialOccupied: Boolean
                     ) {

  private val rng = new Random()
  private var temperature: Double  = initialTemp
  private var humidity:    Double  = initialHumid
  private var occupied:    Boolean = initialOccupied

  // Probability per tick that a room flips occupancy state.
  // Roughly: every ~30 readings (~2.5 minutes) a room changes occupancy.
  private val occupancyFlipProb: Double = 0.03

  /**
   * Produce the next reading. Mutates internal state to drift slowly.
   */
  def nextReading(): SensorReading = {
    // Small Gaussian drift on temperature and humidity.
    temperature = clamp(temperature + rng.nextGaussian() * 0.2, min = 16.0, max = 32.0)
    humidity    = clamp(humidity    + rng.nextGaussian() * 0.5, min = 25.0, max = 70.0)

    // Occasional occupancy flip.
    if (rng.nextDouble() < occupancyFlipProb) {
      occupied = !occupied
    }

    SensorReading(
      sensorId    = sensorId,
      roomId      = roomId,
      buildingId  = buildingId,
      eventTime   = Instant.now(),
      temperature = round2(temperature),
      humidity    = round2(humidity),
      occupied    = occupied
    )
  }

  private def clamp(x: Double, min: Double, max: Double): Double =
    math.max(min, math.min(max, x))

  private def round2(x: Double): Double =
    math.round(x * 100.0) / 100.0
}

object SensorSimulator {

  /**
   * Build a deterministic fleet: one building, N rooms, K sensors per room.
   *
   * Sensor IDs are deterministic ("sensor-<room>-<index>") so re-running
   * the producer gives the same sensor identities — useful when you want
   * to inspect the silver table and see the same `room_id` keys you saw
   * last time.
   */
  def buildFleet(
                  buildingId:      String,
                  numRooms:        Int,
                  sensorsPerRoom:  Int
                ): Seq[SensorSimulator] = {
    val rng = new Random(seed = 42L)  // deterministic initial values

    for {
      roomIdx   <- 1 to numRooms
      sensorIdx <- 1 to sensorsPerRoom
    } yield {
      val roomId   = f"room-$roomIdx%03d"
      val sensorId = f"sensor-$roomId-$sensorIdx%d"

      new SensorSimulator(
        sensorId        = sensorId,
        roomId          = roomId,
        buildingId      = buildingId,
        initialTemp     = 20.0 + rng.nextGaussian() * 2.0,
        initialHumid    = 45.0 + rng.nextGaussian() * 5.0,
        initialOccupied = rng.nextBoolean()
      )
    }
  }
}