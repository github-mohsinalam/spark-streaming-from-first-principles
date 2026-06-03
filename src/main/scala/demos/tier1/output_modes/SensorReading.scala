package demos.tier1.output_modes

import java.time.Instant

/**
 * A single sensor telemetry reading from the building's HVAC system.
 *
 * Shared between the synthetic Kafka producer (which emits these as JSON)
 * and the Spark streaming demos (which consume them).
 *
 * Field notes:
 *   - sensorId / roomId / buildingId: identifiers. A building has many
 *     rooms; a room has many sensors. For these demos the building is
 *     single (`bldg-A`) but the field is present so the schema is
 *     production-shaped.
 *   - eventTime: the moment the sensor took the reading. This is event
 *     time — distinct from ingestion time (when Spark sees it) and
 *     processing time (when the trigger fires). Distinction matters
 *     in Tier 2.
 *   - temperature: degrees Celsius. Realistic indoor range: 18–28.
 *   - humidity: percentage, 0–100. Realistic indoor range: 30–60.
 *   - occupied: whether a person is currently in the room.
 */
case class SensorReading(
                          sensorId:    String,
                          roomId:      String,
                          buildingId:  String,
                          eventTime:   Instant,
                          temperature: Double,
                          humidity:    Double,
                          occupied:    Boolean
                        )
