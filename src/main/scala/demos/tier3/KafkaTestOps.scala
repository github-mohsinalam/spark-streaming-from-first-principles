package demos.tier3

import org.apache.kafka.clients.admin.{Admin, NewPartitions, NewTopic}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import java.util.Properties
import scala.jdk.CollectionConverters._
import scala.util.Try

/**
 * Kafka control-plane + data-plane helpers for Tier 3 demos.
 *
 * Deliberately NOT the Tier 1 sensor producer: these demos need EXACT record
 * counts landed on SPECIFIC partitions, so every send names its partition
 * explicitly rather than relying on the default partitioner. Predicting what
 * the rate limiter will plan requires knowing the per-partition backlog to the
 * record.
 *
 * Topic creation and partition growth are done in-code via AdminClient so a
 * demo run is self-contained -- no CLI prerequisites, and `reset` guarantees a
 * known starting state.
 */
object KafkaTestOps {

  val Bootstrap = "localhost:9092"

  // ---------------------------------------------------------------- admin ---

  private def admin(): Admin = {
    val p = new Properties()
    p.put("bootstrap.servers", Bootstrap)
    Admin.create(p)
  }

  private def withAdmin[T](f: Admin => T): T = {
    val a = admin()
    try f(a) finally a.close()
  }

  def topicExists(topic: String): Boolean =
    withAdmin(_.listTopics().names().get().contains(topic))

  def deleteTopic(topic: String): Unit = withAdmin { a =>
    if (a.listTopics().names().get().contains(topic)) {
      a.deleteTopics(List(topic).asJava).all().get()
      // Deletion is asynchronous on the broker; poll until it is really gone,
      // otherwise a create() immediately after can race and fail.
      var waited = 0
      while (a.listTopics().names().get().contains(topic) && waited < 30) {
        Thread.sleep(500); waited += 1
      }
      println(s"[admin] deleted topic $topic")
    }
  }

  def createTopic(topic: String, partitions: Int, replication: Short = 1): Unit =
    withAdmin { a =>
      a.createTopics(List(new NewTopic(topic, partitions, replication)).asJava).all().get()
      println(s"[admin] created topic $topic with $partitions partitions")
    }

  /** Recreate the topic from scratch -- a known starting state for a demo run. */
  def resetTopic(topic: String, partitions: Int): Unit = {
    deleteTopic(topic)
    createTopic(topic, partitions)
  }

  /** Grow an existing topic. Kafka only allows increasing the partition count. */
  def addPartitions(topic: String, newTotal: Int): Unit = withAdmin { a =>
    a.createPartitions(Map(topic -> NewPartitions.increaseTo(newTotal)).asJava).all().get()
    println(s"[admin] topic $topic grown to $newTotal partitions")
  }

  def partitionCount(topic: String): Int =
    withAdmin(_.describeTopics(List(topic).asJava).allTopicNames().get().get(topic).partitions().size())

  /**
   * Consumer-group offsets committed to Kafka, for every group whose id starts
   * with `prefix`. Spark's Kafka source uses the prefix "spark-kafka-source".
   * Used to demonstrate that the set is empty.
   */
  def committedOffsetsForGroupPrefix(prefix: String): Map[String, Map[TopicPartition, Long]] =
    withAdmin { a =>
      val groups = a.listConsumerGroups().all().get().asScala
        .map(_.groupId()).filter(_.startsWith(prefix)).toList
      groups.flatMap { g =>
        Try {
          val offs = a.listConsumerGroupOffsets(g)
            .partitionsToOffsetAndMetadata().get().asScala
            .map { case (tp, om) => tp -> om.offset() }.toMap
          if (offs.isEmpty) None else Some(g -> offs)
        }.getOrElse(None)
      }.toMap
    }

  // ------------------------------------------------------------- produce ---

  private def producer(): KafkaProducer[String, String] = {
    val p = new Properties()
    p.put("bootstrap.servers", Bootstrap)
    p.put("key.serializer", classOf[StringSerializer].getName)
    p.put("value.serializer", classOf[StringSerializer].getName)
    p.put("acks", "all")
    new KafkaProducer[String, String](p)
  }

  /**
   * Send `count` records to ONE named partition. Values are
   *   p<partition>-<n>
   * with `n` running from `startId`, so a record's identity says where it came
   * from and where it sat in that partition's sequence.
   */
  def produceTo(topic: String, partition: Int, count: Int, startId: Int = 0): Unit = {
    val prod = producer()
    try {
      (startId until startId + count).foreach { i =>
        val v = s"p$partition-$i"
        prod.send(new ProducerRecord[String, String](
          topic, Integer.valueOf(partition), v, v))
      }
      prod.flush()
      println(s"[produce] $topic p$partition <- $count records (ids $startId..${startId + count - 1})")
    } finally prod.close()
  }

  /** Same count to every partition of the topic. */
  def produceEvenly(topic: String, perPartition: Int, startId: Int = 0): Unit = {
    val n = partitionCount(topic)
    (0 until n).foreach(p => produceTo(topic, p, perPartition, startId))
  }

  // -------------------------------------------------------------- offsets ---

  /** The "latest" the driver would see right now: end offset per partition. */
  def endOffsets(topic: String): Map[Int, Long] = {
    val p = new Properties()
    p.put("bootstrap.servers", Bootstrap)
    p.put("key.deserializer", classOf[StringDeserializer].getName)
    p.put("value.deserializer", classOf[StringDeserializer].getName)
    p.put("group.id", s"probe-${System.currentTimeMillis()}")
    val c = new KafkaConsumer[String, String](p)
    try {
      val tps = c.partitionsFor(topic).asScala.map(pi => new TopicPartition(topic, pi.partition())).toList
      c.endOffsets(tps.asJava).asScala.map { case (tp, o) => tp.partition() -> o.longValue() }.toMap
    } finally c.close()
  }

  def printEndOffsets(label: String, topic: String): Unit = {
    val m = endOffsets(topic).toSeq.sortBy(_._1)
    println(s"[kafka] $label  end offsets: " + m.map { case (p, o) => s"p$p=$o" }.mkString("  "))
  }
}