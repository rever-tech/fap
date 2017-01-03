package worker.hadoop.service

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean

import cakesolutions.kafka.KafkaConsumer
import cakesolutions.kafka.KafkaConsumer.Conf
import com.typesafe.config.ConfigFactory
import kafka.TopicAndOffsets
import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

import scala.annotation.tailrec
import scala.collection.JavaConversions._

/**
  * Created by zkidkid on 12/2/16.
  */

trait Worker[T] {

  def process(t: T): Unit

  def start(): Unit

  def stop(): Unit

  def isRunning(): Boolean
}

trait KafkaWorker[K, V] extends Worker[ConsumerRecord[K, V]]


abstract class AbstractKafkaWorker[K, V](config: String) extends KafkaWorker[K, V] {

  def keyDeserialize(): org.apache.kafka.common.serialization.Deserializer[K]

  def valueDeserialize(): org.apache.kafka.common.serialization.Deserializer[V]

  protected val offsetToCommit = new ConcurrentLinkedQueue[TopicAndOffsets]()

  def subscribeList(): Seq[String]

  def beforeStart(): Unit

  def afterStop(): Unit

  val kafkaConfig = Conf(ConfigFactory.parseString(config), keyDeserialize(), valueDeserialize())

  val kafkaConsumer = KafkaConsumer(kafkaConfig)

  kafkaConsumer.subscribe(subscribeList())

  val KafkaPollTimeInMs = 100

  private def commitOffset(topic: String, partition: Int, offset: Long): Unit =
    kafkaConsumer.commitSync(
      Map(new TopicPartition(topic, partition) -> new OffsetAndMetadata(offset)))

  private def commitOffsets(offsets: Map[String, Map[Int, Long]]): Unit =
    kafkaConsumer.commitSync(offsets.flatMap(topic => topic._2.map(partition =>
      new TopicPartition(topic._1, partition._1) -> new OffsetAndMetadata(partition._2))))

  private def commitOffsets(topic: String, offsets: Map[Int, Long]): Unit =
    kafkaConsumer.commitSync(offsets.map(offset =>
      new TopicPartition(topic, offset._1) -> new OffsetAndMetadata(offset._2)))

  def queueCommitOffsets(topic: String, offsets: Map[Int, Long]) : Unit = {
    offsetToCommit.offer(TopicAndOffsets(topic, offsets))
  }


  val flag = new AtomicBoolean(false)

  val running = new AtomicBoolean(false)

  @tailrec
  private def commit(): Unit = {
    Option(offsetToCommit.poll()) match {
      case Some(offset) =>
        commitOffsets(offset.topicName, offset.offsets)
        commit()
      case None =>
    }
  }

  def start(): Unit = {
    running.set(true)
    beforeStart()
    while (flag.get() == false) {
      val kafkaRecords = kafkaConsumer.poll(KafkaPollTimeInMs)
      kafkaRecords.toIterable.foreach {
        process
      }
      //Should it called after every poll?
      commit()
    }

    afterStop()
    running.set(false)
  }

  def stop(): Unit = flag.set(true)

  override def isRunning(): Boolean = running.get()

}