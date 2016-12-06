package worker.hadoop.service

import java.util.concurrent.atomic.AtomicBoolean

import cakesolutions.kafka.KafkaConsumer
import cakesolutions.kafka.KafkaConsumer.Conf
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerRecord

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

  def subscribeList(): Seq[String]

  def beforeStart(): Unit

  def afterStop(): Unit

  val kafkaConfig = Conf(ConfigFactory.parseString(config), keyDeserialize(), valueDeserialize())

  val kafkaConsumer = KafkaConsumer(kafkaConfig)

  kafkaConsumer.subscribe(subscribeList())

  val KafkaPollTimeInMs = 100

  val flag = new AtomicBoolean(false)

  val isRunning = new AtomicBoolean(false)

  def start(): Unit = {
    isRunning.set(true)
    beforeStart()
    while (flag.get() == false) {
      val kafkaRecords = kafkaConsumer.poll(KafkaPollTimeInMs)
      kafkaRecords.toIterable.foreach {
        process
      }
    }
    afterStop()
    isRunning.set(false)

  }

  def stop(): Unit = flag.set(true)


  override def isRunning(): Boolean = isRunning.get()


}