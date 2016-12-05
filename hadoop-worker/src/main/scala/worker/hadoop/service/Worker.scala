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
}

trait KafkaWorker[K, V] extends Worker[ConsumerRecord[K, V]]


abstract class AbstractKafkaWorker[K, V](config: String) extends KafkaWorker[K, V] {

  abstract def keyDeserialize(): org.apache.kafka.common.serialization.Deserializer[K]

  abstract def valueDeserialize(): org.apache.kafka.common.serialization.Deserializer[V]

  abstract def subscribeList(): Seq[String]

  abstract def beforeStart(): Unit

  abstract def afterStop(): Unit

  val kafkaConfig = Conf(ConfigFactory.parseString(config), keyDeserialize(), valueDeserialize())

  val kafkaConsumer = KafkaConsumer(kafkaConfig)

  kafkaConsumer.subscribe(subscribeList())

  val KafkaPollTimeInMs = 100

  val isStop = new AtomicBoolean(false)


  def run(): Unit = {
    beforeStart()
    while (isStop.get() == false) {
      val kafkaRecords = kafkaConsumer.poll(KafkaPollTimeInMs)
      kafkaRecords.toIterable.foreach {
        process
      }
    }
    afterStop()
  }


}