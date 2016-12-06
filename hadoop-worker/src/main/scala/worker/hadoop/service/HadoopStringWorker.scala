package worker.hadoop.service


import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.{Deserializer, StringDeserializer}

/**
 * Created by zkidkid on 12/5/16.
 */
class HadoopStringWorker(kafkaConfig: String, topics: Seq[String]) extends AbstractKafkaWorker[String, String](kafkaConfig) {

  override def keyDeserialize(): Deserializer[String] = new StringDeserializer()

  override def valueDeserialize(): Deserializer[String] = new StringDeserializer()

  override def subscribeList(): Seq[String] = topics

  override def beforeStart(): Unit = {

  }

  override def afterStop(): Unit = {

  }

  override def process(t: ConsumerRecord[String, String]): Unit = {

  }


}
