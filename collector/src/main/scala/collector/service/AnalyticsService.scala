package collector.service

import javax.inject.Inject

import cakesolutions.kafka.KafkaProducer
import cakesolutions.kafka.KafkaProducer.Conf
import collector.domain.AnalyticRequest
import collector.util.ZConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}

import scala.concurrent.ExecutionContext

/**
 * Created by zkidkid on 12/1/16.
 */
trait AnalyticsService {
  def process(request: AnalyticRequest): Unit
}

trait AnalyticsConsumer extends Runnable

trait AnalyticsConsumerBuilder {
  def build(request: AnalyticRequest): AnalyticsConsumer
}


object KafkaAnalyticsConsumerBuilder extends AnalyticsConsumerBuilder {
  override def build(request: AnalyticRequest): AnalyticsConsumer = {
    return new KafkaConsumer(request)
  }
}

object KafkaConsumer {
  final val producer = KafkaProducer(
    Conf(ZConfig.getConfig("kafka"), new IntegerSerializer, new StringSerializer)
  )

  def send(record: ProducerRecord[Integer, String]): Unit = {
    producer.send(record)
  }
}

class KafkaConsumer(request: AnalyticRequest) extends AnalyticsConsumer {

  override def run(): Unit = {

    KafkaConsumer.send(
      new ProducerRecord[Integer, String](
        request.name, null, request.timestamp, request.version, request.data))
  }
}

class AnalyticsServiceImpl @Inject()(builder: AnalyticsConsumerBuilder)(implicit ec: ExecutionContext) extends AnalyticsService {

  override def process(request: AnalyticRequest): Unit = {
    ec.execute(builder.build(request))
  }
}


