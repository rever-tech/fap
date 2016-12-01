package collector.service

import javax.inject.Inject

import collector.domain.AnalyticRequest

import scala.concurrent.ExecutionContext

/**
 * Created by zkidkid on 12/1/16.
 */
trait AnalyticService {
  def process(request: AnalyticRequest): Unit
}

trait AnalyticConsumer extends Runnable

trait AnalyticConsumerBuilder {
  def build(request: AnalyticRequest): AnalyticConsumer
}


object KafkaAnalyticConsumerBuilder extends AnalyticConsumerBuilder {
  override def build(request: AnalyticRequest): AnalyticConsumer = {
    return new KafkaConsumer(request)
  }
}

class KafkaConsumer(request: AnalyticRequest) extends AnalyticConsumer {
  override def run(): Unit = {

  }
}

class AnalyticServiceImpl @Inject()(consumerBuilder: AnalyticConsumerBuilder)(implicit ec: ExecutionContext) extends AnalyticService {

  override def process(request: AnalyticRequest): Unit = {
    ec.execute(consumerBuilder.build(request))
  }
}


