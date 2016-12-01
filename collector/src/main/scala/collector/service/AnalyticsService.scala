package collector.service

import javax.inject.Inject

import collector.domain.AnalyticRequest

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

class KafkaConsumer(request: AnalyticRequest) extends AnalyticsConsumer {
  override def run(): Unit = {
    //ToDo: @tiennt implement push data to kafka
  }
}

class AnalyticsServiceImpl @Inject()(builder: AnalyticsConsumerBuilder)(implicit ec: ExecutionContext) extends AnalyticsService {

  override def process(request: AnalyticRequest): Unit = {
    ec.execute(builder.build(request))
  }
}


