package collector.domain

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}

import scala.collection.JavaConversions._

/**
  * Created by zkidkid on 11/30/16.
  */
case class AnalyticRequest(name: String, version: Int, data: String, timestamp: Long = System.currentTimeMillis())

object AnalyticRequest {
//  def apply(name: String, version: Int, data: String, timestamp: Long = System.currentTimeMillis()): AnalyticRequest = new AnalyticRequest(name, version, data, timestamp)

  def apply(name: String, version: Int, params: Map[String, String]): AnalyticRequest = {
    new AnalyticRequest(name, version, ConfigFactory.parseMap(params).root().render(ConfigRenderOptions.concise()))
  }
}
