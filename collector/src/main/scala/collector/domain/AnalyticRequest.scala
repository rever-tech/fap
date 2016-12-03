package collector.domain

/**
  * Created by zkidkid on 11/30/16.
  */
case class AnalyticRequest(name: String, version: Int, data: String, timestamp: Long = System.currentTimeMillis())
