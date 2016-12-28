package worker.hadoop.service

import com.typesafe.config.Config
import org.slf4j.LoggerFactory

/**
  * Created by tiennt4 on 09/12/2016.
  */
abstract class NotifyService(config: Config) {
  def notify(address: String, message: String)
  def notify(address: String, message: String, t: Throwable)
}

class LoggingNotifier(config: Config) extends NotifyService(config) {
  private final val logger = LoggerFactory.getLogger(config.getString("logger_name"))

  //Testing
  logger.error("======== test =========")

  override def notify(address: String, message: String): Unit = {
    logger.error(s"notify to: $address, message: $message")
  }

  override def notify(address: String, message: String, t: Throwable): Unit = {
    logger.error(s"notify to: $address, message: $message", t)
  }
}