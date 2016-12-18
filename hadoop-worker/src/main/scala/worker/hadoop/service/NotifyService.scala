package worker.hadoop.service

import com.twitter.logging
import com.twitter.logging.Logger
import com.typesafe.config.Config

/**
  * Created by tiennt4 on 09/12/2016.
  */
abstract class NotifyService(config: Config) {
  def notify(address: String, message: String)
}

class LoggingNotifier(config: Config) extends NotifyService(config) {
  private final val logger: Logger = logging.Logger.get(config.getString("file_name"))

  override def notify(address: String, message: String): Unit = {
    logger.error(s"notify to: $address, message: $message")
  }
}