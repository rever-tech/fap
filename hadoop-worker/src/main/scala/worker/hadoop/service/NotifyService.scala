package worker.hadoop.service

import java.util.Calendar

import com.google.inject.Inject
import com.google.inject.name.Named
import com.typesafe.config.Config
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.http.HttpResponse
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.util.EntityUtils
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

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

class SlackNotifyService @Inject()(@Named("slack_notification_config") config: Config) extends NotifyService(config) {

  private val slackNotification = new SlackNotification(config)

  override def notify(address: String, message: String): Unit = {
    slackNotification.notify(address, message)
  }

  override def notify(address: String, message: String, t: Throwable): Unit = {
    slackNotification.notify(address, message, t)
  }
}


object SlackNotification {
  private lazy val _client: HttpClient = new DefaultHttpClient

  @throws[Exception]
  private def notify(url: String, receiver: String, message: String): Boolean = {
    val request: HttpPost = new HttpPost(url)
    val entity =
      s"""
         |{
         | "text": "$message",
         | "channel": "$receiver"
         |}
      """.stripMargin

    request.setEntity(new StringEntity(entity))
    request.setHeader("Content-Type", "application/json; charset=utf-8")
    val responseEntity: HttpResponse = _client.execute(request)
    val response: String = EntityUtils.toString(responseEntity.getEntity).trim
    response == "ok"
  }
}

class SlackNotification(config: Config) {
  private val slackWebhookUrl = config.getString("slack_url")
  private val defaultUsers = if (config.hasPath("default_slack_users")) config.getStringList("default_slack_users").toSeq else Seq.empty[String]

  private val _client = new DefaultHttpClient

  private val dateFormat = {
    val formatStr = if (config.hasPath("time_format")) config.getString("time_format") else "dd/MM/yyyy HH:mm:ss"
    new java.text.SimpleDateFormat(formatStr)
  }


  def notify(users: String, message: String): Boolean = {
    val sb: StringBuilder = new StringBuilder
    sb.append("Time: ").append(dateFormat.format(Calendar.getInstance.getTime)).append("\n")
    sb.append("Msg: ").append(message)
    notify(sb.toString(), users.split(",").toSeq)
  }


  def notify(users: String, message: String, throwable: Throwable): Boolean = {
    val stackTrace = ExceptionUtils.getStackTrace(throwable)
    val sb: StringBuilder = new StringBuilder
    sb.append("Time: ").append(dateFormat.format(Calendar.getInstance.getTime)).append("\n")
    sb.append("Msg: ").append(message)
    sb.append("Stack Trace: \n")
      .append(stackTrace)
    notify(sb.toString(), users.split(",").toSeq)
  }

  def notifyFullInfo(message: String, users: Seq[String]): Boolean = {
    val sb: StringBuilder = new StringBuilder
    sb.append("Time: ").append(dateFormat.format(Calendar.getInstance.getTime)).append("\n")
    sb.append("Msg: ").append(message)
    val newMsg: String = sb.toString
    notify(newMsg, users)
  }


  def notify(message: String, users: Seq[String]): Boolean = users.map((user: String) => {
    try {
      SlackNotification.notify(slackWebhookUrl, user, message)
    } catch {
      case _: Throwable => false
    }
  }).toSet.contains(false)

  def notify(message: String): Boolean = notify(message, defaultUsers)

  def notifyFullInfo(message: String): Boolean = notifyFullInfo(message, defaultUsers)
}
