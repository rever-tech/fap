package worker.hadoop.file

import java.util.Calendar
import java.util.concurrent.TimeUnit

import com.twitter.inject.Logging
import com.typesafe.config.Config
import org.apache.commons.lang.text.StrSubstitutor
import worker.hadoop.util.TimeUtil

import scala.collection.JavaConversions._
import scala.concurrent.duration.Duration

/**
  * Created by tiennt4 on 08/12/2016.
  */
abstract class FileNamingStrategy(conf: Config) {

  val interval: Long = conf.getDuration("interval").getSeconds * 1000
  val namePattern: String = conf.getString("filename_pattern")

  def getFileName(topic: String, version: String, time: Long): String

  def isFileNeedTobeSplit(file: DataFile): Boolean

  def isSectionEnd(section: DataSection): Boolean

  /**
    * Return name and timestamp of section
    *
    * @param topic name of the topic
    * @param time  current time
    * @return pair of section name and timestamp
    */
  def getSectionInfo(topic: String, time: Long): (String, Long)
}

class TimeBasedStrategy(conf: Config) extends FileNamingStrategy(conf) with Logging {

  info("===== TimeBasedStrategyFileNaming =====")
  info(s" Interval: ${Duration(interval, TimeUnit.MILLISECONDS).toMinutes}")
  info(s" Name Pattern: $namePattern")

  /**
    * Get file name that record belongs to
    *
    * @todo benchmark performance
    * @param topic   topic name of record
    * @param version version of record
    * @param time    timestamp of record
    * @return the name that record belongs to
    */
  override def getFileName(topic: String, version: String, time: Long): String = {
    val itvTime = TimeUtil.roundTimeByInterval(time, interval)
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(itvTime)
    debugResult("Get File Name: %s") {
      StrSubstitutor.replace(namePattern, Map[String, Any](
        "yyyy" -> f"${cal.get(Calendar.YEAR)}%04d",
        "dd" -> f"${cal.get(Calendar.DAY_OF_MONTH)}%02d",
        "MM" -> f"${cal.get(Calendar.MONTH) + 1}%02d",
        "HH" -> f"${cal.get(Calendar.HOUR_OF_DAY)}%02d",
        "mm" -> f"${cal.get(Calendar.MINUTE)}%02d",
        "s" -> f"${cal.get(Calendar.SECOND)}%02d",
        "version" -> version,
        "topic" -> topic
      ))
    }
  }

  /**
    *
    * @param file data file to check
    * @return
    */
  override def isFileNeedTobeSplit(file: DataFile): Boolean = false

  /**
    * Check if data section is finished
    *
    * @param section the section tobe check
    * @return section status
    */
  override def isSectionEnd(section: DataSection): Boolean = {
    debugResult("Is Section End? Section name: " + section.topicName
      + ", Section time: " + section.timestamp
      + ", Interval: " + interval + ", result: %s") {
      section.timestamp + interval < System.currentTimeMillis()
    }
  }

  /**
    * Return name and timestamp of section
    *
    * @param topic name of the topic
    * @param time  current time
    * @return pair of section name and timestamp
    */
  override def getSectionInfo(topic: String, time: Long): (String, Long) = {
    debugResult("get Section info: %s") {
      val itvTime = TimeUtil.roundTimeByInterval(time, interval)
      (s"$topic-${TimeUtil.formatDateToMinute(itvTime)}", itvTime)
    }
  }
}

class TimeBasedWithSizeStrategy(config: Config) extends TimeBasedStrategy(config) with Logging {

  override def isFileNeedTobeSplit(file: DataFile): Boolean = {
    file.writer.getCurrentSize()
    false
  }
}