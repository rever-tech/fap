package worker.hadoop.file

import java.util.Calendar

import com.typesafe.config.Config
import org.apache.commons.lang.text.StrSubstitutor
import worker.hadoop.util.TimeUtil

import scala.collection.JavaConversions._

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

class TimeBasedStrategy(conf: Config) extends FileNamingStrategy(conf) {

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
    StrSubstitutor.replace(namePattern, Map[String, Any](
      "yyyy" -> cal.get(Calendar.YEAR),
      "dd" -> cal.get(Calendar.DAY_OF_MONTH),
      "MM" -> cal.get(Calendar.MONTH),
      "HH" -> cal.get(Calendar.HOUR_OF_DAY),
      "mm" -> cal.get(Calendar.MINUTE),
      "s" -> cal.get(Calendar.SECOND),
      "version" -> version,
      "topic" -> topic
    ))
  }

  /**
    *
    * @param file
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
    section.timestamp + interval < System.currentTimeMillis()
  }

//  final val previousSectionInfo: TrieMap[String, (String, Long)] = TrieMap()

  /**
    * Return name and timestamp of section
    *
    * @param topic name of the topic
    * @param time  current time
    * @return pair of section name and timestamp
    */
  override def getSectionInfo(topic: String, time: Long): (String, Long) = {
//    if(previousSectionInfo.containsKey(topic))
//    if (!(time < previousSectionInfo._2 + interval && previousSectionInfo._2 <= time)) {
//      val itvTime = TimeUtil.roundTimeByInterval(time, interval)
//      previousSectionInfo = (s"$topic-${TimeUtil.formatDateToMinute(itvTime)}", itvTime)
//    }
//    previousSectionInfo

    val itvTime = TimeUtil.roundTimeByInterval(time, interval)
    (s"$topic-${TimeUtil.formatDateToMinute(itvTime)}", itvTime)
  }
}