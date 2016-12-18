package worker.hadoop.util

import com.typesafe.config.{Config, ConfigFactory, ConfigValueType}
import org.apache.hadoop.conf.Configuration

import scala.collection.JavaConversions._


/**
  * Created by tiennt4 on 10/12/2016.
  */
class FileSystemConfig(val uri: String, val conf: Configuration)

object FileSystemConfig {
  def apply(uri: String, conf: Config): FileSystemConfig = {
    val fsConf = new Configuration(true)
    conf.entrySet().toList
      .foreach(e => {
        e.getValue.valueType() match {
          case ConfigValueType.OBJECT => //?
          case ConfigValueType.LIST => //TODO: Implement config list
          case ConfigValueType.NUMBER => fsConf.set(e.getKey, e.getValue.unwrapped().toString)
          case ConfigValueType.BOOLEAN => fsConf.setBoolean(e.getKey, e.getValue.unwrapped().asInstanceOf[Boolean])
          case ConfigValueType.STRING => fsConf.set(e.getKey, e.getValue.unwrapped().asInstanceOf[String])
          case ConfigValueType.NULL => //Ignore
        }
      })
    new FileSystemConfig(uri, fsConf)
  }

  def apply(conf: Config): FileSystemConfig = {
    val uri = if (conf.hasPath("uri")) conf.getString("uri") else ""
    val config = if (conf.hasPath("conf")) conf.getConfig("conf") else ConfigFactory.parseString("{}")
    this (uri, config)
  }
}
