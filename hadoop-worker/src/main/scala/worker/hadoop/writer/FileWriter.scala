package worker.hadoop.writer

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import parquet.schema.json.JsonSchema
import parquet.writer.JsonParquetWriter

/**
  * Created by tiennt4 on 05/01/2017.
  */

/**
  * Support for write data json to file
  *
  * @param filePath Full path of file, without file extension (auto add based on implementation)
  * @param schema   Schema of file
  * @param conf     Configuration for writer
  * @tparam T Type of data
  */
abstract class FileWriter[T](filePath: String, schema: JsonSchema, conf: Map[String, String]) extends java.io.Closeable {

  /**
    * File extension, must define when implement
    */
  val fileExtension: String

  /**
    * Write object to file
    *
    * @param obj
    */
  def writeObject(obj: T): Unit

  /**
    * Get current size of file (total size, includes in-memory size)
    *
    * @return
    */
  def getCurrentSize(): Long

  /**
    * Get file path with it's extension
    *
    * @return
    */
  def getFileNameWithExtension: String = filePath + "." + fileExtension

  def getFileNameWithExtension(baseName: String): String = baseName + "." + fileExtension
}

class ParquetFileWriter(filePath: String, schema: JsonSchema, conf: Map[String, String]) extends FileWriter[String](filePath, schema, conf) {
  override val fileExtension: String = "parquet"

  private[writer] val writer = {
    val builder = JsonParquetWriter.builder(getFileNameWithExtension).withSchema(schema)
    if (conf != null) {
      val hadoopConf = new Configuration()
      conf.foreach(c => hadoopConf.set(c._1, c._2))
      builder.withConf(hadoopConf)
    }
    builder.build()
  }

  /**
    * For Test
    */
  private[writer] final val getFilePath: Path = new Path(getFileNameWithExtension)


  override def writeObject(obj: String): Unit = writer.write(obj)

  override def close(): Unit = writer.close()

  override def getCurrentSize(): Long = writer.getDataSize


}

class TextFileWriter(filePath: String, schema: JsonSchema, conf: Map[String, String]) extends FileWriter[String](filePath, schema, conf) {
  override val fileExtension: String = "log"

  private var currentSize: Long = 0

  private val outputStream = {
    val hadoopConf = {
      val tmp = new Configuration()
      if (conf != null) {
        conf.foreach(c => tmp.set(c._1, c._2))
      }
      tmp
    }
    val path = new Path(getFileNameWithExtension)
    path.getFileSystem(hadoopConf).create(path)
  }


  /**
    * For Test
    */
  private[writer] final val getFilePath: Path = new Path(getFileNameWithExtension)

  override def writeObject(obj: String): Unit = {
    val data = TextFileWriter.objectMapper.readValue(obj, classOf[Map[String, Any]])
    val bytes = getLineData(obj, data, schema).getBytes("UTF-8")
    if (currentSize > 0) {
      outputStream.writeChar('\n')
      currentSize += 1
    }
    currentSize += bytes.length
    outputStream.write(bytes)
  }

  /**
    * @note Not check data type
    * @param data
    * @param schema
    * @return
    */
  private[writer] def getLineData(original: String, data: Map[String, Any], schema: JsonSchema): String =
    schema.fields.map(nameAndType => {
      TextFileWriter.getString(original, data, nameAndType.name)
    }).mkString("\t")

  override def close(): Unit = {
    outputStream.flush()
    outputStream.close()
  }

  override def getCurrentSize(): Long = currentSize


}

object TextFileWriter {

  val objectMapper: ObjectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def getString(original: String, data: Map[String, Any], fieldName: String): String = {
    val fieldValueAsString = fieldName match {
      case "_" => original
      case fname => data.get(fname) match {
        case Some(value) => value match {
          case null => "" // Why?
          case map: Map[Any, Any] => objectMapper.writeValueAsString(map)
          case seq: Seq[Any] => objectMapper.writeValueAsString(seq)
          case other => other.toString
        }
        case None => ""
      }
    }

    fieldValueAsString.replaceAll("[\t\r\n]", "")
  }
}

/**
  * This writer is schema-independence
  *
  * @param filePath Full path of file, without file extension (auto add based on implementation)
  * @param schema   Schema of file
  * @param conf     Configuration for writer
  */
class JsonFileWriter(filePath: String, schema: JsonSchema, conf: Map[String, String]) extends FileWriter[String](filePath, schema, conf) {
  /**
    * File extension, must define when implement
    */
  override val fileExtension: String = "json"

  private val outputStream = {
    val hadoopConf = {
      val tmp = new Configuration()
      if (conf != null) {
        conf.foreach(c => tmp.set(c._1, c._2))
      }
      tmp
    }
    val path = new Path(getFileNameWithExtension)
    path.getFileSystem(hadoopConf).create(path)
  }

  /**
    * Write object to file
    *
    * @param obj string represent json object
    */
  override def writeObject(obj: String): Unit = {
    val bytes = obj.getBytes("UTF-8")
    if (outputStream.size() > 0) {
      outputStream.writeChar('\n')
    }
    outputStream.write(bytes)
  }

  /**
    * Get current size of file (total size, includes in-memory size)
    *
    * @return
    */
  override def getCurrentSize(): Long = outputStream.size()

  override def close(): Unit = {
    outputStream.flush()
    outputStream.close()
  }
}