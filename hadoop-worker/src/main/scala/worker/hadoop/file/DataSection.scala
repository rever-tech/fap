package worker.hadoop.file

import java.util.concurrent.atomic.AtomicInteger

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.kafka.clients.consumer.ConsumerRecord
import parquet.schema.json.{JsonInt, JsonSchema, JsonString, NameAndType}
import worker.hadoop.util.{ResourceControl, TimeUtil}
import worker.hadoop.writer.FileWriter

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.reflect.runtime.universe._


/**
  * Created by tiennt4 on 08/12/2016.
  */
class DataSection(val identity: String,
                  val uri: String, val topicName: String,
                  val timestamp: Long, val createdTime: Long,
                  val fileNamingStrategy: FileNamingStrategy,
                  val writerClass: String,
                  val writerConfig: Map[String, String] = null,
                  val metadata: Map[String, String] = Map.empty
                 ) {

  val sectionDir = new Path(uri, s"$identity-$topicName-${TimeUtil.formatDateToMinute(timestamp)}-${TimeUtil.formatDateToMinute(createdTime)}")

  val offsetInfo: mutable.Map[Int, Long] = mutable.Map.empty[Int, Long]

  final val finishedFiles: mutable.Buffer[DataFile] = mutable.Buffer.empty[DataFile]
  private[file] final val files: mutable.Map[String, DataFile] = mutable.Map.empty

  private final val fileNameCounter: TrieMap[String, AtomicInteger] = TrieMap.empty

  private def setOffsetInfo(offsets: Map[Int, Long]): Unit = {
    offsetInfo ++= offsets
  }

  private def addFinishedFiles(files: Seq[DataFile]): Unit = {
    finishedFiles ++= files
  }

  private[file] def getFullDataFileName(fileName: String): String = {
    val counter = fileNameCounter.get(fileName) match {
      case Some(count) => count.incrementAndGet()
      case None =>
        val tmp = new AtomicInteger(0)
        fileNameCounter(fileName) = tmp
        tmp.incrementAndGet()
    }
    s"$fileName-$identity-${TimeUtil.formatDateToMinute(timestamp)}-$counter"
  }

  private def createDataFile(fileName: String, schema: Option[JsonSchema], metadata: Map[String, String]): DataFile = {
    val name = getFullDataFileName(fileName)
    val fullName = new Path(sectionDir, name)
    val constructor = Class.forName(writerClass)
      .getConstructor(classOf[String], classOf[JsonSchema], classOf[Map[String, String]])
    val fileWriter = constructor.newInstance(fullName.toString, schema.getOrElse(DataSection.defaultSchema), writerConfig)
      .asInstanceOf[FileWriter[String]]

    DataFile(fileWriter.getFileNameWithExtension(name), fileWriter.getFileNameWithExtension, fileWriter)
  }

  /**
    * Sequentially called
    *
    * @param schema schema of this record (used when writer for file is not created)
    * @param record record data
    */
  def write(schema: Option[JsonSchema], record: ConsumerRecord[Integer, String]): Unit = {
    val (fileName, recordValue) = schema match {
      case Some(s) => (fileNamingStrategy.getFileName(record.topic(), record.key().toString, record.timestamp()), record.value())
      case None => (fileNamingStrategy.getFileName(record.topic(), "unknown", record.timestamp()),
        s"""{"topic": "${record.topic()}", "version": ${record.key()}, "value": ${Literal(Constant(record.value())).toString}}""")
    }
    if (!files.contains(fileName)) {
      synchronized {
        if (!files.contains(fileName)) {
          files(fileName) = createDataFile(fileName, schema, metadata)
        }
      }
    }
    var file = files(fileName)
    if (fileNamingStrategy.isFileNeedTobeSplit(file)) {
      //Create new writer
      file.close()
      finishedFiles += file
      file = createDataFile(fileName, schema, Map.empty)
      files(fileName) = file
    }
    files(fileName).write(recordValue)
    offsetInfo(record.partition()) = record.offset()
  }

  def ensureClose(): Unit = {
    files.foreach(e => {
      e._2.close()
      finishedFiles += e._2
    })
    files.clear()
    //Write offset info
    val metaFile = new Path(sectionDir, DataSection.METADATA_FILE_NAME)
    ResourceControl.using(metaFile.getFileSystem(new Configuration()).create(metaFile)) {
      metaWriter =>
        metaWriter.writeBytes(getMetaString)
    }
  }

  def clean(): Unit = {
    sectionDir.getFileSystem(new Configuration()).delete(sectionDir, true)
  }

  def getMetaString: String =
    s"""{
       |  "topic": "${topicName}",
       |  "timestamp": ${timestamp},
       |  "createdTime": ${createdTime},
       |  "identity": "${identity}",
       |  "uri": "${uri}",
       |  "offset": [
       |    ${offsetInfo.map(partition => s"""{"partition": ${partition._1}, "offset": ${partition._2}}""").mkString(",\n")}
       |  ],
       |  "metadata": {
       |    ${metadata.map(meta => s""""${meta._1}": "${meta._2}"""").mkString(",\n")}
       |  },
       |  "finishedFiles" : [
       |    ${finishedFiles.map(_.toJson()).mkString(",\n")}
       |  ]
       |}""".stripMargin


  override def toString: String = {
    s"""{"sectionDir": "${sectionDir.toString}", "topicName": "$topicName"}"""
  }
}

object DataSection {
  val METADATA_FILE_NAME = "_SUCCESS"
  private val objectMapper: ObjectMapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)

  def apply(json: String): DataSection = {
    val data = objectMapper.readValue(json, classOf[Map[String, Any]])
    val identity = data("identity").asInstanceOf[String]
    val uri = data("uri").asInstanceOf[String]
    val topicName = data("topic").asInstanceOf[String]
    val timestamp = data("timestamp").asInstanceOf[Long]
    val createdTime = data("createdTime").asInstanceOf[Long]
    val offsets: Map[Int, Long] = data("offset").asInstanceOf[Seq[Map[String, Any]]]
      .map(e => e("partition").asInstanceOf[Int] -> e("offset").asInstanceOf[Long]).toMap
    val metadata = data("metadata").asInstanceOf[Map[String, String]]

    val section = apply(identity, uri, topicName, timestamp, createdTime,
      fileNamingStrategy = null, writerClass = null, writerConfig = null, metadata = metadata)

    section.setOffsetInfo(offsets)
    val files = data("finishedFiles").asInstanceOf[Seq[Map[String, Any]]].map(e =>
      DataFile(e("name").asInstanceOf[String], e("fullPath").asInstanceOf[String], null)
    )
    section.addFinishedFiles(files)
    section
  }

  def apply(identity: String,
            uri: String, topicName: String,
            timestamp: Long, createdTime: Long,
            fileNamingStrategy: FileNamingStrategy,
            writerClass: String,
            writerConfig: Map[String, String] = null,
            metadata: Map[String, String] = Map.empty): DataSection =
    new DataSection(identity, uri, topicName, timestamp, createdTime,
      fileNamingStrategy, writerClass, writerConfig, metadata)

  private final val defaultSchema: JsonSchema =
    JsonSchema("default", 1,
      Seq(
        NameAndType("topic", JsonString()),
        NameAndType("value", JsonString()),
        NameAndType("version", JsonInt())
      ))
}