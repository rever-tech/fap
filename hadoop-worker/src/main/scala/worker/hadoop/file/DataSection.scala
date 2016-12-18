package worker.hadoop.file

import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.kafka.clients.consumer.ConsumerRecord
import parquet.schema.json.{JsonInt, JsonSchema, JsonString, NameAndType}
import parquet.writer.JsonParquetWriter
import worker.hadoop.util.{ResourceControl, TimeUtil}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

/**
  * Created by tiennt4 on 08/12/2016.
  */
case class DataSection(identity: String,
                       uri: String, topicName: String,
                       timestamp: Long, createdTime: Long,
                       fileNamingStrategy: FileNamingStrategy,
                       metadata: Map[String, String] = Map.empty) {

  val sectionDir = new Path(uri, s"$identity-$topicName-${TimeUtil.formatDateToMinute(timestamp)}-${TimeUtil.formatDateToMinute(createdTime)}")

  private final val METADATA_FILE_NAME = "_SUCCESS"

  val offsetInfo: mutable.Map[Int, Long] = mutable.Map.empty[Int, Long]

  final val finishedFiles: mutable.Buffer[DataFile] = mutable.Buffer.empty[DataFile]
  private[file] final val files: mutable.Map[String, DataFile] = mutable.Map.empty

  private final val fileNameCounter: TrieMap[String, AtomicInteger] = TrieMap.empty

  private final val defaultSchema: JsonSchema =
    JsonSchema("default", 1,
      Seq(
        NameAndType("topic", JsonString()),
        NameAndType("value", JsonString()),
        NameAndType("version", JsonInt())
      ))

  private[file] def getFullDataFileName(fileName: String): String = {
    val counter = fileNameCounter.get(fileName) match {
      case Some(count) => count.incrementAndGet()
      case None =>
        val tmp = new AtomicInteger(0)
        fileNameCounter(fileName) = tmp
        tmp.incrementAndGet()
    }
    s"$fileName-$identity-${TimeUtil.formatDateToMinute(timestamp)}-$counter.parquet"
  }

  private def createDataFile(fileName: String, schema: Option[JsonSchema], metadata: Map[String, String]): DataFile = {
    val name = getFullDataFileName(fileName)
    val fullName = new Path(sectionDir, name)

    DataFile(name, fullName.toString, JsonParquetWriter.builder(fullName)
      .withSchema(schema match {
        case Some(s) => s
        case None => defaultSchema
      })
      .build())
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
        s"""{"topic": "${record.topic()}", "version": ${record.key()}, "value": ${s"""${record.value()}"""}}""")
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
    val metaFile = new Path(sectionDir, METADATA_FILE_NAME)
    ResourceControl.using(metaFile.getFileSystem(new Configuration()).create(metaFile)) {
      metaWriter =>
        metaWriter.writeChars(
          s"""
             |{
             |  "topic": "${topicName}",
             |  "timestamp": ${timestamp},
             |  "createdTime": ${createdTime},
             |  "identity": "${identity}",
             |  "uri": "${uri}",
             |  "offset": {
             |    ${offsetInfo.map(partition => s"""{"partition": ${partition._1}, "offset": ${partition._2}}""").mkString(",\n")}
             |  },
             |  "metadata": {
             |    ${metadata.map(meta => s""""${meta._1}": "${meta._2}"""").mkString(",\n")}
             |  }
             |}
         """.stripMargin)
    }
  }

  def clean(): Unit = {
    sectionDir.getFileSystem(new Configuration()).delete(sectionDir, true)
  }
}
