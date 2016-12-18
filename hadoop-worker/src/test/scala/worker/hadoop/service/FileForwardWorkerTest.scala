package worker.hadoop.service

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.Path
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.record.TimestampType
import org.scalatest.FunSuite
import parquet.schema.json._
import util.TestUtil
import worker.hadoop.file.{DataSection, TimeBasedStrategy}
import worker.hadoop.util.FileSystemConfig

/**
  * Created by tiennt4 on 18/12/2016.
  */
class FileForwardWorkerTest extends FunSuite with TestUtil {

  val conf = ConfigFactory.parseString(
    """
      |{
      | fs.buffersize = 4194304
      |}
    """.stripMargin)

  val fileForwardWorker: FileForwardWorker = new FileForwardWorker(conf)
  val srcTest = FileSystemConfig("", ConfigFactory.parseString("{}"))
  val destTest = FileSystemConfig("src/test/resources/forwarded", ConfigFactory.parseString("{}"))


  test("forward should valid checksum") {

    val sectionTimestamp = 1481947200000l
    val sectionCreatedTime = System.currentTimeMillis()
    val fileNamingConf = ConfigFactory.parseString(
      """
        |{
        | "interval": 30m,
        | "filename_pattern": "${topic}/${yyyy}/${MM}/${dd}/${HH}/${topic}-v${version}"
        |}
      """.stripMargin)
    val section = DataSection("test_section", "src/test/resources/sections", "test_topic",
      sectionTimestamp, sectionCreatedTime, new TimeBasedStrategy(fileNamingConf))

    val recordTimeStamp = System.currentTimeMillis()
    val v1Schema = JsonSchema("test_topic", 1,
      Seq(
        NameAndType("myarray", JsonArray(JsonInt())),
        NameAndType("myboolean", JsonBool()),
        NameAndType("mydouble", JsonDouble()),
        NameAndType("myfloat", JsonFloat()),
        NameAndType("myint", JsonInt()),
        NameAndType("mylong", JsonLong()),
        NameAndType("mynestedrecord", JsonObject(Seq(NameAndType("mynestedint", JsonInt())))),
        NameAndType("myrecordarray", JsonArray(JsonObject(Seq(NameAndType("a", JsonInt()), NameAndType("b", JsonInt()))))),
        NameAndType("mystring", JsonString()),
        NameAndType("myobjasstring", JsonObjAsString())
      ))

    val recordValue =
      """
        |{
        |  "myarray": [1,2,4,5],
        |  "myboolean": false,
        |  "mydouble": 12.3,
        |  "myfloat": 12.3,
        |  "myint": 3,
        |  "mylong": 1481956237000,
        |  "mynestedrecord": {
        |    "mynestedint": 39
        |  },
        |  "myrecordarray": [
        |    {
        |      "a": 3,
        |      "b": 4
        |    },
        |    {
        |      "a": 3,
        |      "b": 4
        |    }
        |  ],
        |  "mystring": "hello world",
        |  "myobjasstring": {
        |    "abc": 323,
        |    "hello": "world"
        |  }
        |}
      """.stripMargin
    val record1 = new ConsumerRecord[Integer, String]("test_topic", 1, 1, recordTimeStamp, TimestampType.CREATE_TIME, -1, -1, -1, 1, recordValue)
    val record2 = new ConsumerRecord[Integer, String]("test_topic", 1, 2, recordTimeStamp, TimestampType.CREATE_TIME, -1, -1, -1, 1, recordValue)
    val record3 = new ConsumerRecord[Integer, String]("test_topic", 1, 3, recordTimeStamp, TimestampType.CREATE_TIME, -1, -1, -1, 1, recordValue)
    val record4 = new ConsumerRecord[Integer, String]("test_topic", 1, 4, recordTimeStamp, TimestampType.CREATE_TIME, -1, -1, -1, 1, recordValue)
    val record5 = new ConsumerRecord[Integer, String]("test_topic", 1, 5, recordTimeStamp, TimestampType.CREATE_TIME, -1, -1, -1, 1, recordValue)
    val record6 = new ConsumerRecord[Integer, String]("test_topic", 1, 6, recordTimeStamp, TimestampType.CREATE_TIME, -1, -1, -1, 1, recordValue)
    val record7 = new ConsumerRecord[Integer, String]("test_topic", 1, 7, recordTimeStamp, TimestampType.CREATE_TIME, -1, -1, -1, 1, recordValue)

    section.write(None, record1)
    section.write(Some(v1Schema), record2)
    section.write(Some(v1Schema), record3)
    section.write(Some(v1Schema), record4)
    section.write(Some(v1Schema), record5)
    section.write(Some(v1Schema), record6)
    section.write(Some(v1Schema), record7)

    fileForwardWorker.forward(srcTest, destTest, section)

    section.finishedFiles.foreach(file => {
      val destPath = new Path(destTest.uri, file.name)
      val srcPath = new Path(file.fullPath)
      assertFileExist(destPath)
      assert(getMd5HexOfFile(srcPath) == getMd5HexOfFile(destPath))
    })
    section.clean()
  }
}
