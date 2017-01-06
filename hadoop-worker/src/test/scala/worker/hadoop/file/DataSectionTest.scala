package worker.hadoop.file

import java.util.Calendar

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.Path
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.record.TimestampType
import org.scalatest.FunSuite
import parquet.schema.json._
import util.TestUtil

/**
  * Created by tiennt4 on 17/12/2016.
  */
class DataSectionTest extends FunSuite with TestUtil {
  val writerClass = "worker.hadoop.writer.ParquetFileWriter"
  val sectionTimestamp = {
    val tmpCal = Calendar.getInstance()
    tmpCal.set(2016, 11, 17, 11, 0, 0)
    tmpCal.set(Calendar.MILLISECOND, 0)
    tmpCal.getTimeInMillis
  }
  private val sectionCreatedTime = System.currentTimeMillis()
  private val fileNamingConf = ConfigFactory.parseString(
    """
      |{
      | "interval": 30m,
      | "filename_pattern": "${topic}/${yyyy}/${MM}/${dd}/${HH}/${topic}-v${version}"
      |}
    """.stripMargin)
  val section = DataSection("test_section", "src/test/resources/sections", "test_topic",
    sectionTimestamp, sectionCreatedTime, new TimeBasedStrategy(fileNamingConf), writerClass)

  test("Section directory should correct") {
    val createTimeCal = Calendar.getInstance()
    createTimeCal.setTimeInMillis(sectionCreatedTime)
    assert(section.sectionDir == new Path("src/test/resources/sections",
      f"test_section-test_topic-2016_12_17_11_00-" +
        f"${createTimeCal.get(Calendar.YEAR)}%04d_" +
        f"${createTimeCal.get(Calendar.MONTH) + 1}%02d_" +
        f"${createTimeCal.get(Calendar.DAY_OF_MONTH)}%02d_" +
        f"${createTimeCal.get(Calendar.HOUR_OF_DAY)}%02d_" +
        f"${createTimeCal.get(Calendar.MINUTE)}%02d"))
  }

  test("Write and close should succeed") {
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
        NameAndType("mystring", JsonString())
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

    assert(section.files.size == 2)
    assert(section.finishedFiles.isEmpty)
    section.ensureClose()
    assert(section.files.isEmpty)
    assert(section.finishedFiles.size == 2)


    assertFileExist(new Path(section.sectionDir,
      s"${section.fileNamingStrategy.getFileName("test_topic", "1", recordTimeStamp)}-${section.identity}-2016_12_17_11_00-1.parquet"))
    assertFileExist(new Path(section.sectionDir,
      s"${section.fileNamingStrategy.getFileName("test_topic", "unknown", recordTimeStamp)}-${section.identity}-2016_12_17_11_00-1.parquet"))
    assertFileExist(new Path(section.sectionDir,
      "_SUCCESS"))
    section.clean()
    assertFileNotExist(section.sectionDir)
  }


}
