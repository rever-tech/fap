package worker.hadoop.writer

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.scalatest.FunSuite
import parquet.schema.json._
import util.TestUtil

/**
  * Created by tiennt4 on 05/01/2017.
  */
class FileWriterTest extends FunSuite with TestUtil {

  val schema = JsonSchema("test", 1, Seq(NameAndType("abc", JsonString())))

  test("ParquetFileWriter should create correct writer") {
    val writer = new ParquetFileWriter("src/test/resources/files/tmp_abc", schema, null)
    assert(writer.getFilePath.toString == "src/test/resources/files/tmp_abc.parquet")
    writer.close()
    deleteFile(writer.getFilePath)
  }

  test("TextFileWriter should create correct writer") {
    val writer = new TextFileWriter("src/test/resources/files/tmp_abc", schema, null)
    assert(writer.getFilePath.toString == "src/test/resources/files/tmp_abc.log")
    writer.close()
    deleteFile(writer.getFilePath)
  }

  test("TextFileWriter getLineData should correct") {
    val jsonSchema = JsonSchema("parquet.json.record", 1,
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
        NameAndType("objasstring", JsonString()),
        NameAndType("arrayasstring", JsonString())
      ))

    val record =
      """{
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
        |  "objasstring": {
        |    "abc": 323,
        |    "hello": "world"
        |  },
        |  "arrayasstring": [12,313,13]
        |}""".stripMargin

    val expectedString = Seq("[1,2,4,5]",
      "false", "12.3", "12.3", "3", "1481956237000",
      """{\"mynestedint\":39}""",
      """[{\"a\":3,\"b\":4},{\"a\":3,\"b\":4}]""",
      "hello world", """{\"abc\":323,\"hello\":\"world\"}""", "[12,313,13]").mkString("\t")
    val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)
    val data = objectMapper.readValue(record, classOf[Map[String, Any]])
    val writer = new TextFileWriter("src/test/resources/files/tmp_abc", jsonSchema, null)
    assert(writer.getFilePath.toString == "src/test/resources/files/tmp_abc.log")
    val string = writer.getLineData(data, jsonSchema)
//    writer.writeObject(record)
    writer.close()
    deleteFile(writer.getFilePath)
    assert(string == expectedString)
  }
}
