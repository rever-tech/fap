package worker.hadoop.writer

import java.util.Base64

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
      """{"mynestedint":39}""",
      """[{"a":3,"b":4},{"a":3,"b":4}]""",
      "hello world", """{"abc":323,"hello":"world"}""", "[12,313,13]").mkString("\t")
    val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)
    val data = objectMapper.readValue(record, classOf[Map[String, Any]])
    val writer = new TextFileWriter("src/test/resources/files/tmp_abc", jsonSchema, null)
    assert(writer.getFilePath.toString == "src/test/resources/files/tmp_abc.log")
    val string = writer.getLineData(record, data, jsonSchema)
//    writer.writeObject(record)
    writer.close()
    deleteFile(writer.getFilePath)
    assert(string == expectedString)
  }

  test("TextFileWriter getString should return correct value") {
    val encodedString = "eyIkb3MiOiAiV2luZG93cyIsIiRicm93c2VyIjogIkNocm9tZSIsIiRyZWZlcnJlciI6ICJodHRwOi8vcmV2ZXIudm4vbXVhL3ZpbGxhLTItdGFuZy1kb24tbGFwLW15LXBodS0xYSIsIiRjdXJyZW50X3VybCI6ICJodHRwOi8vcmV2ZXIudm4vbXVhL3BlbnRob3VzZS13MS1zdW5yaXNlLWNpdHkiLCIkYnJvd3Nlcl92ZXJzaW9uIjogNTUsIiRzY3JlZW5faGVpZ2h0IjogNzY4LCIkc2NyZWVuX3dpZHRoIjogMTM2NiwiJHRpdGxlIjogIlBlbnRob3VzZSBXMSBTdW5yaXNlIENpdHksIFJWMjE1IiwiJHRpbWVzdGFtcCI6IDE0ODQyMjg2NTY1MjAsInV1aWQiOiAiMTU5OTJlOGI3NjE4NS0wMDRmNzc2OTM4YmIwNS01ODEzM2IxNS0xMDAyMDAtMTU5OTJlOGI3NjIzMTQiLCIkaW5pdGlhbF9yZWZlcnJlciI6ICJodHRwOi8vcmV2ZXIudm4vdmlsbGEtZHVvbmctNDEtYmluaC1hbi1xdWFuLTItcnY5NzciLCIkaW5pdGlhbF9yZWZlcnJpbmdfZG9tYWluIjogInJldmVyLnZuIiwic3VidHlwZSI6ICJlbnRlciIsInByb3BlcnR5IjogeyJpZCI6ICIxNDgwOTUxMTYwOTIzXzI5NjYiLCJydmlkIjogIlJWMjE1IiwidGl0bGUiOiAiUGVudGhvdXNlIFcxIFN1bnJpc2UgQ2l0eSwgUlYyMTUiLCJwcmljZSI6IDMyMDAwMDAwMDAwLCJhcmVhIjogNDMwLCJzZXJ2aWNlX3R5cGUiOiAyLCJwcm9wZXJ0eV90eXBlIjogMSwiaGFzXzNkIjogdHJ1ZSwiZGlyZWN0aW9uIjogMiwiYWRkcmVzcyI6IHsic3RyZWV0IjogIk5ndXnhu4VuIEjhu691IFRo4buNIiwid2FyZCI6ICJUw6JuIEjGsG5nIiwiZGlzdHJpY3QiOiAiUXXhuq1uIDciLCJjaXR5IjogIkjhu5MgQ2jDrSBNaW5oIiwibmVpZ2hib3Job29kIjogIiIsImdlb2xvY2F0aW9uIjogeyJsYXQiOiAxMC43NDEzNDIsImxvbiI6IDEwNi43MDA3MzQwMDAwMDAwMX19fSwic291cmNlIjogInNpbWlsYXJfbmF2aWdhdGUifQ=="

    val decodedString = new String(Base64.getDecoder.decode(encodedString), "utf-8")

    val map = TextFileWriter.objectMapper.readValue(decodedString, classOf[Map[String, Any]])
    println(map)
    println(TextFileWriter.getString(decodedString, map, "address"))
    val file = new TextFileWriter("test.txt", JsonSchema("test", 1, Seq(NameAndType("property", JsonString()))), Map.empty[String, String])
    file.writeObject(decodedString)
    file.close()
  }
}
