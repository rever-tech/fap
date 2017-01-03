package parquet.schema.json

import org.scalatest.FunSuite

/**
  * Created by tiennt4 on 09/12/2016.
  */
class JsonSchemaTest extends FunSuite {
  test("JsonType apply primitive type should succeed") {
    assert(JsonType("int") == JsonInt())
    assert(JsonType("long") == JsonLong())
    assert(JsonType("float") == JsonFloat())
    assert(JsonType("double") == JsonDouble())
    assert(JsonType("bool") == JsonBool())
    assert(JsonType("string") == JsonString())
  }

  test("JsonType apply on unsupported type should throw SchemaFormatException") {
    intercept[SchemaFormatException] {
      JsonType("abc")
    }
  }

  test("JsonType apply on array of primitive type should succeed") {
    assert(JsonType("[ int ]") == JsonArray(JsonInt()))
    assert(JsonType("[long]") == JsonArray(JsonLong()))
    assert(JsonType("[float]") == JsonArray(JsonFloat()))
    assert(JsonType("[double]") == JsonArray(JsonDouble()))
    assert(JsonType("[bool]") == JsonArray(JsonBool()))
    assert(JsonType("[string]") == JsonArray(JsonString()))
  }

  test("JsonType apply on object of primitive types should succeed") {
    val obj =
      """{
        |   "int": "int",
        |   "long": "long",
        |   "float": "float",
        |   "double": "double",
        |   "bool": "bool",
        |   "string": "string",
        |   "obj_as_string": "obj_as_string"
        |}""".stripMargin

    val real = JsonType(obj)
    val expect = JsonObject(Seq(
      NameAndType("bool", JsonBool()),
      NameAndType("double", JsonDouble()),
      NameAndType("float", JsonFloat()),
      NameAndType("int", JsonInt()),
      NameAndType("long", JsonLong()),
      NameAndType("string", JsonString())
    ))
    assert(real == expect)
  }

  test("JsonType apply on object types should succeed") {
    val obj =
      """{
        |   "intarr": "[int]",
        |   "obj": {
        |     "long": "long",
        |     "bool": "[bool]"
        |   },
        |   "float": "float",
        |   "double": "double",
        |   "bool": "bool",
        |   "string": "string",
        |   "obj_as_string": "obj_as_string"
        |}""".stripMargin

    val real = JsonType(obj)
    val expect = JsonObject(Seq(
      NameAndType("bool", JsonBool()),
      NameAndType("double", JsonDouble()),
      NameAndType("float", JsonFloat()),
      NameAndType("intarr", JsonArray(JsonInt())),
      NameAndType("obj", JsonObject(Seq(
        NameAndType("bool", JsonArray(JsonBool())),
        NameAndType("long", JsonLong())
      ))),
      NameAndType("string", JsonString())
    ))
    assert(real == expect)
  }

  test("JsonType parse schema should succeed") {
    val data = Map(
      "intarr" -> "[int]",
      "obj" ->
        """{
        "long": "long",
        "bool": "[bool]"
      }""",
      "float" -> "float",
      "double" -> "double",
      "bool" -> "bool",
      "string" -> "string",
      "obj_as_string" -> "obj_as_string"
    )
    assert(JsonType.parseSchema("test", 1, data) == JsonSchema("test", 1, Seq(
      NameAndType("bool", JsonBool()),
      NameAndType("double", JsonDouble()),
      NameAndType("float", JsonFloat()),
      NameAndType("intarr", JsonArray(JsonInt())),
      NameAndType("obj", JsonObject(Seq(
        NameAndType("bool", JsonArray(JsonBool())),
        NameAndType("long", JsonLong())
      ))),
      NameAndType("string", JsonString())
    )))
  }
}
