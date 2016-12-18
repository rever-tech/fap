package parquet.schema.json

import org.apache.parquet.schema.Type.Repetition
import org.scalatest.FunSuite

/**
  * Created by tiennt4 on 17/12/2016.
  */
class JsonSchemaConverterTest extends FunSuite{
  val ALL_PARQUET_SCHEMA =
    """message parquet.json.record {
      |  required group myarray (LIST) {
      |    repeated group list {
      |      required int32 element;
      |    }
      |  }
      |  required boolean myboolean;
      |  required double mydouble;
      |  required float myfloat;
      |  required int32 myint;
      |  required int64 mylong;
      |  required group mynestedrecord {
      |    required int32 mynestedint;
      |  }
      |  required group myrecordarray (LIST) {
      |    repeated group list {
      |      required group element {
      |        required int32 a;
      |        required int32 b;
      |      }
      |    }
      |  }
      |  required binary mystring (UTF8);
      |  required binary myobjasstring (UTF8);
      |}
      |""".stripMargin

  test("All type") {

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
        NameAndType("myobjasstring", JsonObjAsString())
      ))

    val parquetSchema = JsonSchemaConverter.convert(jsonSchema, Repetition.REQUIRED)
    assert(parquetSchema.toString == ALL_PARQUET_SCHEMA)
  }
}
