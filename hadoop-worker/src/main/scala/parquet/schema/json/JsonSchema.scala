package parquet.schema.json

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions, ConfigValue, ConfigValueType}

import scala.collection.JavaConversions._

/**
  * Created by tiennt4 on 07/12/2016.
  */
trait JsonType

case class JsonObject(fields: Seq[NameAndType]) extends JsonType

case class JsonArray(elementType: JsonType) extends JsonType

case class JsonInt() extends JsonType

case class JsonBool() extends JsonType

case class JsonLong() extends JsonType

case class JsonFloat() extends JsonType

case class JsonDouble() extends JsonType

case class JsonString() extends JsonType



case class JsonSchema(name: String, version: Int, fields: Seq[NameAndType])

case class NameAndType(name: String, jsonType: JsonType)

object JsonType {

  private val arrayTypePattern = "^\\s*\\[(.+)\\]\\s*$".r
  private val objectTypePattern = "^\\s*(\\{[\\w\\W]+\\})\\s*$".r

  /**
    * Parse schema from map of `field name` and `field type`
    * @todo should throw exception when failure or return Option?
    * @param name name of schema
    * @param version version of schema
    * @param fields map of field name and it's type
    * @return
    */
  def parseSchema(name: String, version: Int, fields: Map[String, String]): JsonSchema = {
    val fieldsName = fields.keySet.toList.sorted
    JsonSchema(name, version, fieldsName.map(fName => NameAndType(fName, JsonType(fields(fName)))))
  }

  def apply(string: String): JsonType = {
    string.trim() match {
      case arrayTypePattern(childs) => JsonArray(JsonType(childs))
      case objectTypePattern(obj) =>
        try {
          val conf = ConfigFactory.parseString(obj)
          val fieldsName: List[String] = conf.root().keySet().toList.sorted
          JsonObject(fieldsName.map(name => NameAndType(name,
            JsonType(parseConfigValue(conf.getValue(name))))))
        } catch {
          case e: Throwable => throw new SchemaFormatException(obj, e)
        }
      case "int" => JsonInt()
      case "long" => JsonLong()
      case "float" => JsonFloat()
      case "double" => JsonDouble()
      case "bool" => JsonBool()
      case "string" => JsonString()
      case s => throw new SchemaFormatException("Data type does not supported", s, null)
    }
  }

  private def parseConfigValue(value: ConfigValue): String = value.valueType() match {
    case ConfigValueType.STRING => value.unwrapped().asInstanceOf[String]
    case ConfigValueType.OBJECT => value.render(ConfigRenderOptions.concise())
    case _ => throw new SchemaFormatException("Data type does not supported", value.toString, null)
  }
}

class SchemaFormatException(msg: String, schema: String, parent: Throwable)
  extends Exception(s"$msg\nSchema: $schema", parent) {
  def this(schema: String, parent: Throwable) = this("Schema format error", schema, parent)

  def this(msg: String, schema: String) = this(msg, schema, null)
}