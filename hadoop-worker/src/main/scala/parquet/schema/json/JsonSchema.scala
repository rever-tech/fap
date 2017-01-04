package parquet.schema.json

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.collection.immutable.ListMap

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

object JsonSchema {
  def apply(name: String, version: Int, schemaString: String): JsonSchema = {
    JsonType(schemaString) match {
      case JsonObject(fields) => new JsonSchema(name, version, fields)
      case _ => throw new SchemaFormatException("Create schema from string require Object Type", schemaString)
    }
  }
}


case class NameAndType(name: String, jsonType: JsonType)

object JsonType {

  private val arrayTypePattern = "^\\s*\\[(.+)\\]\\s*$".r
  private val objectTypePattern = "^\\s*(\\{[\\w\\W]+\\})\\s*$".r
  private val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  /**
    * Parse schema from map of `field name` and `field type`
    *
    * @todo should throw exception when failure or return Option?
    * @param name    name of schema
    * @param version version of schema
    * @param fields  map of field name and it's type
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
          val tmp = objectMapper.readValue(obj, classOf[Map[String, Any]])
          val fields = ListMap(tmp.toSeq.sortBy(_._1): _*)
          JsonObject(fields.toSeq.map(field => NameAndType(field._1,
            JsonType(parseConfigValue(field._2)))))
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

  private def parseConfigValue(value: Any): String = value match {
    case map: Map[Any, Any] => objectMapper.writeValueAsString(map)
    case seq: Seq[Any] => objectMapper.writeValueAsString(seq)
    case str: String => str
    case _ => throw new SchemaFormatException("Data type does not supported", value.toString, null)
  }
}

class SchemaFormatException(msg: String, schema: String, parent: Throwable)
  extends Exception(s"$msg\nSchema: $schema", parent) {
  def this(schema: String, parent: Throwable) = this("Schema format error", schema, parent)

  def this(msg: String, schema: String) = this(msg, schema, null)
}