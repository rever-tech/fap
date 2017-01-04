package parquet.writer

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext
import org.apache.parquet.io.api.{Binary, RecordConsumer}
import org.apache.parquet.schema.{GroupType, Type}
import parquet.schema.json._

import scala.collection.JavaConversions._

/**
  * Created by tiennt4 on 07/12/2016.
  */
class JsonWriteSupport(schema: JsonSchema, conf: Configuration)
  extends WriteSupport[String] {

  private val objectMapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
  private val messageType = JsonSchemaConverter.convert(schema)
  var recordConsumer: RecordConsumer = _
  var listWriter: ListWriter = _

  override def init(configuration: Configuration): WriteContext = {
    val writeOldListStructure = configuration.getBoolean(
      JsonWriteSupport.WRITE_OLD_LIST_STRUCTURE,
      JsonWriteSupport.WRITE_OLD_LIST_STRUCTURE_DEFAULT)

    if (writeOldListStructure)
      this.listWriter = new TwoLevelListWriter
    else
      this.listWriter = new ThreeLevelListWriter

    new WriteContext(messageType, Map.empty[String, String])
  }

  override def write(record: String): Unit = {
    val data = objectMapper.readValue(record, classOf[Map[String, Any]])
    recordConsumer.startMessage()
    writeMessageFields(schema.fields, messageType.getFields, data)
    recordConsumer.endMessage()
  }

  private def writeMessageFields(jsonFields: Seq[NameAndType],
                                 messageFields: Seq[Type],
                                 data: Map[String, Any]) = {
    var index = 0
    while (index < jsonFields.size) {
      val jsonField = jsonFields(index)
      getValue(jsonField.jsonType, jsonField.name, data) match {
        case Some(value) =>
          recordConsumer.startField(jsonField.name, index)
          writeValue(jsonField.jsonType, messageFields(index), value)
          recordConsumer.endField(jsonField.name, index)
        case None =>
          if (messageFields(index).isRepetition(Type.Repetition.REQUIRED))
            throw new RuntimeException("Null-value for required field: " + jsonField.name)
      }
      index += 1
    }
  }

  private def getValue(dataType: JsonType, fieldName: String, data: Map[String, Any]): Option[Any] = dataType match {
    case JsonObject(_) => data.get(fieldName).map(_.asInstanceOf[Map[String, Any]])
    case JsonArray(_) => data.get(fieldName).map(_.asInstanceOf[Seq[Any]])
    case JsonBool() => data.get(fieldName).map(_.asInstanceOf[Boolean])
    case JsonInt() => data.get(fieldName).map(TypeConverter.toInt)
    case JsonLong() => data.get(fieldName).map(TypeConverter.toLong)
    case JsonFloat() => data.get(fieldName).map(TypeConverter.toFloat)
    case JsonDouble() => data.get(fieldName).map(TypeConverter.toDouble)
    case JsonString() => data.get(fieldName).map(getString)
  }

  private def writeValue(jsonType: JsonType, messageType: Type, value: Any) = jsonType match {
    case JsonBool() => recordConsumer.addBoolean(value.asInstanceOf[Boolean])
    case JsonInt() => recordConsumer.addInteger(value.asInstanceOf[Int])
    case JsonLong() => recordConsumer.addLong(value.asInstanceOf[Long])
    case JsonFloat() => recordConsumer.addFloat(value.asInstanceOf[Float])
    case JsonDouble() => recordConsumer.addDouble(value.asInstanceOf[Double])
    case JsonString() => recordConsumer.addBinary(Binary.fromCharSequence(TypeConverter.toString(value)(objectMapper)))
    case JsonArray(elementType) => listWriter.writeList(elementType, messageType.asGroupType(), value.asInstanceOf[Seq[Any]])
    case JsonObject(_) =>
      writeRecord(jsonType.asInstanceOf[JsonObject],
        messageType.asGroupType(), value.asInstanceOf[Map[String, Any]])
  }

  private def writeObjectFields(schema: JsonObject, parquetSchema: GroupType, data: Map[String, Any]): Unit = {
    writeMessageFields(schema.fields, parquetSchema.getFields, data)
  }

  private def writeRecord(schema: JsonObject, parquetSchema: GroupType, data: Map[String, Any]) {
    recordConsumer.startGroup()
    writeObjectFields(schema, parquetSchema, data)
    recordConsumer.endGroup()
  }

  override def prepareForWrite(recordConsumer: RecordConsumer): Unit = {
    this.recordConsumer = recordConsumer
  }

  private def getString(value: Any): String = value match {
    case str: String => str
    case any => any.toString
  }

  abstract private[parquet] class ListWriter {

    protected def writeSeq(elementType: JsonType, schema: GroupType, values: Seq[Any])

    def writeList(elementType: JsonType, schema: GroupType, values: Seq[Any]) {
      recordConsumer.startGroup() // group wrapper (original type LIST)
      writeSeq(elementType, schema, values)
      recordConsumer.endGroup()
    }
  }

  /**
    * For backward-compatibility. This preserves how lists were written in 1.x.
    */
  private[parquet] class TwoLevelListWriter extends ListWriter {
    override def writeSeq(elementType: JsonType, schema: GroupType, values: Seq[Any]): Unit = {
      if (values.nonEmpty) {
        recordConsumer.startField(JsonWriteSupport.OLD_LIST_REPEATED_NAME, 0)
        try
            for (elt <- values) {
              writeValue(elementType, schema.getType(0), elt)
            }
        catch {
          case e: NullPointerException =>
            // find the null element and throw a better error message
            var i = 0
            for (elt <- values) {
              if (elt == null) throw new NullPointerException("Array contains a null element at " + i + "\n" + "Set parquet.avro.write-old-list-structure=false to turn " + "on support for arrays with null elements.")
              i += 1
            }
            // no element was null, throw the original exception
            throw e
        }
        recordConsumer.endField(JsonWriteSupport.OLD_LIST_REPEATED_NAME, 0)
      }
    }
  }

  private[parquet] class ThreeLevelListWriter extends ListWriter {

    override protected def writeSeq(elementType: JsonType, schema: GroupType, values: Seq[Any]): Unit = {
      if (values.nonEmpty) {
        recordConsumer.startField(JsonWriteSupport.LIST_REPEATED_NAME, 0)
        val repeatedType = schema.getType(0).asGroupType
        val elementMsgType = repeatedType.getType(0)
        for (element <- values) {
          recordConsumer.startGroup() // repeated group array, middle layer
          if (element != null) {
            recordConsumer.startField(JsonWriteSupport.LIST_ELEMENT_NAME, 0)
            writeValue(elementType, elementMsgType, element)
            recordConsumer.endField(JsonWriteSupport.LIST_ELEMENT_NAME, 0)
          }
          else if (!elementMsgType.isRepetition(Type.Repetition.OPTIONAL)) throw new RuntimeException("Null list element for " + schema.getName)
          recordConsumer.endGroup()
        }
        recordConsumer.endField(JsonWriteSupport.LIST_REPEATED_NAME, 0)
      }
    }
  }

}

object JsonWriteSupport {
  private[parquet] val LIST_REPEATED_NAME = "list"
  private[parquet] val OLD_LIST_REPEATED_NAME = "array"
  private[parquet] val LIST_ELEMENT_NAME = "element"
  val WRITE_OLD_LIST_STRUCTURE = "parquet.avro.write-old-list-structure"
  private[parquet] val WRITE_OLD_LIST_STRUCTURE_DEFAULT = false
}

/**
  * Convert any number without losing data
  *
  * @todo Does we need to convert string?
  */
object TypeConverter {
  def toInt(number: Any): Int = number match {
    case num: Int => num
    case num: String => num.toInt
    case any => any.asInstanceOf[Int]
  }

  def toFloat(number: Any): Float = number match {
    case num: Float => num
    case num: Int => num.toFloat
    case num: Long => num.toFloat
    case num: Double => num.toFloat
    case num: String => num.toFloat
    case any => any.asInstanceOf[Float] //Get class cast exception
  }

  def toDouble(number: Any): Double = number match {
    case num: Double => num
    case num: Int => num.toDouble
    case num: Long => num.toLong
    case num: Float => num.toDouble
    case num: String => num.toDouble
    case any => any.asInstanceOf[Double] //Get class cast exception
  }

  def toLong(number: Any): Long = number match {
    case num: Long => num
    case num: Int => num.toLong
    case num: String => num.toLong
    case any => any.asInstanceOf[Long] //Get class cast exception
  }

  def toString(any: Any)(implicit objectMapper: ObjectMapper): String = any match {
    case str: String => str
    case map: Map[Any, Any] => objectMapper.writeValueAsString(map)
    case seq: Seq[Any] => objectMapper.writeValueAsString(seq)
    case any => any.toString
  }
}