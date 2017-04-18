package parquet.schema.json

import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema._
import parquet.writer.JsonWriteSupport

import scala.collection.JavaConversions._

/**
  * Created by tiennt4 on 07/12/2016.
  */
object JsonSchemaConverter {

  val writeOldListStructure = false

  def convert(schema: JsonSchema, repetition: Repetition = Repetition.OPTIONAL): MessageType =
    new MessageType(schema.name, convertFields(schema.fields, repetition))

  def convertFields(fields: Seq[NameAndType], repetition: Repetition): Seq[Type] =
    fields.map(convertField(_, repetition))

  def convertField(nameAndType: NameAndType, repetition: Type.Repetition): Type = nameAndType.jsonType match {
    case JsonObject(objFields) =>
      new GroupType(Type.Repetition.REQUIRED, nameAndType.name, convertFields(objFields, repetition))

    case JsonArray(elementType) =>
      if (writeOldListStructure)
        ConversionPatterns.listType(Type.Repetition.REQUIRED, nameAndType.name,
          convertField(NameAndType("array", elementType), repetition))
      else ConversionPatterns.listOfElements(Type.Repetition.REQUIRED, nameAndType.name,
        convertField(NameAndType(JsonWriteSupport.LIST_ELEMENT_NAME, elementType), repetition))

    case JsonBool() =>
      Types.primitive(PrimitiveTypeName.BOOLEAN, repetition)
        .named(nameAndType.name)

    case JsonInt() =>
      Types.primitive(PrimitiveTypeName.INT32, repetition)
        .named(nameAndType.name)

    case JsonLong() =>
      Types.primitive(PrimitiveTypeName.INT64, repetition)
        .named(nameAndType.name)

    case JsonFloat() =>
      Types.primitive(PrimitiveTypeName.FLOAT, repetition)
        .named(nameAndType.name)

    case JsonDouble() =>
      Types.primitive(PrimitiveTypeName.DOUBLE, repetition)
        .named(nameAndType.name)

    case JsonString() =>
      Types.primitive(PrimitiveTypeName.BINARY, repetition)
        .as(OriginalType.UTF8).named(nameAndType.name)

  }

}
