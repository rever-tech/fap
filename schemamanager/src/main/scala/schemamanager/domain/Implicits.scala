package schemamanager.domain

import com.twitter.util.FuturePool

/**
 * @author sonpn
 */
object Implicits {
  implicit def futurePool = FuturePool.unboundedPool

  implicit def T2FieldSchema(tFieldSchema: TFieldSchema) = FieldSchema(tFieldSchema.name, tFieldSchema.`type`)

  implicit def FieldSchema2T(fieldSchema: FieldSchema) = TFieldSchema(fieldSchema.name, fieldSchema.`type`)

  implicit def T2SchemaData(tschemaData: TSchemaData) = SchemaData(tschemaData.fieldSchemas.map(f => T2FieldSchema(f)))

  implicit def SchemaData2T(schemaData: SchemaData) = TSchemaData(schemaData.fieldSchemas.map(f => FieldSchema2T(f)))

  implicit def Schema2T(schema: Schema) = TSchema(schema.name, schema.version, schema.schema)

  implicit def T2Schema(tschema: TSchema) = Schema(tschema.name, tschema.version, tschema.schema)

  implicit def SeqSchema2T(seqSchema: Seq[Schema]): Seq[TSchema] = seqSchema.map(f => Schema2T(f))

}
