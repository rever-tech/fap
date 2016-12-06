#@namespace scala schemamanager.service

include "SchemaManagerDT.thrift"


service TSchemaManager {
    bool addSchema(1: SchemaManagerDT.TSchema schema)
    list<SchemaManagerDT.TSchema> getSchemas(1:string name)
    SchemaManagerDT.TSchema getSchema(1:string name,2:i32 version)
    list<string> getAllSchemaName()
    bool exist(1:string name)
}

