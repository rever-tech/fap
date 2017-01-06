#@namespace scala schemamanager.service

include "SchemaManagerDT.thrift"


service TSchemaManager {
    bool addSchema(1: SchemaManagerDT.TSchema schema)
    SchemaManagerDT.TGetSchemasResp getSchemas(1:string name)
    SchemaManagerDT.TGetSchemaResp getSchema(1:string name,2:i32 version)
    list<string> getAllSchemaName()
    bool exist(1:string name)
    bool deleteSchemaName(1: string name)
    bool deleteSchema(1: string name, 2:i32 version)
    bool deleteAllSchema()
}
