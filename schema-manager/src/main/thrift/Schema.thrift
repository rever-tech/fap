#@namespace scala schema-manager.service


struct TSchemaData{
    1:map<string,string> nameToType
}
struct TSchema{
    1:string name,
    2:i32 version,
    3:optional TSchemaData schema
}
service TSchemaManager {
    bool addSchema(1:required TSchema schema)
    list<TSchema> getSchemas(1:required string name)
    TSchema getSchema(1:required string name,2:required i32 version)
    list<string> getAllSchemaName()
    bool exist(1:required string name)
}

