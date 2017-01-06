#@namespace scala schemamanager.domain

struct TFieldSchema{
    1: required string name
    2: required string type
}

struct TSchemaData {
    1: list<TFieldSchema> fieldSchemas
}

struct TSchema {
    1: string name
    2: i32 version
    3: TSchemaData schema
}

struct TGetSchemaResp{
    1: required bool exist
    2: optional TSchema data
}

struct TGetSchemasResp{
    1: required bool exist
    2: optional list<TSchema> data
}