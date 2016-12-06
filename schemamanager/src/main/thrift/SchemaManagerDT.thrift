#@namespace scala schemamanager.domain

struct TSchemaData {
    1:map<string, string> nameToType
}

struct TSchema {
    1: string name
    2: i32 version
    3: TSchemaData schema
}

