package worker.hadoop.service

import com.google.inject.Singleton
import com.twitter.conversions.time._
import com.twitter.util.{Await, Future}
import parquet.schema.json.{JsonSchema, JsonType}
import schemamanager.service.TSchemaManager
import worker.hadoop.schema.SchemaInfo

import scala.collection.mutable

/**
  * Created by tiennt4 on 08/12/2016.
  */

@Singleton
class SchemaManagerService(schemaManagerClient: TSchemaManager[Future]) {

  private final val schemas: mutable.Map[SchemaInfo, JsonSchema] = mutable.Map.empty

  def getSchema(schemaInfo: SchemaInfo): Option[JsonSchema] = {
    if (!schemas.contains(schemaInfo)) {
      synchronized {
        if (!schemas.contains(schemaInfo)) {
          Await.result(schemaManagerClient.getSchema(schemaInfo.name, schemaInfo.version)
            .map(resp => resp.data.map(schema =>
              JsonType.parseSchema(schema.name, schema.version, schema.schema.nameToType.toMap)))
            , 5 seconds) match {
            case Some(schema) =>
              schemas.put(schemaInfo, schema)
            case None =>
          }
        }
      }
    }
    schemas.get(schemaInfo)
  }
}
