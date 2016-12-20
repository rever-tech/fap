package worker.hadoop.service

import com.google.inject.Singleton
import com.twitter.conversions.time._
import com.twitter.inject.Logging
import com.twitter.util.{Await, Future}
import parquet.schema.json.{JsonSchema, JsonType}
import schemamanager.service.TSchemaManager
import worker.hadoop.schema.SchemaInfo

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/**
  * Created by tiennt4 on 08/12/2016.
  */

@Singleton
class SchemaManagerService(schemaManagerClient: TSchemaManager[Future]) extends Logging {

  private val nRetry: Int = 3
  private final val schemas: mutable.Map[SchemaInfo, JsonSchema] = mutable.Map.empty

  def getSchema(schemaInfo: SchemaInfo): Option[JsonSchema] = {
    if (!schemas.contains(schemaInfo)) {
      synchronized {
        if (!schemas.contains(schemaInfo)) {
          retryOrNone(nRetry) {
            getJsonSchemaFromSchemaManager(schemaInfo)
          } match {
            case Some(schema) =>
              schemas.put(schemaInfo, schema)
            case None =>
          }
        }
      }
    }
    schemas.get(schemaInfo)
  }

  private def getJsonSchemaFromSchemaManager(schemaInfo: SchemaInfo): Option[JsonSchema] = {
    Await.result(schemaManagerClient.getSchema(schemaInfo.name, schemaInfo.version)
      .map(resp => resp.data.map(schema =>
        JsonType.parseSchema(schema.name, schema.version, schema.schema.nameToType.toMap)))
      , 5 seconds)
  }

  @tailrec
  private def retryOrNone[T](n: Int)(fn: => Option[T]): Option[T] = {
    Try {
      fn
    } match {
      case Success(x) => x
      case _ if n > 1 => retryOrNone(n - 1)(fn)
      case Failure(e) =>
        error("", e)
        None
    }
  }
}
