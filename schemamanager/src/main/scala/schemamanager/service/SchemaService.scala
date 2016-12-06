package schemamanager.service

import com.twitter.util.Future
import schemamanager.domain.TSchema

/**
 * Created by zkidkid on 12/6/16.
 */
trait SchemaService {

  def addSchema(schema: TSchema): Future[Boolean]

  def getSchemas(name: String): Future[Seq[TSchema]]

  def getSchema(name: String, version: Int): Future[TSchema]

  def getAllSchemaName(): Future[Seq[String]]

  def exist(name: String): Future[Boolean]

}

class LevelDBSchemaService extends SchemaService {

  override def addSchema(schema: TSchema): Future[Boolean] = ???

  override def getSchemas(name: String): Future[Seq[TSchema]] = ???

  override def getSchema(name: String, version: Int): Future[TSchema] = ???

  override def getAllSchemaName(): Future[Seq[String]] = ???

  override def exist(name: String): Future[Boolean] = ???

}

