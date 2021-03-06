package schemamanager.service

import com.google.inject.{Inject, Singleton}
import com.twitter.util.Future
import org.iq80.leveldb.DB
import schemamanager.client.LevelDBClient
import schemamanager.domain.Implicits._
import schemamanager.domain.{Schema, SchemaData, TSchema}
import schemamanager.util.JsonUtils

/**
 * Created by zkidkid on 12/6/16.
 */
trait SchemaService {

  def addSchema(schema: Schema): Future[Boolean]

  def getSchemas(name: String): Future[Option[Seq[TSchema]]]

  def getSchema(name: String, version: Int): Future[Option[TSchema]]

  def getAllSchemaName(): Future[Seq[String]]

  def exist(name: String): Future[Boolean]

  def deleteSchemaName(name: String): Future[Boolean]

  def deleteSchema(name: String, version: Int): Future[Boolean]

  def deleteAllSchema(): Future[Boolean]
}

@Singleton
class LevelDBSchemaService @Inject()(levelDBClient: LevelDBClient) extends SchemaService {
  val db: DB = levelDBClient.getDB
  val schemaKeys = getBytes("schemas_key")
  val schemaNameVersionsPrefix = "schema_versions"
  val schemaNameVersionPrefix = "schema_version"

  initDB

  private[this] def initDB = {
    val batch = db.createWriteBatch()
    try {
      db.get(schemaKeys) match {
        case null => {
          batch.put(schemaKeys, getBytes(JsonUtils.toJson[Set[String]](Set.empty)))
          db.write(batch)
        }
        case _ =>
      }
    } finally {
      batch.close
    }
  }

  private def verifySchema(schema: Schema) = {
    if (schema.schema.fieldSchemas.map(f => f.name).toSet.size != schema.schema.fieldSchemas.size) {
      throw new Exception("field schemas duplicated")
    }
  }

  override def addSchema(schema: Schema): Future[Boolean] = futurePool {
    var addOk = false
    verifySchema(schema)
    val batch = db.createWriteBatch()
    try {
      val schemaNameVersionsKey = getBytes(s"${schemaNameVersionsPrefix}_${schema.name}")
      var schemaNameVersionsValue = db.get(schemaNameVersionsKey) match {
        case null => {
          var schemaNames = JsonUtils.fromJson[Set[String]](getString(db.get(schemaKeys)))
          schemaNames += schema.name
          batch.put(schemaKeys, getBytes(JsonUtils.toJson[Set[String]](schemaNames)))
          Set.empty[Int]
        }
        case x => JsonUtils.fromJson[Set[Int]](getString(x))
      }
      schemaNameVersionsValue.contains(schema.version) match {
        case false => {
          schemaNameVersionsValue += schema.version
          batch.put(schemaNameVersionsKey, getBytes(JsonUtils.toJson[Set[Int]](schemaNameVersionsValue)))
        }
        case true =>
      }
      val schemaNameVersionKey = getBytes(s"${schemaNameVersionPrefix}_${schema.name}_${schema.version}")
      batch.put(schemaNameVersionKey, getBytes(JsonUtils.toJson[SchemaData](schema.schema)))
      db.write(batch)
      addOk = true
    } finally {
      batch.close
    }
    addOk
  }

  override def getSchemas(name: String): Future[Option[Seq[TSchema]]] = futurePool {
    val schemaNameVersionsKey = getBytes(s"${schemaNameVersionsPrefix}_$name")
    db.get(schemaNameVersionsKey) match {
      case null => None
      case x => {
        val versions = JsonUtils.fromJson[Set[Int]](getString(x))
        val nameVersions = versions.map(version => {
          val schemaNameVersionKey = getBytes(s"${schemaNameVersionPrefix}_${name}_${version}")
          Schema(name, version, JsonUtils.fromJson[SchemaData](getString(db.get(schemaNameVersionKey))))
        }).toSeq
        Some(nameVersions)
      }
    }
  }

  override def getSchema(name: String, version: Int): Future[Option[TSchema]] = futurePool {
    val schemaNameVersionKey = getBytes(s"${schemaNameVersionPrefix}_${name}_${version}")
    db.get(schemaNameVersionKey) match {
      case null => None
      case x => Some(Schema(name, version, JsonUtils.fromJson[SchemaData](getString(x))))
    }
  }

  override def getAllSchemaName(): Future[Seq[String]] = futurePool {
    JsonUtils.fromJson[Seq[String]](getString(db.get(schemaKeys)))
  }

  override def exist(name: String): Future[Boolean] = futurePool {
    val schemaNameVersionsKey = getBytes(s"${schemaNameVersionsPrefix}_${name}")
    db.get(schemaNameVersionsKey) match {
      case null => false
      case x => true
    }
  }

  override def deleteSchemaName(name: String): Future[Boolean] = futurePool {
    var deleteOk = false
    val batch = db.createWriteBatch()
    try {
      val schemaNameVersionsKey = getBytes(s"${schemaNameVersionsPrefix}_${name}")
      db.get(schemaNameVersionsKey) match {
        case null =>
        case x => {
          JsonUtils.fromJson[Set[Int]](getString(x)).foreach(version => {
            val schemaNameVersionKey = getBytes(s"${schemaNameVersionPrefix}_${name}_${version}")
            batch.delete(schemaNameVersionKey)
          })
          batch.delete(schemaNameVersionsKey)
          batch.put(schemaKeys, getBytes(JsonUtils.toJson[Set[String]](JsonUtils.fromJson[Set[String]](getString(db.get(schemaKeys))) - name)))
        }
      }
      db.write(batch)
      deleteOk = true
    } finally {
      batch.close
    }
    deleteOk
  }

  override def deleteSchema(name: String, version: Int): Future[Boolean] = futurePool {
    var deleteOk = false
    val batch = db.createWriteBatch()
    try {
      val schemaNameVersionKey = getBytes(s"${schemaNameVersionPrefix}_${name}_${version}")
      batch.delete(schemaNameVersionKey)
      val schemaNameVersionsKey = getBytes(s"${schemaNameVersionsPrefix}_$name")
      val versions = JsonUtils.fromJson[Set[Int]](getString(db.get(schemaNameVersionsKey))) - version
      versions.size match {
        case 0 => {
          batch.delete(schemaNameVersionsKey)
          batch.put(schemaKeys, getBytes(JsonUtils.toJson(JsonUtils.fromJson[Set[String]](getString(db.get(schemaKeys))) - name)))
        }
        case _ => batch.put(schemaNameVersionsKey, getBytes(JsonUtils.toJson[Set[Int]](versions)))
      }

      db.write(batch)
      deleteOk = true
    } finally {
      batch.close()
    }
    deleteOk
  }

  override def deleteAllSchema(): Future[Boolean] = futurePool {
    var deleteOk = false
    val batch = db.createWriteBatch()
    try {
      JsonUtils.fromJson[Set[String]](getString(db.get(schemaKeys))).foreach(name => {
        val schemaNameVersionsKey = getBytes(s"${schemaNameVersionsPrefix}_$name")
        JsonUtils.fromJson[Set[Int]](getString(db.get(schemaNameVersionsKey))).foreach(version => {
          val schemaNameVersionKey = getBytes(s"${schemaNameVersionPrefix}_${name}_${version}")
          batch.delete(schemaNameVersionKey)
        })
        batch.delete(schemaNameVersionsKey)
      })
      batch.put(schemaKeys, getBytes(JsonUtils.toJson[Set[String]](Set.empty)))

      db.write(batch)
      deleteOk = true
    } finally {
      batch.close
    }
    deleteOk
  }

  private def getBytes(s: String) = s.getBytes("UTF-8")

  private def getString(bytes: Array[Byte]) = new String(bytes, "UTF-8")
}
