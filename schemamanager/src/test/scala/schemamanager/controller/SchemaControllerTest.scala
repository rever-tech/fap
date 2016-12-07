package schemamanager.controller

import com.twitter.finatra.http.EmbeddedHttpServer
import com.twitter.finatra.thrift.ThriftClient
import com.twitter.inject.server.FeatureTest
import com.twitter.util.{Await, Future}
import schemamanager.ServerTest
import schemamanager.domain.Implicits.{T2Schema, T2SchemaData}
import schemamanager.domain.{Schema, TSchema, TSchemaData}
import schemamanager.service.TSchemaManager

/**
 * @author sonpn
 */
class SchemaControllerTest extends FeatureTest {
  override protected def server = new EmbeddedHttpServer(twitterServer = new ServerTest) with ThriftClient

  "thrift test" should {
    val client = server.thriftClient[TSchemaManager[Future]](clientId = "thrift-test")

    val name = "schema name test"
    val version = 1
    val schemaData = TSchemaData(Map(
      "age" -> "int",
      "name" -> "string",
      "address" -> "string"
    ))

    "add schema" in {
//      val schema = TSchema(name, version, schemaData)
//      assertResult(true)(Await.result(client.addSchema(schema)))
//      val schemaResp = Await.result(client.getSchema(name, version))
//      assertResult(name)(schemaResp.name)
//      assertResult(version)(schemaResp.version)
//      assertResult(0)((schemaResp.schema.nameToType.toSet diff schemaData.nameToType.toSet).toMap.size)
//      assertResult(schemaData.nameToType.get("age"))(schemaResp.schema.nameToType.get("age"))
//      assertResult(schemaData.nameToType.get("age1"))(schemaResp.schema.nameToType.get("age1"))
//
//      assertResult(true)(Await.result(client.deleteSchemaName(name)))
    }
    //    "get schema by name" in {
    //      val numVersion = 100
    //      for (i <- 1 to numVersion) {
    //        val schema = TSchema(name, version + i, schemaData)
    //        assertResult(true)(Await.result(client.addSchema(schema)))
    //      }
    //      val schemaVersionsResp = Await.result(client.getSchemas(name))
    //      assertResult(numVersion)(schemaVersionsResp.size)
    //      schemaVersionsResp.foreach(schemaResp => {
    //        assertResult(name)(schemaResp.name)
    //        assertResult(0)((schemaResp.schema.nameToType.toSet diff schemaData.nameToType.toSet).toMap.size)
    //        assertResult(schemaData.nameToType.get("age"))(schemaResp.schema.nameToType.get("age"))
    //        assertResult(schemaData.nameToType.get("age1"))(schemaResp.schema.nameToType.get("age1"))
    //      })
    //
    //      assertResult(true)(Await.result(client.deleteSchemaName(name)))
    //    }
    //    "get schame by name - version" in {
    //      val numVersion = 100
    //      for (i <- 1 to numVersion) {
    //        val schema = TSchema(name, version + i, schemaData)
    //        assertResult(true)(Await.result(client.addSchema(schema)))
    //      }
    //      assertResult(Schema(name, version + numVersion, T2SchemaData(schemaData)))(T2Schema(Await.result(client.getSchema(name, version + numVersion))))
    //      assertResult(null)(Await.result(client.getSchema(name, version + numVersion + 1)))
    //
    //      assertResult(true)(Await.result(client.deleteSchemaName(name)))
    //    }
    //    "get all schema name" in {
    //      val numName = 100
    //      for (i <- 1 to numName) {
    //        val name1 = name + " - name " + i
    //        assertResult(true)(Await.result(client.addSchema(TSchema(name1, version, schemaData))))
    //      }
    //      assertResult(numName)(Await.result(client.getAllSchemaName()).size)
    //      for (i <- 1 to numName) {
    //        val newName = name + " - name " + i
    //        assertResult(true)(Await.result(client.deleteSchemaName(newName)))
    //      }
    //    }
    //    "exist schema" in {
    //      assertResult(true)(Await.result(client.addSchema(TSchema(name, version, schemaData))))
    //      val name1 = name + " - name " + 1
    //      assertResult(true)(Await.result(client.addSchema(TSchema(name1, version, schemaData))))
    //      val name2 = name + " - name " + 2
    //      assertResult(true)(Await.result(client.addSchema(TSchema(name2, version, schemaData))))
    //
    //      assertResult(true)(Await.result(client.exist(name)))
    //      assertResult(true)(Await.result(client.exist(name1)))
    //      assertResult(true)(Await.result(client.exist(name2)))
    //      assertResult(true)(Await.result(client.deleteSchemaName(name1)))
    //      assertResult(false)(Await.result(client.exist(name1)))
    //
    //      assertResult(true)(Await.result(client.deleteSchemaName(name)))
    //      assertResult(true)(Await.result(client.deleteSchemaName(name1)))
    //      assertResult(true)(Await.result(client.deleteSchemaName(name2)))
    //    }
    //    "delete schema by name" in {
    //      assertResult(true)(Await.result(client.addSchema(TSchema(name, version, schemaData))))
    //      val name1 = name + " - name " + 1
    //      assertResult(true)(Await.result(client.addSchema(TSchema(name1, version, schemaData))))
    //      val name2 = name + " - name " + 2
    //      assertResult(true)(Await.result(client.addSchema(TSchema(name2, version, schemaData))))
    //
    //      assertResult(true)(Await.result(client.exist(name)))
    //      assertResult(true)(Await.result(client.exist(name1)))
    //      assertResult(true)(Await.result(client.exist(name2)))
    //      assertResult(true)(Await.result(client.deleteSchemaName(name1)))
    //      assertResult(false)(Await.result(client.exist(name1)))
    //      assertResult(true)(Await.result(client.exist(name)))
    //      assertResult(true)(Await.result(client.exist(name2)))
    //      assertResult(true)(Await.result(client.deleteSchemaName(name)))
    //      assertResult(false)(Await.result(client.exist(name)))
    //      assertResult(false)(Await.result(client.exist(name1)))
    //      assertResult(true)(Await.result(client.exist(name2)))
    //      assertResult(true)(Await.result(client.deleteSchemaName(name2)))
    //      assertResult(false)(Await.result(client.exist(name)))
    //      assertResult(false)(Await.result(client.exist(name1)))
    //      assertResult(false)(Await.result(client.exist(name2)))
    //    }
    //    "delete scheme by name-version" in {
    //      val schema = TSchema(name, version, schemaData)
    //      val schema1 = TSchema(name, version + 1, schemaData)
    //      val schema2 = TSchema(name, version + 2, schemaData)
    //      val schema3 = TSchema(name + 3, version + 3, schemaData)
    //      assertResult(true)(Await.result(client.addSchema(schema)))
    //      assertResult(true)(Await.result(client.addSchema(schema1)))
    //      assertResult(true)(Await.result(client.addSchema(schema2)))
    //      assertResult(true)(Await.result(client.addSchema(schema3)))
    //
    //      assertResult(schema)(Await.result(client.getSchema(schema.name, schema.version)))
    //      assertResult(schema1)(Await.result(client.getSchema(schema1.name, schema1.version)))
    //      assertResult(schema2)(Await.result(client.getSchema(schema2.name, schema2.version)))
    //      assertResult(schema3)(Await.result(client.getSchema(schema3.name, schema3.version)))
    //
    //      assertResult(true)(Await.result(client.deleteSchema(schema.name, schema.version)))
    //      assertResult(null)(Await.result(client.getSchema(schema.name, schema.version)))
    //      assertResult(schema1)(Await.result(client.getSchema(schema1.name, schema1.version)))
    //      assertResult(schema2)(Await.result(client.getSchema(schema2.name, schema2.version)))
    //      assertResult(schema3)(Await.result(client.getSchema(schema3.name, schema3.version)))
    //
    //      assertResult(true)(Await.result(client.deleteSchema(schema3.name, schema3.version)))
    //      assertResult(null)(Await.result(client.getSchema(schema.name, schema.version)))
    //      assertResult(schema1)(Await.result(client.getSchema(schema1.name, schema1.version)))
    //      assertResult(schema2)(Await.result(client.getSchema(schema2.name, schema2.version)))
    //      assertResult(null)(Await.result(client.getSchema(schema3.name, schema3.version)))
    //
    //      assertResult(true)(Await.result(client.deleteSchemaName(name)))
    //      assertResult(true)(Await.result(client.deleteSchemaName(name + 3)))
    //    }
    //    "delete all schema" in {
    //      val schema = TSchema(name, version, schemaData)
    //      val schema1 = TSchema(name, version + 1, schemaData)
    //      val schema2 = TSchema(name, version + 2, schemaData)
    //      val schema3 = TSchema(name + 3, version + 3, schemaData)
    //      assertResult(true)(Await.result(client.addSchema(schema)))
    //      assertResult(true)(Await.result(client.addSchema(schema1)))
    //      assertResult(true)(Await.result(client.addSchema(schema2)))
    //      assertResult(true)(Await.result(client.addSchema(schema3)))
    //
    //      assertResult(schema)(Await.result(client.getSchema(schema.name, schema.version)))
    //      assertResult(schema1)(Await.result(client.getSchema(schema1.name, schema1.version)))
    //      assertResult(schema2)(Await.result(client.getSchema(schema2.name, schema2.version)))
    //      assertResult(schema3)(Await.result(client.getSchema(schema3.name, schema3.version)))
    //
    //      assertResult(true)(Await.result(client.deleteAllSchema()))
    //
    //      assertResult(null)(Await.result(client.getSchema(schema.name, schema.version)))
    //      assertResult(null)(Await.result(client.getSchema(schema1.name, schema1.version)))
    //      assertResult(null)(Await.result(client.getSchema(schema2.name, schema2.version)))
    //      assertResult(null)(Await.result(client.getSchema(schema3.name, schema3.version)))
    //    }
  }
}
