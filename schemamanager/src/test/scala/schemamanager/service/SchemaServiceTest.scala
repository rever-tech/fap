package schemamanager.service

import com.google.inject.Guice
import com.twitter.inject.{Injector, IntegrationTest}
import com.twitter.util.Await
import schemamanager.client.LevelDBClient
import schemamanager.domain.Implicits._
import schemamanager.domain._
import schemamanager.module.SchemaServiceModuleTest

/**
 * @author sonpn
 */
class SchemaServiceTest extends IntegrationTest {
  override protected val injector: Injector = Injector(Guice.createInjector(SchemaServiceModuleTest))

  val schemaService = injector.instance[SchemaService]
  val levelDBClient = injector.instance[LevelDBClient]

  override def afterAll(): Unit = {
    super.afterAll()
    levelDBClient.close
    levelDBClient.destroy
  }

  def getType(fieldSchemas: Seq[TFieldSchema], name: String): Option[String] = {
    fieldSchemas.flatMap(f => if (f.name.equals(name)) Some(f.`type`) else None) match {
      case Nil => None
      case x => Some(x(0))
    }
  }

  val name = "schema name test"
  val version = 1
  val schemaData = TSchemaData(Seq(
    TFieldSchema("age", "int"),
    TFieldSchema("name", "string"),
    TFieldSchema("address", "string")
  ))

  val schemaData2 = TSchemaData(Seq(
    TFieldSchema("age", "int"),
    TFieldSchema("name", "string"),
    TFieldSchema("name", "string")
  ))

  "add schema" in {
    val schema = TSchema(name, version, schemaData)
    assertResult(true)(Await.result(schemaService.addSchema(schema)))
    val schemaResp = Await.result(schemaService.getSchema(name, version)).get
    assertResult(name)(schemaResp.name)
    assertResult(version)(schemaResp.version)
    assertResult(0)((schemaResp.schema.fieldSchemas.toSet diff schemaData.fieldSchemas.toSet).size)
    assertResult(getType(schemaData.fieldSchemas, "age"))(getType(schemaResp.schema.fieldSchemas, "age"))
    assertResult(getType(schemaData.fieldSchemas, "age1"))(getType(schemaResp.schema.fieldSchemas, "age1"))

    assertResult(true)(Await.result(schemaService.deleteSchemaName(name)))
  }

  "add schema with field duplicated" in {
    val schema = TSchema(name, version, schemaData2)
    try {
      Await.result(schemaService.addSchema(schema))
      assert(false)
    } catch {
      case e: Exception => {
        println(e)
        assert(true)
      }
    }
  }

  "get schema by name" in {
    val numVersion = 100
    for (i <- 1 to numVersion) {
      val schema = TSchema(name, version + i, schemaData)
      assertResult(true)(Await.result(schemaService.addSchema(schema)))
    }
    val schemaVersionsResp = Await.result(schemaService.getSchemas(name)).get
    assertResult(numVersion)(schemaVersionsResp.size)
    schemaVersionsResp.foreach(schemaResp => {
      assertResult(name)(schemaResp.name)
      assertResult(0)((schemaResp.schema.fieldSchemas.toSet diff schemaData.fieldSchemas.toSet).size)
      assertResult(getType(schemaData.fieldSchemas, "age"))(getType(schemaResp.schema.fieldSchemas, "age"))
      assertResult(getType(schemaData.fieldSchemas, "age1"))(getType(schemaResp.schema.fieldSchemas, "age1"))
    })

    assertResult(true)(Await.result(schemaService.deleteSchemaName(name)))
  }
  "get schame by name - version" in {
    val numVersion = 100
    for (i <- 1 to numVersion) {
      val schema = TSchema(name, version + i, schemaData)
      assertResult(true)(Await.result(schemaService.addSchema(schema)))
    }
    assertResult(Schema(name, version + numVersion, schemaData))(T2Schema(Await.result(schemaService.getSchema(name, version + numVersion)).get))
    assertResult(None)(Await.result(schemaService.getSchema(name, version + numVersion + 1)))
    assertResult(true)(Await.result(schemaService.deleteSchemaName(name)))
  }
  "get all schema name" in {
    val numName = 100
    for (i <- 1 to numName) {
      val name1 = name + " - name " + i
      assertResult(true)(Await.result(schemaService.addSchema(TSchema(name1, version, schemaData))))
    }
    assertResult(numName)(Await.result(schemaService.getAllSchemaName()).size)
    for (i <- 1 to numName) {
      val newName = name + " - name " + i
      assertResult(true)(Await.result(schemaService.deleteSchemaName(newName)))
    }
  }
  "exist schema" in {
    assertResult(true)(Await.result(schemaService.addSchema(TSchema(name, version, schemaData))))
    val name1 = name + " - name " + 1
    assertResult(true)(Await.result(schemaService.addSchema(TSchema(name1, version, schemaData))))
    val name2 = name + " - name " + 2
    assertResult(true)(Await.result(schemaService.addSchema(TSchema(name2, version, schemaData))))

    assertResult(true)(Await.result(schemaService.exist(name)))
    assertResult(true)(Await.result(schemaService.exist(name1)))
    assertResult(true)(Await.result(schemaService.exist(name2)))
    assertResult(true)(Await.result(schemaService.deleteSchemaName(name1)))
    assertResult(false)(Await.result(schemaService.exist(name1)))

    assertResult(true)(Await.result(schemaService.deleteSchemaName(name)))
    assertResult(true)(Await.result(schemaService.deleteSchemaName(name1)))
    assertResult(true)(Await.result(schemaService.deleteSchemaName(name2)))
  }
  "delete schema by name" in {
    assertResult(true)(Await.result(schemaService.addSchema(TSchema(name, version, schemaData))))
    val name1 = name + " - name " + 1
    assertResult(true)(Await.result(schemaService.addSchema(TSchema(name1, version, schemaData))))
    val name2 = name + " - name " + 2
    assertResult(true)(Await.result(schemaService.addSchema(TSchema(name2, version, schemaData))))

    assertResult(true)(Await.result(schemaService.exist(name)))
    assertResult(true)(Await.result(schemaService.exist(name1)))
    assertResult(true)(Await.result(schemaService.exist(name2)))
    assertResult(true)(Await.result(schemaService.deleteSchemaName(name1)))
    assertResult(false)(Await.result(schemaService.exist(name1)))
    assertResult(true)(Await.result(schemaService.exist(name)))
    assertResult(true)(Await.result(schemaService.exist(name2)))
    assertResult(true)(Await.result(schemaService.deleteSchemaName(name)))
    assertResult(false)(Await.result(schemaService.exist(name)))
    assertResult(false)(Await.result(schemaService.exist(name1)))
    assertResult(true)(Await.result(schemaService.exist(name2)))
    assertResult(true)(Await.result(schemaService.deleteSchemaName(name2)))
    assertResult(false)(Await.result(schemaService.exist(name)))
    assertResult(false)(Await.result(schemaService.exist(name1)))
    assertResult(false)(Await.result(schemaService.exist(name2)))
  }
  "delete scheme by name-version" in {
    val schema = TSchema(name, version, schemaData)
    val schema1 = TSchema(name, version + 1, schemaData)
    val schema2 = TSchema(name, version + 2, schemaData)
    val schema3 = TSchema(name + 3, version + 3, schemaData)
    assertResult(true)(Await.result(schemaService.addSchema(schema)))
    assertResult(true)(Await.result(schemaService.addSchema(schema1)))
    assertResult(true)(Await.result(schemaService.addSchema(schema2)))
    assertResult(true)(Await.result(schemaService.addSchema(schema3)))

    assertResult(Some(schema))(Await.result(schemaService.getSchema(schema.name, schema.version)))
    assertResult(Some(schema1))(Await.result(schemaService.getSchema(schema1.name, schema1.version)))
    assertResult(Some(schema2))(Await.result(schemaService.getSchema(schema2.name, schema2.version)))
    assertResult(Some(schema3))(Await.result(schemaService.getSchema(schema3.name, schema3.version)))

    assertResult(true)(Await.result(schemaService.deleteSchema(schema.name, schema.version)))
    assertResult(None)(Await.result(schemaService.getSchema(schema.name, schema.version)))
    assertResult(Some(schema1))(Await.result(schemaService.getSchema(schema1.name, schema1.version)))
    assertResult(Some(schema2))(Await.result(schemaService.getSchema(schema2.name, schema2.version)))
    assertResult(Some(schema3))(Await.result(schemaService.getSchema(schema3.name, schema3.version)))

    assertResult(true)(Await.result(schemaService.deleteSchema(schema3.name, schema3.version)))
    assertResult(None)(Await.result(schemaService.getSchema(schema.name, schema.version)))
    assertResult(Some(schema1))(Await.result(schemaService.getSchema(schema1.name, schema1.version)))
    assertResult(Some(schema2))(Await.result(schemaService.getSchema(schema2.name, schema2.version)))
    assertResult(None)(Await.result(schemaService.getSchema(schema3.name, schema3.version)))

    assertResult(true)(Await.result(schemaService.deleteSchemaName(name)))
    assertResult(true)(Await.result(schemaService.deleteSchemaName(name + 3)))
  }
  "delete all schema" in {
    val schema = TSchema(name, version, schemaData)
    val schema1 = TSchema(name, version + 1, schemaData)
    val schema2 = TSchema(name, version + 2, schemaData)
    val schema3 = TSchema(name + 3, version + 3, schemaData)
    assertResult(true)(Await.result(schemaService.addSchema(schema)))
    assertResult(true)(Await.result(schemaService.addSchema(schema1)))
    assertResult(true)(Await.result(schemaService.addSchema(schema2)))
    assertResult(true)(Await.result(schemaService.addSchema(schema3)))

    assertResult(Some(schema))(Await.result(schemaService.getSchema(schema.name, schema.version)))
    assertResult(Some(schema1))(Await.result(schemaService.getSchema(schema1.name, schema1.version)))
    assertResult(Some(schema2))(Await.result(schemaService.getSchema(schema2.name, schema2.version)))
    assertResult(Some(schema3))(Await.result(schemaService.getSchema(schema3.name, schema3.version)))

    assertResult(true)(Await.result(schemaService.deleteAllSchema()))

    assertResult(None)(Await.result(schemaService.getSchema(schema.name, schema.version)))
    assertResult(None)(Await.result(schemaService.getSchema(schema1.name, schema1.version)))
    assertResult(None)(Await.result(schemaService.getSchema(schema2.name, schema2.version)))
    assertResult(None)(Await.result(schemaService.getSchema(schema3.name, schema3.version)))
  }

  "delete All" in {
    assertResult(true)(Await.result(schemaService.deleteAllSchema()))
  }
}
