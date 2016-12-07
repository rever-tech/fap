package schemamanager.controller.thrift

import javax.inject.{Inject, Singleton}

import com.twitter.finatra.thrift.Controller
import com.twitter.inject.Logging
import schemamanager.service.TSchemaManager._
import schemamanager.service.{SchemaService, TSchemaManager}


/**
 * Created by SangDang on 9/16/16.
 */
@Singleton
class SchemaController @Inject()(schemaService: SchemaService) extends Controller with TSchemaManager.BaseServiceIface with Logging {

  override val addSchema = handle(AddSchema) {
    args: AddSchema.Args => {
      schemaService.addSchema(args.schema)
    }
  }


  override val getSchemas = handle(GetSchemas) {
    args: GetSchemas.Args => {
      schemaService.getSchemas(args.name)
    }
  }

  override def getSchema = handle(GetSchema) {
    args: GetSchema.Args => {
      schemaService.getSchema(args.name, args.version)
    }
  }

  override def getAllSchemaName = handle(GetAllSchemaName) {
    args: GetAllSchemaName.Args => {
      schemaService.getAllSchemaName()
    }
  }

  override def exist = handle(Exist) {
    args: Exist.Args => {
      schemaService.exist(args.name)
    }
  }

  override def deleteSchemaName = handle(DeleteSchemaName) {
    args: DeleteSchemaName.Args => {
      schemaService.deleteSchemaName(args.name)
    }
  }

  override def deleteSchema = handle(DeleteSchema) {
    args: DeleteSchema.Args => {
      schemaService.deleteSchema(args.name, args.version)
    }
  }

  override def deleteAllSchema = handle(DeleteAllSchema) {
    args: DeleteAllSchema.Args => {
      schemaService.deleteAllSchema()
    }
  }
}
