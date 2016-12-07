package schemamanager.module

import javax.inject.Named

import com.google.inject.{Provides, Singleton}
import com.twitter.inject.TwitterModule
import schemamanager.service.{LevelDBSchemaService, SchemaService}

/**
 * @author sonpn
 */
object SchemaModuleTest extends TwitterModule {
  @Singleton
  @Provides
  def providesSchemaServiceController(): SchemaService = new LevelDBSchemaService("./db/leveldb_test_2")

  @Singleton
  @Provides
  @Named("Service")
  def providesSchemaService(): SchemaService = new LevelDBSchemaService("./db/leveldb_test_3")

}
