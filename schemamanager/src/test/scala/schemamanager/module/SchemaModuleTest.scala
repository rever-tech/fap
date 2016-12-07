package schemamanager.module

import com.google.inject.{Provides, Singleton}
import com.twitter.inject.TwitterModule
import schemamanager.service.{LevelDBSchemaService, SchemaService}

/**
 * @author sonpn
 */
object SchemaModuleTest extends TwitterModule {
  @Singleton
  @Provides
  def providesSchemaService(): SchemaService = new LevelDBSchemaService("./db/leveldb_test")

}
