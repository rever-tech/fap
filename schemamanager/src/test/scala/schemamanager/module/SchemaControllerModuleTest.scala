package schemamanager.module

import java.util.UUID
import javax.inject.{Inject, Named}

import com.google.inject.{Provides, Singleton}
import com.twitter.inject.TwitterModule
import schemamanager.client.LevelDBClient
import schemamanager.service.{LevelDBSchemaService, SchemaService}

/**
 * @author sonpn
 */
object SchemaControllerModuleTest extends TwitterModule {

  @Singleton
  @Provides
  def providesLevelDB(): LevelDBClient = {
    new LevelDBClient("./db/leveldb_test_" + UUID.randomUUID().toString)
  }

  @Singleton
  @Provides
  def providesSchemaServiceController(@Inject levelDbClient: LevelDBClient): SchemaService = {
    new LevelDBSchemaService(levelDbClient)
  }

}
