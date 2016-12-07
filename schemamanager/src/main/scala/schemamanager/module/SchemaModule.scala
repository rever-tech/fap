package schemamanager.module

import com.google.inject.{Provides, Singleton}
import com.twitter.inject.TwitterModule
import schemamanager.service.{LevelDBSchemaService, SchemaService}
import schemamanager.util.ZConfig

/**
 * Created by SangDang on 9/16/16.
 */
object SchemaModule extends TwitterModule {
  @Singleton
  @Provides
  def providesSchemaService(): SchemaService = LevelDBSchemaService(ZConfig.getString("leveldb.dir"))
}
