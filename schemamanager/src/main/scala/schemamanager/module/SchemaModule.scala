package schemamanager.module

import javax.inject.{Inject, Named}

import com.google.inject.{Provides, Singleton}
import com.twitter.inject.TwitterModule
import schemamanager.client.LevelDBClient
import schemamanager.service.{LevelDBSchemaService, SchemaService}
import schemamanager.util.ZConfig

/**
 * Created by SangDang on 9/16/16.
 */
object SchemaModule extends TwitterModule {

  @Singleton
  @Provides
  def providerLevelDBClient(): LevelDBClient = {
    val levelDir = ZConfig.getString("leveldb.dir.schemamanager")
    val cacheSizeInMb = ZConfig.getLong("leveldb.cache_size_in_mb")
    new LevelDBClient(levelDir, cacheSizeInMb * 1024 * 1024)
  }

  @Singleton
  @Provides
  def providesSchemaService(@Inject levelDBClient: LevelDBClient): SchemaService = {
    new LevelDBSchemaService(levelDBClient)
  }
}
