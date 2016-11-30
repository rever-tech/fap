package hadoop-worker.module

import javax.inject.Singleton

import com.google.inject.Provides
import com.twitter.inject.TwitterModule
import hadoop-worker.domain.{UserID, UserInfo}
import hadoop-worker.repository.{CacheRepository, OnMemoryCacheRepository}
import hadoop-worker.service.{UserCacheService, UserCacheServiceImpl}

/**
  * Created by SangDang on 9/16/16.
  */
object UserCacheModule extends TwitterModule {
  override def configure: Unit = {
    bind[UserCacheService].to[UserCacheServiceImpl]
  }

  @Singleton
  @Provides
  def providesUserCacheRepository(): CacheRepository[UserID, UserInfo] = {
    new OnMemoryCacheRepository[UserID, UserInfo]()
  }
}
