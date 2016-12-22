package collector.module

import java.util.concurrent.Executors
import javax.inject.{Inject, Singleton}

import collector.service.{AnalyticsConsumerBuilder, AnalyticsService, AnalyticsServiceImpl, KafkaAnalyticsConsumerBuilder}
import collector.util.ZConfig
import com.google.inject.Provides
import com.google.inject.name.Named
import com.twitter.inject.TwitterModule

import scala.concurrent.ExecutionContext

/**
  * Created by SangDang on 9/16/16.
  */
object AnalyticsModule extends TwitterModule {

  @Singleton
  @Provides
  def providesAnalyticsService(@Inject builder: AnalyticsConsumerBuilder): AnalyticsService = {
    val numAnalyticsWorker = ZConfig.getInt("analytics-worker.num", Runtime.getRuntime.availableProcessors() * 2)
    new AnalyticsServiceImpl(builder)(ExecutionContext.fromExecutor(Executors.newFixedThreadPool(numAnalyticsWorker)))
  }

  @Singleton
  @Provides
  def providesAnalyticsConsumerBuilder(): AnalyticsConsumerBuilder = {
    KafkaAnalyticsConsumerBuilder
  }

  @Singleton
  @Provides
  @Named("validGifName")
  def validGifName(): Set[String] = {
    ZConfig.getStringList("analytic.gif_names").toSet
  }
}


