package worker.hadoop.module

import java.net.InetSocketAddress

import com.google.inject.name.Named
import com.google.inject.{Inject, Provides, Singleton}
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.thrift.ThriftClientFramedCodec
import com.twitter.inject.TwitterModule
import com.twitter.util.Future
import com.typesafe.config.ConfigRenderOptions
import schemamanager.service.TSchemaManager
import schemamanager.service.TSchemaManager.FinagledClient
import worker.hadoop.service.SchemaManagerService
import worker.hadoop.util.ZConfig

/**
  * Created by tiennt4 on 19/12/2016.
  */
object HadoopWorkerModule extends TwitterModule {

  @Singleton
  @Provides
  def provideSchemaManagerService(@Inject schemaManagerClient: TSchemaManager[Future]): SchemaManagerService = {
    new SchemaManagerService(schemaManagerClient)
  }

  @Singleton
  @Provides
  def provideSchemaManagerClient(): TSchemaManager[Future] =
    new FinagledClient(
      ClientBuilder()
        .hosts(Seq(new InetSocketAddress(ZConfig.getString("SchemaManager.thrift.host"), ZConfig.getInt("SchemaManager.thrift.port"))))
        .codec(ThriftClientFramedCodec())
        .hostConnectionLimit(1)
        .retries(5)
        .build())

  @Provides
  @Named("worker_config")
  def provideWorkerConfig: String =
    ZConfig.getConfig("worker").root().render(ConfigRenderOptions.concise())

  @Provides
  @Named("kafka_config")
  def provideKafkaConfig: String =
    ZConfig.getConfig("kafka").root().render(ConfigRenderOptions.concise())

}
