package collector


import collector.controller.http.AnalyticsController
import collector.module.AnalyticsModule
import collector.util.ZConfig
import com.twitter.finatra.http.HttpServer
import com.twitter.finatra.http.filters.CommonFilters
import com.twitter.finatra.http.routing.HttpRouter

/**
 * Created by SangDang on 9/8/
 **/
object MainApp extends Server

class Server extends HttpServer {

  override protected def defaultFinatraHttpPort: String = ZConfig.getString("server.http.port", ":8080")

  override protected def disableAdminHttpServer: Boolean = ZConfig.getBoolean("server.admin.disable", true)

  override val modules = Seq(AnalyticsModule)

  override protected def configureHttp(router: HttpRouter): Unit = {
    router.filter[CommonFilters]
      .add[AnalyticsController]
  }
}
