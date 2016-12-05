package worker.hadoop

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

  override val modules = Seq(UserCacheModule)

  override protected def configureHttp(router: HttpRouter): Unit = {
    router.filter[CommonFilters]

  }

}
