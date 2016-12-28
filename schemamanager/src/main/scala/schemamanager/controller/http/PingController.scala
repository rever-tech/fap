package schemamanager.controller.http

import javax.inject.Singleton

import com.twitter.finagle.http.Request
import com.twitter.finatra.http.Controller

/**
 * @author sonpn
 */
@Singleton
class PingController extends Controller {
  get("/ping") { request: Request =>
    response.ok("pong")
  }
}
