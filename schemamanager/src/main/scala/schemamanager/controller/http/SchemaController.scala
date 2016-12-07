package schemamanager.controller.http

import javax.inject.{Inject, Singleton}

import com.twitter.finagle.http.Request
import com.twitter.finatra.http.Controller


/**
 * Created by SangDang on 9/16/16.
 */

@Singleton
class SchemaController @Inject() extends Controller {
  post("/getAllSchema") { request: Request =>
    response.ok
  }
}
