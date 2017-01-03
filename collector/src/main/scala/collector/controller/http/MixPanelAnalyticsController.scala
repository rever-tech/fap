package collector.controller.http

import java.util.Base64
import javax.inject.Inject

import collector.domain.AnalyticRequest
import collector.service.AnalyticsService
import com.twitter.finagle.http.Request
import com.twitter.finatra.http.Controller

/**
  * Created by tiennt4 on 23/12/2016.
  */
class MixPanelAnalyticsController  @Inject()(service: AnalyticsService) extends Controller {
get("/mixpanel/track") {
    request: Request => {
      service.process(
        AnalyticRequest("people_analytic", 0,
          new String(Base64.getDecoder.decode(request.params("data"))),
          request.params.getOrElse("_", System.currentTimeMillis().toString).toLong))
      response.ok
    }
  }

  post("/mixpanel/track") {
    request: Request => {
      service.process(
        AnalyticRequest("people_analytic", 0,
          new String(Base64.getDecoder.decode(request.params("data"))),
          request.params.getOrElse("_", System.currentTimeMillis().toString).toLong))
      response.ok
    }
  }

  /**
    * Receive `user info` and update to user-profile
    * @todo implement engage
    */
  post("/mixpanel/engage") {
    req: Request => {
      response.ok(0)
    }
  }

  /**
    * Return configurations
    * @todo implement decide
    */
  get("/mixpanel/decide") {
    req: Request => {
      response.ok("{}")
    }
  }
}
