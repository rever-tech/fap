package collector.controller.http

import java.nio.charset.Charset
import java.util.Base64
import javax.inject.Inject

import collector.domain.AnalyticRequest
import collector.service.AnalyticsService
import com.twitter.finagle.http.Request
import com.twitter.finatra.http.Controller

/**
  * Created by tiennt4 on 23/12/2016.
  */
class MixPanelAnalyticsController @Inject()(service: AnalyticsService) extends Controller {

  get("/mixpanel/track") {
    request: Request => {
      service.process(
        AnalyticRequest(request.getParam("topic"), request.getIntParam("version", 0),
          new String(Base64.getDecoder.decode(request.getParam("data")), Charset.forName("utf-8"))))
      response.ok
    }
  }

  post("/mixpanel/track") {
    request: Request => {
      service.process(
        AnalyticRequest(request.getParam("topic"), request.getIntParam("version", 0),
          new String(Base64.getDecoder.decode(request.params("data")), Charset.forName("UTF-8"))))

      response.ok
    }
  }

  /**
    * Receive `user info` and update to user-profile
    *
    */
  post("/mixpanel/engage") {
    req: Request => {
      service.process(AnalyticRequest("user_info", req.getIntParam("version", 1),
        new String(Base64.getDecoder.decode(req.getParam("data")), Charset.forName("utf-8"))))
      response.ok(0)
    }
  }

  get("/mixpanel/engage") {
    req: Request => {
      service.process(AnalyticRequest("user_info", req.getIntParam("version", 1),
        new String(Base64.getDecoder.decode(req.getParam("data")), Charset.forName("utf-8"))))
      response.ok(0)
    }
  }

  /**
    * Return configurations
    *
    * @todo implement decide
    */
  get("/mixpanel/decide") {
    req: Request => {
      response.ok("{}")
    }
  }
}
