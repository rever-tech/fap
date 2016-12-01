package collector.controller.http

import javax.inject.Inject

import collector.domain.AnalyticRequest
import collector.service.AnalyticsService
import com.twitter.finatra.http.Controller

/**
 * Created by zkidkid on 11/30/16.
 */
class AnalyticsController @Inject()(service: AnalyticsService) extends Controller {

  post("/analytic") {
    request: AnalyticRequest => {
      service.process(request)
      response.ok()
    }
  }

}
