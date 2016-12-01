package collector.controller.http

import collector.domain.AnalyticRequest
import com.twitter.finatra.http.Controller

/**
 * Created by zkidkid on 11/30/16.
 */
class AnalyticsController extends Controller{

  post("/analytic"){
    request: AnalyticRequest => response.ok()
  }

}
