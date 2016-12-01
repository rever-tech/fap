package collector.controller

import collector.Server
import com.twitter.finatra.http.EmbeddedHttpServer
import com.twitter.inject.server.FeatureTest

/**
 * Created by zkidkid on 12/1/16.
 */
class AnalyticsControllerTest extends FeatureTest {
  override protected val server = new EmbeddedHttpServer(twitterServer = new Server)

}
