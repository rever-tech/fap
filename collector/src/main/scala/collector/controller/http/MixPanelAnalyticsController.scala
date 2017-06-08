package collector.controller.http

import java.awt.Color
import java.awt.image.BufferedImage
import java.io.ByteArrayOutputStream
import java.nio.charset.Charset
import java.util.Base64
import javax.imageio.ImageIO
import javax.inject.Inject

import collector.domain.AnalyticRequest
import collector.service.AnalyticsService
import com.twitter.finagle.http.{MediaType, Request}
import com.twitter.finatra.http.Controller

/**
  * Created by tiennt4 on 23/12/2016.
  */
class MixPanelAnalyticsController @Inject()(service: AnalyticsService) extends Controller {

  //Create gif
  private final val imgInByte = {
    val baos = new ByteArrayOutputStream()
    val singlePixelImage = new BufferedImage(1, 1, BufferedImage.TYPE_4BYTE_ABGR)
    val transparent = new Color(0, 0, 0, 0)
    singlePixelImage.setRGB(0, 0, transparent.getRGB)
    ImageIO.write(singlePixelImage, "gif", baos)
    baos.flush()
    val tmp = baos.toByteArray
    baos.close()
    tmp
  }


  get("/mixpanel/track") {
    request: Request => {
      service.process(
        AnalyticRequest(request.getParam("topic"), request.getIntParam("version", 0),
          new String(Base64.getDecoder.decode(request.getParam("data")), Charset.forName("utf-8"))))
      response.ok.contentType(MediaType.Gif).body(imgInByte)
    }
  }

  post("/mixpanel/track") {
    request: Request => {
      service.process(
        AnalyticRequest(request.getParam("topic"), request.getIntParam("version", 0),
          new String(Base64.getDecoder.decode(request.params("data")), Charset.forName("UTF-8"))))
      response.ok.contentType(MediaType.Gif).body(imgInByte)
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
