package collector.controller.http

import java.awt.Color
import java.awt.image.BufferedImage
import java.io.ByteArrayOutputStream
import javax.imageio.ImageIO
import javax.inject.Inject

import collector.controller.http.filter.GifTrackingPathFilter
import collector.domain.AnalyticRequest
import collector.service.AnalyticsService
import com.twitter.finagle.http.{MediaType, Request}
import com.twitter.finatra.http.Controller

/**
  * Created by zkidkid on 11/30/16.
  */
class AnalyticsController @Inject()(service: AnalyticsService) extends Controller {

  private final val gifNamePattern = "^([\\w-]+)_v(\\d+).gif".r

  post("/analytic", name = "Analytic") {
    request: AnalyticRequest => {
      service.process(request)
      response.ok()
    }
  }

  //Create gif
  private val imgInByte = {
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

  filter[GifTrackingPathFilter].get("/__:*", name = "GIF Analytic") {
    request: Request => {
      val gifName = request.params("*")
      service.process(AnalyticRequest("utm", 1, request.params - "*"))
      response.ok.contentType(MediaType.Gif).body(imgInByte)
    }
  }

}
