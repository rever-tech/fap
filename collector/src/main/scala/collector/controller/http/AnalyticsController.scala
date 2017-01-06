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

  post("/analytic", name = "Analytic") {
    request: AnalyticRequest => {
      service.process(request)
      response.ok()
    }
  }



  private final val gifNamePatternWithVersion = "^([\\w-]+)_v(\\d+).gif".r
  private final val gifNamePatternWithoutVersion = "^([\\w-]+).gif".r
  filter[GifTrackingPathFilter].get("/__:*", name = "GIF Analytic") {
    request: Request => {
      request.params("*") match {
        case gifNamePatternWithVersion(name, version) =>
          service.process(AnalyticRequest(name, version.toInt, request.params - "*"))
          response.ok.contentType(MediaType.Gif).body(imgInByte)

        case gifNamePatternWithoutVersion(name) =>
          service.process(AnalyticRequest(name, 0, request.params - "*"))
          response.ok.contentType(MediaType.Gif).body(imgInByte)

        case _ => response.notFound
      }
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
}
