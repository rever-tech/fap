package collector.controller.http.filter

import com.google.inject.Inject
import com.google.inject.name.Named
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.util.Future

/**
  * Created by tiennt4 on 20/12/2016.
  */
class GifTrackingPathFilter @Inject()(@Named("validGifName") validGifName: Set[String])
  extends SimpleFilter[Request, Response] {

  override def apply(request: Request, service: Service[Request, Response]): Future[Response] = {
    if (validGifName.contains(request.params("*")) && request.params("*").endsWith(".gif"))
      service.apply(request)
    else Future.exception(new GifNotFoundException)
  }
}

class GifNotFoundException extends Exception