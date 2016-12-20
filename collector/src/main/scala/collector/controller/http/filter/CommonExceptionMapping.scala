package collector.controller.http.filter

import java.io.IOException
import java.lang.reflect.InvocationTargetException
import javax.inject.Inject
import javax.management.{InstanceAlreadyExistsException, InstanceNotFoundException}
import com.twitter.finagle.http.{Request, Response, ResponseProxy}
import com.twitter.finatra.http.exceptions.ExceptionMapper
import com.twitter.finatra.http.response.ResponseBuilder

/**
  * Create by PhuongLam at 07-10-2016
  */
class CommonExceptionMapping @Inject()(response: ResponseBuilder) extends ExceptionMapper[Exception] {
  override def toResponse(request: Request, exception: Exception): Response = {
    exception match {
      case e: IOException => this.badRequest(exception)
      case e2: IllegalArgumentException => this.badRequest(exception)
      case e: InvocationTargetException => e.getTargetException match {
        case te: NoSuchElementException => this.badRequest(e)
      }
      case e: InstanceNotFoundException => this.notFound(e)
      case e: InstanceAlreadyExistsException => this.conflict(e)
      case _ => this.internalServerError(exception)
    }
  }

  private def badRequest(e: Exception): ResponseProxy = {
    response.badRequest(ErrorResponse(-400, e.getLocalizedMessage))
  }

  private def notFound(e: Exception): ResponseProxy = {
    response.notFound(ErrorResponse(-404, e.getLocalizedMessage))
  }

  private def conflict(e: Exception): ResponseProxy = {
    response.conflict(ErrorResponse(-409, e.getLocalizedMessage))
  }

  private def internalServerError(e: Exception): ResponseProxy = {
    response.internalServerError(ErrorResponse(-500, e.getLocalizedMessage))
  }
}

case class ErrorResponse(code: Int, msg: String)