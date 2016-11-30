package collector.controller.http

import javax.inject.{Inject, Singleton}

import com.twitter.finagle.http.{Request, Status}
import com.twitter.finatra.http.Controller
import com.twitter.finatra.request
import collector.domain.{GetCacheRequest, PutCacheRequest, UserID}
import collector.service.UserCacheService
import com.twitter.finatra.utils.FuturePools


/**
  * Created by SangDang on 9/16/16.
  */

@Singleton
class CacheController @Inject()(userCacheService: UserCacheService) extends Controller {
  post("/addUser") { request: PutCacheRequest =>
    userCacheService.addUser(request.userID, request.userInfo)
    response.ok()
  }
  get("/getUser") {
    request: GetCacheRequest =>
      for {
        userInfo <- userCacheService.getUser(UserID(request.userID))
      } yield {
        response.ok(userInfo)
      }
  }

}
