package example.api

import example._
import zhttp.http.HttpApp
import zio._
import zhttp.http._
import zhttp.service.Server
import zio.json._

object Api {

  val app: HttpApp[ContributorsCache, Nothing] = HttpApp.collectM {
    case Method.GET -> Root / "repositories" / owner / name / "contributors" =>
      ZIO
        .serviceWith[ContributorsCache.Service](_.fetchAll(Repository(Owner(owner), Name(name))))
        .bimap(ApiError.errorResponse, r => Response.jsonString(r.toJson))
        .merge
  }

  val server = Server.app(app)

}
