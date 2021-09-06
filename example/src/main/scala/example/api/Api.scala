package example.api

import example._
import zhttp.http.{ HttpApp, _ }
import zhttp.service.Server

import zio._
import zio.json._

object Api {

  private val app: HttpApp[ContributorsCache, Nothing] = HttpApp.collectM {
    case Method.GET -> Root / "repositories" / owner / name / "contributors" =>
      ZIO
        .serviceWith[ContributorsCache.Service](_.fetchAll(Repository(Owner(owner), Name(name))))
        .mapBoth(ApiError.errorResponse, r => Response.jsonString(r.toJson))
        .merge
  }

  val routes: Server[ContributorsCache, Nothing] = Server.app(app)

}
