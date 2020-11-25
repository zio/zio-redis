package example.api

import zio._
import akka.http.interop.{ErrorResponse, ZIOSupport}
import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import example.domain.{ApiError, Contributors, GithubUnavailable, NoContributors}
object Api {

  trait Service {
    def routes: Route
  }

  lazy val live: ZLayer[Contributors, Nothing, Api] =
    ZLayer.fromService { contributorService =>
      new Service with ZIOSupport {

        implicit val apiErrorResponse: ErrorResponse[ApiError] = {
          case GithubUnavailable(_) => HttpResponse(StatusCodes.InternalServerError)
          case NoContributors(_) => HttpResponse(StatusCodes.BadRequest)
        }

        def routes =
          pathPrefix("contributors") {
            path(Segment / Segment) { (organization, repository) =>
              get {
                complete {
                  contributorService.getContributors(organization, repository)
                }
              }
            }
          }
      }
    }

  val routes: URIO[Api, Route] = ZIO.access[Api](api => Route.seal(api.get.routes))
}
