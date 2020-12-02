package example.api

import akka.http.interop.ZIOSupport
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import example._

import zio._

object Api {

  trait Service {
    def routes: Route
  }

  lazy val live: ZLayer[ContributorsCache, Nothing, Api] =
    ZLayer.fromService { contributorCache =>
      new Service with ZIOSupport {
        def routes =
          pathPrefix("contributors") {
            path(Segment / Segment) { (organization, repository) =>
              get {
                complete {
                  contributorCache.fetch(organization, repository)
                }
              }
            }
          }
      }
    }

  val routes: URIO[Api, Route] = ZIO.access[Api](api => Route.seal(api.get.routes))
}
