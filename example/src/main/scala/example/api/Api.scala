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
        val routes =
          path("organizations" / Segment / "repositories" / Segment / "contributors") { (organization, repository) =>
            get {
              complete {
                contributorCache.fetchAll(Organization(organization), Repository(repository))
              }
            }
          }
      }
    }

  val routes: URIO[Api, Route] = ZIO.access(api => Route.seal(api.get.routes))
}
