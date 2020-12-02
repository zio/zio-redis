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
    ZLayer.fromService { contributorsCache =>
      new Service with ZIOSupport {
        val routes =
          path("repositories" / Segment / Segment / "contributors") { (organization, name) =>
            get {
              complete {
                contributorsCache.fetchAll(Repository(Organization(organization), Name(name)))
              }
            }
          }
      }
    }
}
