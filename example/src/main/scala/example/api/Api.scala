package example.api

import akka.http.interop.{HttpServer, ZIOSupport}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import example.config.RedisConfig
import example.domain.ContributorService
import example.domain.ContributorService.SttpClient
import zio._
import zio.config.ZConfig

object Api {

  private val url = "https://api.github.com/repos/zio/zio-redis/contributors"

  trait Service {
    def routes: Route
  }

  val live: ZLayer[ZConfig[HttpServer.Config] with ZConfig[RedisConfig] with SttpClient, Nothing, Api] =
    ZLayer.fromFunction { env =>
      new Service with ZIOSupport {

        def routes = contributorRoutes

        val contributorRoutes: Route =
          pathPrefix("contributors") {
            get {
              complete {
                ContributorService
                  .getContributors(url)
                  .provide(env)
              }
            }
          }
      }
    }

  val routes: URIO[Api, Route] = ZIO.access[Api](api => Route.seal(api.get.routes))
}
