package example.api

import akka.http.interop.{HttpServer, ZIOSupport}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import example.config.RedisConfig
import example.domain.ContributorService
import sttp.client.asynchttpclient.zio.SttpClient
import zio._
import zio.clock.Clock
import zio.redis.RedisExecutor
import zio.config.ZConfig
import zio.logging.Logging

object Api {

  private val url = "https://api.github.com/repos/zio/zio-redis/contributors"

  trait Service {
    def routes: Route
  }

  val live: ZLayer[ContributorService, Nothing, Api] =
    ZLayer.fromService { contributorService =>
      new Service with ZIOSupport {
        def routes =
          pathPrefix("contributors") {
            get {
              complete {
                  contributorService.getContributors(url)
              }
            }
          }
      }
    }

  val routes: URIO[Api, Route] = ZIO.access[Api](api => Route.seal(api.get.routes))
}
