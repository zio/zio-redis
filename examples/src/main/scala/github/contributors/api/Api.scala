package github.contributors.api

import akka.http.interop.{HttpServer, ZIOSupport}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import github.contributors.domain.{Contributor, GithubUnavailable}
import sttp.client._
import sttp.client.circe._
import sttp.client.{NothingT, SttpBackend, basicRequest}
import sttp.client.asynchttpclient.zio.AsyncHttpClientZioBackend
import zio._
import zio.config.ZConfig
import io.circe.syntax._

object Api {

  private val url = "https://api.github.com/repos/zio/zio-redis/contributors"

  trait Service {
    def routes: Route
  }

  val live: ZLayer[ZConfig[HttpServer.Config], Nothing, Api] =
    ZLayer.fromFunction { env =>
      new Service with ZIOSupport {
        import Contributor.encoder
        def routes = contributorRoutes

        val contributorRoutes: Route =
          pathPrefix("contributors") {
            get {
              val response = for {
                backend <- AsyncHttpClientZioBackend()
                contributors <- fetchContributors(backend)
                json <- ZIO.succeed(contributors.asJson.toString)
              } yield json

              complete(response)
            }
          }

        private def fetchContributors(implicit sttpBackend: SttpBackend[Task, Nothing, NothingT]): Task[List[Contributor]] =
          basicRequest
            .get(uri"$url")
            .response(asJson[List[Contributor]])
            .send()
            .map(_.body)
            .flatMap {
              case Right(contributor) => Task.succeed(contributor)
              case Left(error) => Task.fail(GithubUnavailable(error.body))
            }
      }
    }

  val routes: URIO[Api, Route] = ZIO.access[Api](a => Route.seal(a.get.routes))
}
