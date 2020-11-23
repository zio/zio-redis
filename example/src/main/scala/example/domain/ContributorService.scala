package example.domain

import zio._
import sttp.client._
import sttp.client.asynchttpclient.zio.SttpClient
import sttp.client.basicRequest
import sttp.client.circe.asJson
import zio.redis._

object ContributorService {

  trait Service {
    def getContributors(url: String): IO[String, Chunk[Contributor]]
  }

  lazy val live: ZLayer[RedisExecutor with SttpClient, Nothing, ContributorService] =
    ZLayer.fromServices[RedisExecutor.Service, SttpClient.Service, Service] { (re, sttp) =>
      new Service {
        override def getContributors(organization: String) = ???

      }
    }

/*  def getContributors(url: String): ZIO[RedisExecutor with SttpClient, Throwable, Chunk[Contributor]] =
    ZIO.accessM[SttpClient] { sttpClient =>
      val request = basicRequest
        .get(uri"$url")
        .response(asJson[List[Contributor]])

      sttpClient.get
        .send(request)
        .map(_.body)
        .flatMap {
          case Right(contributors) => ZIO.succeed(Chunk.fromIterable(contributors))
          case Left(error) => ZIO.fail(GithubUnavailable(error.body))
        }
    }*/
}
