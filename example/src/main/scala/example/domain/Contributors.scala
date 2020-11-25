package example.domain

import io.circe
import io.circe.syntax._
import zio._
import zio.redis._
import io.circe.parser.decode
import sttp.client.asynchttpclient.zio.SttpClient
import sttp.client.{UriContext, basicRequest}
import sttp.client.circe.asJson
import sttp.model.Uri
import Contributor._
import zio.duration._

object Contributors {

  trait Service {
    def getContributors(organization: String, repository: String): IO[ApiError, Chunk[Contributor]]
  }

  lazy val live: ZLayer[RedisExecutor with SttpClient, Nothing, Contributors] =
    ZLayer.fromFunction { env =>
      new Service {
        override def getContributors(organization: String, repository: String): IO[ApiError, Chunk[Contributor]] =
          sMembers(repository).flatMap { response => // key should be unique combination of org + repo
              if (response.isEmpty)
                for {
                  contributors <- fetchContributors(organization, repository)
                  _ <- sAdd(repository, contributors.asJson.toString, contributors.map(_.asJson.toString):_*)
                  _ <- pExpire(repository, 1.minute)
                } yield contributors
              else
                deserialize(response).orElseFail(GithubUnavailable("Github Client Unavailable"))
            }.orElseFail(GithubUnavailable("Github Client Unavailable")).provide(env)

        private def deserialize(response: Chunk[String]): IO[circe.Error, Chunk[Contributor]] = {
          response.mapM(contributor => ZIO.fromEither(decode[Contributor](contributor)))
        }

        private def fetchContributors(organization: String, repository: String): IO[ApiError, Chunk[Contributor]] = {
          val request = basicRequest
            .get(buildUrl(organization, repository))
            .response(asJson[List[Contributor]])

          SttpClient.send(request).map(_.body)
            .flatMap { case Right(value) => ZIO.succeed(Chunk.fromIterable(value)) }
            .orElseFail(GithubUnavailable("Github Client Unavailable"))
            .provide(env)
        }

        private def buildUrl(organization: String, repository: String): Uri =
          uri"https://api.github.com/repos/$organization/$repository/contributors"
      }
    }
}
