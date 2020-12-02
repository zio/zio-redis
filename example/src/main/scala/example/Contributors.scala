package example

import example.Contributor._
import example.ApiError._
import io.circe.Error
import io.circe.parser.decode
import io.circe.syntax._
import sttp.client.asynchttpclient.zio.SttpClient
import sttp.client.circe.asJson
import sttp.client.{ UriContext, basicRequest }
import sttp.model.Uri

import zio._
import zio.duration._
import zio.redis._

object Contributors {

  trait Service {
    def getContributors(organization: String, repository: String): IO[ApiError, Chunk[Contributor]]
  }

  lazy val live: ZLayer[RedisExecutor with SttpClient, Nothing, Contributors] =
    ZLayer.fromFunction { env =>
      new Service {
        def getContributors(organization: String, repository: String): IO[ApiError, Chunk[Contributor]] =
          sMembers(repository).flatMap { response => // key should be unique combination of org + repo
            if (response.isEmpty)
              for {
                contributors <- fetchContributors(organization, repository)
                _            <- sAdd(repository, contributors.asJson.toString, contributors.map(_.asJson.toString): _*)
                _            <- pExpire(repository, 1.minute)
              } yield contributors
            else
              deserialize(response).orElseFail(GithubUnavailable)
          }.orElseFail(GithubUnavailable).provide(env)

        private def deserialize(response: Chunk[String]): IO[Error, Chunk[Contributor]] =
          response.mapM(contributor => ZIO.fromEither(decode[Contributor](contributor)))

        private def fetchContributors(organization: String, repository: String): IO[ApiError, Chunk[Contributor]] =
          SttpClient
            .send(basicRequest.get(urlOf(organization, repository)).response(asJson[List[Contributor]]))
            .flatMap(_.body.fold(_ => ZIO.fail(UnknownProject), vals => ZIO.succeed(Chunk.fromIterable(vals))))
            .orElseFail(GithubUnavailable)
            .provide(env)

        private def urlOf(organization: String, repository: String): Uri =
          uri"https://api.github.com/repos/$organization/$repository/contributors"
      }
    }
}
