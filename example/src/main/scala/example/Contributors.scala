package example

import example.Contributor._
import example.ApiError._
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
    def fetchContributors(organization: String, repository: String): IO[ApiError, Chunk[Contributor]]
  }

  lazy val live: ZLayer[RedisExecutor with SttpClient, Nothing, Contributors] =
    ZLayer.fromFunction { env =>
      new Service {
        def fetchContributors(organization: String, repository: String): IO[ApiError, Chunk[Contributor]] =
          sMembers(repository).flatMap { response => // key should be unique combination of org + repo
            if (response.isEmpty)
              for {
                contributors <- retrieveContributors(organization, repository)
                _            <- sAdd(repository, contributors.asJson.toString, contributors.map(_.asJson.toString): _*)
                _            <- pExpire(repository, 1.minute)
              } yield contributors
            else
              response
                .mapM(contributor => ZIO.fromEither(decode[Contributor](contributor)))
                .orElseFail(GithubUnavailable)
          }.orElseFail(GithubUnavailable).provide(env)

      }
    }

  private[this] def retrieveContributors(
    organization: String,
    repository: String
  ): ZIO[SttpClient, ApiError, Chunk[Contributor]] =
    SttpClient
      .send(basicRequest.get(urlOf(organization, repository)).response(asJson[Chunk[Contributor]]))
      .flatMap(_.body.fold(_ => ZIO.fail(UnknownProject), ZIO.succeed(_)))
      .orElseFail(GithubUnavailable)

  private[this] def urlOf(organization: String, repository: String): Uri =
    uri"https://api.github.com/repos/$organization/$repository/contributors"

}
