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

object ContributorsCache {
  trait Service {
    def fetchAll(organization: Organization, repository: Repository): IO[ApiError, Contributors]
  }

  lazy val live: ZLayer[RedisExecutor with SttpClient, Nothing, ContributorsCache] =
    ZLayer.fromFunction { env =>
      new Service {
        def fetchAll(organization: Organization, repository: Repository): IO[ApiError, Contributors] = {
          val key = s"$organization:$repository"

          sMembers(key).flatMap { data =>
            if (data.isEmpty)
              for {
                contributors <- retrieveContributors(organization, repository)
                _            <- sAdd(key, contributors.asJson.toString, contributors.map(_.asJson.toString): _*)
                _            <- pExpire(key, 1.minute)
              } yield Contributors(contributors)
            else
              data
                .mapM(contributor => ZIO.fromEither(decode[Contributor](contributor)))
                .map(Contributors(_))
                .orElseFail(GithubUnavailable)
          }.orElseFail(GithubUnavailable).provide(env)
        }
      }
    }

  private[this] def retrieveContributors(
    organization: Organization,
    repository: Repository
  ): ZIO[SttpClient, ApiError, Chunk[Contributor]] =
    SttpClient
      .send(basicRequest.get(urlOf(organization, repository)).response(asJson[Chunk[Contributor]]))
      .flatMap(_.body.fold(_ => ZIO.fail(UnknownProject), ZIO.succeed(_)))
      .orElseFail(GithubUnavailable)

  private[this] def urlOf(organization: Organization, repository: Repository): Uri =
    uri"https://api.github.com/repos/$organization/$repository/contributors"

}
