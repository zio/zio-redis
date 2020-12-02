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
        def fetchAll(organization: Organization, repository: Repository): IO[ApiError, Contributors] =
          (read(organization, repository) <> retrieve(organization, repository))
            .refineToOrDie[ApiError]
            .provide(env)
      }
    }

  private[this] def read(organization: Organization, repository: Repository) =
    sMembers(keyOf(organization, repository)).flatMap(decodeCached)

  private[this] def decodeCached(data: Chunk[String]) =
    data.mapM(c => ZIO.fromEither(decode[Contributor](c)).orElseFail(CorruptedData)).map(Contributors(_))

  private[this] def retrieve(
    organization: Organization,
    repository: Repository
  ): ZIO[RedisExecutor with SttpClient, ApiError, Contributors] =
    for {
      req          <- ZIO.succeed(basicRequest.get(urlOf(organization, repository)).response(asJson[Chunk[Contributor]]))
      res          <- SttpClient.send(req).orElseFail(GithubUnreachable)
      contributors <- res.body.fold(_ => ZIO.fail(UnknownProject), ZIO.succeed(_))
      key           = keyOf(organization, repository)
      _            <- cache(key, contributors)
    } yield Contributors(contributors)

  private def cache(key: String, contributors: Chunk[Contributor]): URIO[RedisExecutor, Any] =
    ZIO
      .fromOption(NonEmptyChunk.fromChunk(contributors))
      .flatMap { contributors =>
        val json = contributors.map(_.asJson.noSpaces)
        (sAdd(key, json.head, json.tail: _*) *> pExpire(key, 1.minute)).orDie
      }
      .orElse(ZIO.unit)

  private def keyOf(organization: Organization, repository: Repository): String = s"$organization:$repository"

  private[this] def urlOf(organization: Organization, repository: Repository): Uri =
    uri"https://api.github.com/repos/$organization/$repository/contributors"

}
