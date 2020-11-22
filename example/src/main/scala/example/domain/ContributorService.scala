package example.domain

import zio._
import sttp.client._
import sttp.client.basicRequest
import sttp.client.circe.asJson

object ContributorService {

  type SttpClient = Has[SttpBackend[Task, Nothing, NothingT]]

  def getContributors(url: String): ZIO[SttpClient, Throwable, List[Contributor]] =
    ZIO.accessM[SttpClient] { sttpClient =>
      val request = basicRequest
        .get(uri"$url")
        .response(asJson[List[Contributor]])

      sttpClient.get
        .send(request)
        .map(_.body)
        .flatMap {
          case Right(contributors) => ZIO.succeed(contributors)
          case Left(error) => ZIO.fail(GithubUnavailable(error.body))
        }
    }
}
