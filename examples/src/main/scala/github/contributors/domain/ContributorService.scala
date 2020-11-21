package github.contributors.domain

import zio._
import sttp.client._
import github.contributors.api.SttpClient
import sttp.client.basicRequest
import sttp.client.circe.asJson

object ContributorService {

  def getContributors(url: String): ZIO[SttpClient, Throwable, List[Contributor]] = {
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
}
