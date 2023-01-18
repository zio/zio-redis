/*
 * Copyright 2021 John A. De Goes and the ZIO contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package example

import example.ApiError._
import sttp.client3.ziojson.asJson
import sttp.client3.{UriContext, basicRequest}
import sttp.model.Uri
import zio._
import zio.json._
import zio.redis._

trait ContributorsCache {
  def fetchAll(repository: Repository): IO[ApiError, Contributors]
}

final case class ContributorsCacheLive(r: Redis, s: Sttp) extends ContributorsCache {
  def fetchAll(repository: Repository): IO[ApiError, Contributors] =
    (read(repository) <> retrieve(repository)).provide(ZLayer.succeed(r), ZLayer.succeed(s))

  private def cache(repository: Repository, contributors: Chunk[Contributor]): URIO[Redis, Any] =
    ZIO
      .fromOption(NonEmptyChunk.fromChunk(contributors))
      .map(Contributors(_).toJson)
      .flatMap(data => ZIO.serviceWithZIO[Redis](_.set(repository.key, data, Some(1.minute))).orDie)
      .ignore

  private def read(repository: Repository): ZIO[Redis, ApiError, Contributors] =
    ZIO
      .serviceWithZIO[Redis](_.get(repository.key).returning[String])
      .someOrFail(ApiError.CacheMiss(repository.key))
      .map(_.fromJson[Contributors])
      .foldZIO(_ => ZIO.fail(ApiError.CorruptedData), s => ZIO.succeed(s.getOrElse(Contributors(Chunk.empty))))

  private def retrieve(repository: Repository): ZIO[Redis with Sttp, ApiError, Contributors] =
    for {
      req          <- ZIO.succeed(basicRequest.get(urlOf(repository)).response(asJson[Chunk[Contributor]]))
      res          <- ZIO.service[Sttp].flatMap(_.send(req).orElseFail(GithubUnreachable))
      contributors <- res.body.fold(_ => ZIO.fail(UnknownProject(urlOf(repository).toString)), ZIO.succeed(_))
      _            <- cache(repository, contributors)
    } yield Contributors(contributors)

  private def urlOf(repository: Repository): Uri =
    uri"https://api.github.com/repos/${repository.owner}/${repository.name}/contributors"
}

object ContributorsCacheLive {
  lazy val layer: URLayer[Redis with Sttp, ContributorsCache] =
    ZLayer.fromFunction(ContributorsCacheLive.apply _)
}
