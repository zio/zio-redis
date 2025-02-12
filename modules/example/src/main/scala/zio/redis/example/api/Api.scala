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

package zio.redis.example.api

import zio._
import zio.http.codec.HttpCodec
import zio.http.codec.PathCodec.string
import zio.http.endpoint.AuthType.None
import zio.http.endpoint.Endpoint
import zio.http.{Method, Routes, Status, handler}
import zio.redis.example._

object Api {
  private val `GET /repositories/:owner/:name/contributors`
    : Endpoint[(String, String), (String, String), ApiError, Contributors, None] =
    Endpoint(Method.GET / "repositories" / string("owner") / string("name") / "contributors")
      .out[Contributors]
      .outErrors[ApiError](
        HttpCodec.error[ApiError.UnknownProject](Status.NotFound),
        HttpCodec.error[ApiError.CacheMiss](Status.NotFound),
        HttpCodec.error[ApiError.CorruptedData](Status.InternalServerError),
        HttpCodec.error[ApiError.GithubUnreachable](Status.InternalServerError)
      )

  private val fetchContributors = {
    // Had to extract this function to a val otherwise the compiler would complain about the type of the handler
    val fetchAll: (String, String) => ZIO[ContributorsCache, ApiError, Contributors] =
      (owner, name) => ZIO.serviceWithZIO[ContributorsCache](_.fetchAll(Repository(Owner(owner), Name(name))))

    `GET /repositories/:owner/:name/contributors`.implementHandler(handler(fetchAll))
  }

  val routes: Routes[ContributorsCache, Nothing] = Routes(fetchContributors)
}
