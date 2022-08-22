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

package example.api

import example._
import zhttp.http._
import zhttp.service.Server
import zio._
import zio.json._

object Api {

  private val app: HttpApp[ContributorsCache.Service, Nothing] = Http.collectZIO {
    case Method.GET -> !! / "repositories" / owner / name / "contributors" =>
      ZIO
        .serviceWithZIO[ContributorsCache.Service](_.fetchAll(Repository(Owner(owner), Name(name))))
        .mapBoth(_.toResponse, r => Response.json(r.toJson))
        .merge
  }

  val routes: Server[ContributorsCache.Service, Nothing] = Server.app(app)

}
