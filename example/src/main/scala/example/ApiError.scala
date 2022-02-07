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

import scala.util.control.NoStackTrace

import zhttp.http._

sealed trait ApiError extends NoStackTrace { self =>
  import ApiError._

  final def toResponse: Response =
    self match {
      case CorruptedData | GithubUnreachable => Response.fromHttpError(HttpError.InternalServerError())
      case CacheMiss(key)                    => Response.fromHttpError(HttpError.NotFound(Path.empty / key))
      case UnknownProject(path)              => Response.fromHttpError(HttpError.NotFound(Path(path)))
    }
}

object ApiError {
  final case class CacheMiss(key: String)       extends ApiError
  case object CorruptedData                     extends ApiError
  case object GithubUnreachable                 extends ApiError
  final case class UnknownProject(path: String) extends ApiError
}
