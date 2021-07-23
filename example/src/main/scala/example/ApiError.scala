package example

import scala.util.control.NoStackTrace

import zhttp.http._

sealed trait ApiError extends NoStackTrace

object ApiError {
  final case class CacheMiss(key: String)       extends ApiError
  case object CorruptedData                     extends ApiError
  case object GithubUnreachable                 extends ApiError
  final case class UnknownProject(path: String) extends ApiError

  val errorResponse: ApiError => UResponse = {
    case CorruptedData | GithubUnreachable => Response.fromHttpError(HttpError.InternalServerError())
    case CacheMiss(key)                    => Response.fromHttpError(HttpError.NotFound(Path.empty / key))
    case UnknownProject(path)              => Response.fromHttpError(HttpError.NotFound(Path(path)))
  }

}
