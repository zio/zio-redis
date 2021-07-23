package example

import zhttp.http._

import scala.util.control.NoStackTrace

sealed trait ApiError extends NoStackTrace

object ApiError {
  case class CacheMiss(key: String)       extends ApiError
  case object CorruptedData               extends ApiError
  case object GithubUnreachable           extends ApiError
  case class UnknownProject(path: String) extends ApiError

  val errorResponse: ApiError => UResponse = {
    case CorruptedData | GithubUnreachable => Response.fromHttpError(HttpError.InternalServerError())
    case CacheMiss(key)                    => Response.fromHttpError(HttpError.NotFound(Path.empty / key))
    case UnknownProject(path)              => Response.fromHttpError(HttpError.NotFound(Path(path)))
  }

}
