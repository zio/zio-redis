package example

import akka.http.interop.ErrorResponse
import akka.http.scaladsl.model.{ HttpResponse, StatusCodes }

import scala.util.control.NoStackTrace

sealed trait ApiError extends NoStackTrace

object ApiError {
  case object CacheMiss         extends ApiError
  case object CorruptedData     extends ApiError
  case object GithubUnreachable extends ApiError
  case object UnknownProject    extends ApiError

  implicit val errorResponse: ErrorResponse[ApiError] = {
    case CorruptedData | GithubUnreachable => HttpResponse(StatusCodes.InternalServerError)
    case CacheMiss | UnknownProject        => HttpResponse(StatusCodes.NotFound)
  }

}
