package example

import akka.http.interop.ErrorResponse
import akka.http.scaladsl.model.{ HttpResponse, StatusCodes }

import scala.util.control.NoStackTrace

sealed trait ApiError extends NoStackTrace

object ApiError {
  case object GithubUnavailable extends ApiError
  case object UnknownProject    extends ApiError

  implicit val apiErrorResponse: ErrorResponse[ApiError] = {
    case GithubUnavailable => HttpResponse(StatusCodes.InternalServerError)
    case UnknownProject    => HttpResponse(StatusCodes.NotFound)
  }

}
