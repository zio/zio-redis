package example.domain

sealed trait ApiError {
  def asThrowable: Throwable =
    this match {
      case GithubUnavailable(msg) => new Throwable(msg)
      case NoContributors(msg)    => new Throwable(msg)
    }
}

final case class GithubUnavailable(msg: String) extends ApiError
final case class NoContributors(msg: String)    extends ApiError
