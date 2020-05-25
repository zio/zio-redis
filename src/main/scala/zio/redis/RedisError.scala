package zio.redis

sealed trait RedisError

object RedisError {
  final case class ProtocolError(message: String) extends RedisError
  final case class WrongType(message: String)     extends RedisError

  val asProtocolError: PartialFunction[Throwable, RedisError] = {
    case t: Throwable => ProtocolError(t.getMessage)
  }
}
