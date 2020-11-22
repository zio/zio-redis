package zio.redis

import java.io.IOException

import scala.util.control.NoStackTrace

sealed trait RedisError extends NoStackTrace

object RedisError {
  final case class ProtocolError(message: String)  extends RedisError
  final case class WrongType(message: String)      extends RedisError
  final case class Moved(message: String)          extends RedisError
  final case class IOError(exception: IOException) extends RedisError

}
