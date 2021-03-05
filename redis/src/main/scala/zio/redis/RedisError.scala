package zio.redis

import java.io.IOException

import scala.util.control.NoStackTrace

sealed trait RedisError extends NoStackTrace

object RedisError {
  final case class ProtocolError(message: String) extends RedisError {
    override def toString: String = s"ProtocolError: $message"
  }
  final case class WrongType(message: String)      extends RedisError
  final case class BusyGroup(message: String)      extends RedisError
  final case class NoGroup(message: String)        extends RedisError
  final case class IOError(exception: IOException) extends RedisError
}
