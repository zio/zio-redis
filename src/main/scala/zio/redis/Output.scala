package zio.redis

import zio.Chunk
import zio.duration.Duration

sealed trait Output[+A] {
  private[redis] final def decode(text: String): Either[RedisError, A] =
    decodeError(text).fold(tryDecode(text))(Left(_))

  private[this] def decodeError(text: String): Option[RedisError] =
    if (text.startsWith("-ERR"))
      Some(RedisError.ProtocolError(text.drop(4).trim()))
    else if (text.startsWith("-WRONGTYPE"))
      Some(RedisError.WrongType(text.drop(10).trim()))
    else
      None

  protected def tryDecode(text: String): Either[RedisError, A]
}

object Output {
  import RedisError._

  case object BoolOutput extends Output[Boolean] {
    def tryDecode(text: String): Either[RedisError, Boolean] =
      if (text == ":1\r\n")
        Right(true)
      else if (text == ":0\r\n")
        Right(false)
      else
        Left(ProtocolError(s"$text isn't a boolean."))
  }

  case object ChunkOutput extends Output[Chunk[String]] {
    def tryDecode(text: String): Either[RedisError, Chunk[String]] = ???
  }

  case object DoubleOutput extends Output[Double] {
    def tryDecode(text: String): Either[RedisError, Double] = ???
  }

  case object DurationOutput extends Output[Duration] {
    def tryDecode(text: String): Either[RedisError, Duration] = ???
  }

  case object LongOutput extends Output[Long] {
    def tryDecode(text: String): Either[RedisError, Long] =
      Either.cond(text.startsWith(":"), parse(text), ProtocolError(s"$text isn't an integer."))

    private[this] def parse(text: String): Long = {
      var pos   = 1
      var value = 0L

      while (text.charAt(pos) != '\r') {
        value = value * 10 + text.charAt(pos) - '0'
        pos += 1
      }

      value
    }
  }

  final case class OptionalOutput[+A](output: Output[A]) extends Output[Option[A]] {
    def tryDecode(text: String): Either[RedisError, Option[A]] =
      if (text.startsWith("$-1")) Right(None) else output.tryDecode(text).map(Some(_))
  }

  case object ScanOutput extends Output[(Long, Chunk[String])] {
    def tryDecode(text: String): Either[RedisError, (Long, Chunk[String])] = ???
  }

  case object StringOutput extends Output[String] {
    def tryDecode(text: String): Either[RedisError, String] =
      Either.cond(text.startsWith("$"), parse(text), ProtocolError(s"$text isn't a string."))

    private[this] def parse(text: String): String = {
      var pos = 1
      var len = 0

      while (text.charAt(pos) != '\r') {
        len = len * 10 + text.charAt(pos) - '0'
        pos += 1
      }

      // skip to the first payload char
      pos += 2

      text.substring(pos, pos + len)
    }
  }

  case object UnitOutput extends Output[Unit] {
    def tryDecode(text: String): Either[RedisError, Unit] =
      Either.cond(text == "+OK\r\n", (), ProtocolError(s"$text isn't unit."))
  }
}
