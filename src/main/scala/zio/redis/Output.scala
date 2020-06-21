package zio.redis

import zio.Chunk
import zio.duration.Duration

sealed trait Output[+A] {
  private[redis] def decode(text: String): Either[RedisError, A] = Left(RedisError.WrongType(text))
}

object Output {
  import RedisError._

  case object BoolOutput extends Output[Boolean] {
    // def decode(text: String): Either[RedisError, Boolean] = ???
  }

  case object ChunkOutput extends Output[Chunk[String]] {
    // def decode(text: String): Either[RedisError, Chunk[Chunk[Byte]]] = ???
  }

  case object DoubleOutput extends Output[Double] {
    // def decode(text: String): Either[RedisError, Double] = ???
  }

  case object DurationOutput extends Output[Duration] {
    // def decode(text: String): Either[RedisError, Duration] = ???
  }

  case object LongOutput extends Output[Long] {
    // def decode(text: String): Either[RedisError, Long] = ???
  }

  final case class OptionalOutput[+A](output: Output[A]) extends Output[Option[A]] {
    override def decode(text: String): Either[RedisError, Option[A]] =
      if (text.startsWith("$-1")) Right(None) else output.decode(text).map(Some(_))
  }

  case object ScanOutput extends Output[(Long, Chunk[String])] {
    // def decode(text: String): Either[RedisError, (Long, Chunk[Chunk[Byte]])] = ???
  }

  case object StringOutput extends Output[String] {
    override def decode(text: String): Either[RedisError, String] =
      Either.cond(text.startsWith("$"), parse(text), ProtocolError(s"$text isn't a string."))

    private def parse(text: String): String = {
      var pos = 1
      var len = 0

      while (text.charAt(pos) != '\r') {
        len = len * 10 + (text.charAt(pos) - '0')
        pos += 1
      }

      // skip to the first payload char
      pos += 2

      text.substring(pos, pos + len)
    }
  }

  case object UnitOutput extends Output[Unit] {
    override def decode(text: String): Either[RedisError, Unit] =
      Either.cond(text == "+OK\r\n", (), ProtocolError(s"$text isn't unit."))
  }
}
