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

  case object ChunkOutput extends Output[Chunk[Chunk[Byte]]] {
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
    override def decode(text: String): Either[RedisError, Option[A]] = output.decode(text).map(Some(_))
  }

  case object ScanOutput extends Output[(Long, Chunk[Chunk[Byte]])] {
    // def decode(text: String): Either[RedisError, (Long, Chunk[Chunk[Byte]])] = ???
  }

  case object StringOutput extends Output[String] {
    override def decode(text: String): Either[RedisError, String] =
      Either.cond(text.startsWith("$"), parse(text), ProtocolError(s"$text isn't a string."))

    private def parse(text: String): String = {
      var i = 0
      while (text.charAt(i) != '\n')
        i += 1

      var j = i + 1
      while (text.charAt(j) != '\r')
        j += 1

      text.substring(i + 1, j)
    }
  }

  case object UnitOutput extends Output[Unit] {
    override def decode(text: String): Either[RedisError, Unit] =
      Either.cond(text == "+OK\r\n", (), ProtocolError(s"$text isn't unit."))
  }
}
