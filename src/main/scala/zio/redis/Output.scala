package zio.redis

import zio.Chunk
import zio.duration._

sealed trait Output[+A] {
  private[redis] final def decode(text: String): Either[RedisError, A] = {
    val error = decodeError(text)

    if (error eq null) tryDecode(text) else Left(error)
  }

  private[this] def decodeError(text: String): RedisError =
    if (text.startsWith("-ERR"))
      RedisError.ProtocolError(text.drop(4).trim())
    else if (text.startsWith("-WRONGTYPE"))
      RedisError.WrongType(text.drop(10).trim())
    else
      null

  protected def tryDecode(text: String): Either[RedisError, A]
}

object Output {
  import RedisError._

  case object BoolOutput extends Output[Boolean] {
    protected def tryDecode(text: String): Either[RedisError, Boolean] =
      if (text == ":1\r\n")
        Right(true)
      else if (text == ":0\r\n")
        Right(false)
      else
        Left(ProtocolError(s"$text isn't a boolean."))
  }

  case object ChunkOutput extends Output[Chunk[String]] {
    protected def tryDecode(text: String): Either[RedisError, Chunk[String]] =
      Either.cond(text.startsWith("*"), parse(text), ProtocolError(s"$text isn't an array."))

    private[this] def parse(text: String): Chunk[String] = {
      var pos = 1
      var len = 0

      while (text.charAt(pos) != '\r') {
        len = len * 10 + text.charAt(pos) - '0'
        pos += 1
      }

      if (len == 0) Chunk.empty
      else {
        val data = Array.ofDim[String](len)
        var idx  = 0

        while (idx < len) {
          // skip to the first size character
          pos += 3

          var itemLen = 0

          while (text.charAt(pos) != '\r') {
            itemLen = itemLen * 10 + text.charAt(pos) - '0'
            pos += 1
          }

          // skip to the first payload char
          pos += 2

          data(idx) = text.substring(pos, pos + itemLen)
          idx += 1
          pos += itemLen
        }

        Chunk.fromArray(data)
      }
    }
  }

  case object DoubleOutput extends Output[Double] {
    protected def tryDecode(text: String): Either[RedisError, Double] =
      if (text.startsWith("$"))
        parse(text)
      else
        Left(ProtocolError(s"$text isn't a double."))

    private[this] def parse(text: String): Either[RedisError, Double] = {
      var pos = 1
      var len = 0

      while (text.charAt(pos) != '\r') {
        len = len * 10 + text.charAt(pos) - '0'
        pos += 1
      }

      // skip to the first payload char
      pos += 2

      try Right(text.substring(pos, pos + len).toDouble)
      catch {
        case _: Throwable => Left(ProtocolError(s"$text isn't a double."))
      }
    }
  }

  case object DurationMillisecondsOutput extends Output[Duration] {
    protected def tryDecode(text: String): Either[RedisError, Duration] =
      if (text.startsWith(":"))
        parse(text)
      else
        Left(ProtocolError(s"$text isn't a duration."))

    private[this] def parse(text: String): Either[RedisError, Duration] = {
      val value = unsafeReadNumber(text)

      if (value == -2)
        Left(ProtocolError("Key not found."))
      else if (value == -1)
        Left(ProtocolError("Key has no expire."))
      else
        Right(value.millis)
    }
  }

  case object DurationSecondsOutput extends Output[Duration] {
    protected def tryDecode(text: String): Either[RedisError, Duration] =
      if (text.startsWith(":"))
        parse(text)
      else
        Left(ProtocolError(s"$text isn't a duration."))

    private[this] def parse(text: String): Either[RedisError, Duration] = {
      val value = unsafeReadNumber(text)

      if (value == -2)
        Left(ProtocolError("Key not found."))
      else if (value == -1)
        Left(ProtocolError("Key has no expire."))
      else
        Right(value.seconds)
    }
  }

  case object LongOutput extends Output[Long] {
    protected def tryDecode(text: String): Either[RedisError, Long] =
      Either.cond(text.startsWith(":"), unsafeReadNumber(text), ProtocolError(s"$text isn't a number."))
  }

  final case class OptionalOutput[+A](output: Output[A]) extends Output[Option[A]] {
    protected def tryDecode(text: String): Either[RedisError, Option[A]] =
      if (text.startsWith("$-1")) Right(None) else output.tryDecode(text).map(Some(_))
  }

  case object ScanOutput extends Output[(Long, Chunk[String])] {
    protected def tryDecode(text: String): Either[RedisError, (Long, Chunk[String])] = ???
  }

  case object StringOutput extends Output[String] {
    protected def tryDecode(text: String): Either[RedisError, String] =
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
    protected def tryDecode(text: String): Either[RedisError, Unit] =
      Either.cond(text == "+OK\r\n", (), ProtocolError(s"$text isn't unit."))
  }

  private[this] def unsafeReadNumber(text: String): Long = {
    var pos = 1

    while (text.charAt(pos) != '\r')
      pos += 1

    text.substring(1, pos).toLong
  }
}
