package zio.redis

import zio.Chunk
import zio.duration._

sealed trait Output[+A] {
  private[redis] final def unsafeDecode(text: String): A = {
    val error = decodeError(text)

    if (error eq null) tryDecode(text) else throw error
  }

  private[this] def decodeError(text: String): RedisError =
    if (text.startsWith("-ERR"))
      RedisError.ProtocolError(text.drop(4).trim())
    else if (text.startsWith("-WRONGTYPE"))
      RedisError.WrongType(text.drop(10).trim())
    else
      null

  protected def tryDecode(text: String): A
}

object Output {
  import RedisError._

  case object BoolOutput extends Output[Boolean] {
    protected def tryDecode(text: String): Boolean =
      if (text == ":1\r\n")
        true
      else if (text == ":0\r\n")
        false
      else
        throw ProtocolError(s"$text isn't a boolean.")
  }

  case object ChunkOutput extends Output[Chunk[String]] {
    protected def tryDecode(text: String): Chunk[String] =
      if (text.startsWith("*"))
        unsafeReadChunk(text, 0)
      else
        throw ProtocolError(s"$text isn't an array.")
  }

  case object DoubleOutput extends Output[Double] {
    protected def tryDecode(text: String): Double =
      if (text.startsWith("$"))
        parse(text)
      else
        throw ProtocolError(s"$text isn't a double.")

    private[this] def parse(text: String): Double = {
      var pos = 1
      var len = 0

      while (text.charAt(pos) != '\r') {
        len = len * 10 + text.charAt(pos) - '0'
        pos += 1
      }

      // skip to the first payload char
      pos += 2

      try text.substring(pos, pos + len).toDouble
      catch {
        case _: Throwable => throw ProtocolError(s"$text isn't a double.")
      }
    }
  }

  case object DurationMillisecondsOutput extends Output[Duration] {
    protected def tryDecode(text: String): Duration =
      if (text.startsWith(":"))
        parse(text)
      else
        throw ProtocolError(s"$text isn't a duration.")

    private[this] def parse(text: String): Duration = {
      val value = unsafeReadNumber(text)

      if (value == -2)
        throw ProtocolError("Key not found.")
      else if (value == -1)
        throw ProtocolError("Key has no expire.")
      else
        value.millis
    }
  }

  case object DurationSecondsOutput extends Output[Duration] {
    protected def tryDecode(text: String): Duration =
      if (text.startsWith(":"))
        parse(text)
      else
        throw ProtocolError(s"$text isn't a duration.")

    private[this] def parse(text: String): Duration = {
      val value = unsafeReadNumber(text)

      if (value == -2)
        throw ProtocolError("Key not found.")
      else if (value == -1)
        throw ProtocolError("Key has no expire.")
      else
        value.seconds
    }
  }

  case object LongOutput extends Output[Long] {
    protected def tryDecode(text: String): Long =
      if (text.startsWith(":"))
        unsafeReadNumber(text)
      else
        throw ProtocolError(s"$text isn't a number.")
  }

  final case class OptionalOutput[+A](output: Output[A]) extends Output[Option[A]] {
    protected def tryDecode(text: String): Option[A] =
      if (text.startsWith("$-1")) None else Some(output.tryDecode(text))
  }

  case object ScanOutput extends Output[(String, Chunk[String])] {
    protected def tryDecode(text: String): (String, Chunk[String]) =
      if (text.startsWith("*2\r\n"))
        parse(text)
      else
        throw ProtocolError(s"$text isn't scan output.")

    private[this] def parse(text: String): (String, Chunk[String]) = {
      var pos = 5
      var len = 0

      while (text.charAt(pos) != '\r') {
        len = len * 10 + text.charAt(pos) - '0'
        pos += 1
      }

      // skip to the first payload char
      pos += 2

      val cursor = text.substring(pos, pos + len)
      val items  = unsafeReadChunk(text, pos + len + 2)

      (cursor, items)
    }
  }

  case object StringOutput extends Output[String] {
    protected def tryDecode(text: String): String =
      if (text.startsWith("$"))
        parse(text)
      else
        throw ProtocolError(s"$text isn't a string.")

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
    protected def tryDecode(text: String): Unit =
      if (text == "+OK\r\n")
        ()
      else
        throw ProtocolError(s"$text isn't unit.")
  }

  case object SimpleStringOutput extends Output[String] {
    protected def tryDecode(text: String): Either[RedisError, String] =
      Either.cond(text.startsWith("+"), parse(text), ProtocolError(s"$text isn't a simple string."))

    private[this] def parse(text: String): String =
      text.substring(1, text.length - 2)
  }

  private[this] def unsafeReadChunk(text: String, start: Int): Chunk[String] = {
    var pos = start + 1
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

  private[this] def unsafeReadNumber(text: String): Long = {
    var pos = 1

    while (text.charAt(pos) != '\r')
      pos += 1

    text.substring(1, pos).toLong
  }
}
