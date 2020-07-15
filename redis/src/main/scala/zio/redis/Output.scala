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
      if (text.startsWith("+"))
        parse(text)
      else
        throw ProtocolError(s"$text isn't a simple string.")

    private[this] def parse(text: String): String =
      text.substring(1, text.length - 2)
  }

  case object MultiStringOutput extends Output[String] {
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

  case object TypeOutput extends Output[RedisType] {
    override protected def tryDecode(text: String): RedisType =
      text match {
        case "+string\r\n" => RedisType.String
        case "+list\r\n"   => RedisType.List
        case "+set\r\n"    => RedisType.Set
        case "+zset\r\n"   => RedisType.SortedSet
        case "+hash\r\n"   => RedisType.Hash
        case _             => throw ProtocolError(s"$text isn't redis type.")
      }
  }

  case object UnitOutput extends Output[Unit] {
    protected def tryDecode(text: String): Unit =
      if (text == "+OK\r\n")
        ()
      else
        throw ProtocolError(s"$text isn't unit.")
  }

  case object GeoOutput extends Output[Chunk[LongLat]] {
    override protected def tryDecode(text: String) =
      if (text.startsWith("*"))
        parse(text)
      else
        throw ProtocolError(s"$text isn't scan output.")

    private[this] def parse(text: String): Chunk[LongLat] =
      unsafeReadCoordinates(text)
  }

  case object GeoRadiusOutput extends Output[Chunk[GeoView]] {
    override protected def tryDecode(text: String) =
      if (text.startsWith("*"))
        parse(text)
      else
        throw ProtocolError(s"$text isn't scan output.")

    private[this] def parse(text: String): Chunk[GeoView] = {
      val parts  = text.split("\r\n")
      val len    = unsafeReadLength(text)
      val output = Array.ofDim[GeoView](len)
      var idx    = 0
      var pos    = 0

      if (containsInnerArray(text)) {
        pos += 3
        val coordinates  = unsafeReadCoordinates(text)
        val containsHash = containsGeoHash(text)
        if (coordinates.isEmpty && !containsHash)
          while (idx < len) {
            val member   = parts(pos)
            val distance = parts(pos + 2).toDouble
            output(idx) = GeoView(member, Some(distance), None, None)
            idx += 1
            pos += 5
          }
        else if (containsHash)
          while (idx < len) {
            val member           = parts(pos)
            val longLat          = if (coordinates.isEmpty) None else Some(coordinates(idx))
            val containsDistance = !parts(pos + 1).startsWith(":")
            if (!containsDistance) {
              pos += 1
              val hash = parts(pos).substring(1).toLong
              output(idx) = GeoView(member, None, Some(hash), longLat)
            } else {
              pos += 2
              val distance = parts(pos).toDouble
              pos += 1
              val hash = parts(pos).substring(1).toLong
              output(idx) = GeoView(member, Some(distance), Some(hash), longLat)
            }

            idx += 1
            if (longLat.isDefined)
              pos += 8
            else
              pos += 3
          }
        else
          while (idx < len) {
            val member           = parts(pos)
            val containsDistance = parts(pos + 2).charAt(0).isDigit
            if (containsDistance) {
              pos += 2
              val distance = parts(pos).toDouble
              output(idx) = GeoView(member, Some(distance), None, Some(coordinates(idx)))
            } else output(idx) = GeoView(member, None, None, Some(coordinates(idx)))
            idx += 1
            pos += 8
          }
        Chunk.fromArray(output)
      } else {
        pos += 2
        while (pos < parts.length) {
          val current = parts(pos)

          if (!current.startsWith("*") && !current.startsWith("$")) {
            output(idx) = GeoView(current, None, None, None)
            idx += 1
          }

          pos += 1
        }
        Chunk.fromArray(output)
      }
    }
  }

  private[this] def unsafeReadLength(text: String): Int = {
    var pos = 1

    while (text.charAt(pos) != '\r') pos += 1

    text.substring(1, pos).toInt
  }

  private[this] def unsafeReadCoordinates(text: String): Chunk[LongLat] = {
    var pos       = 1
    var idx       = 0
    val len       = text.length
    val outputLen = countCoordinates(text, len)

    if (outputLen == 0) Chunk.empty
    else {
      val output = Array.ofDim[LongLat](outputLen)
      while (pos < len) {
        if (coordinatesArrayStarts(text, pos)) {
          pos += 9
          output(idx) = unsafeReadLongLat(text, pos)
          idx += 1
        }
        pos += 1
      }

      Chunk.fromArray(output)
    }
  }

  private[this] def countCoordinates(text: String, len: Int): Int = {
    var cnt = 0
    var pos = 1

    while (pos < len) {
      if (!coordinatesArrayStarts(text, pos)) pos += 1
      else {
        cnt += 1
        pos += 9
      }
    }

    cnt
  }

  private[this] def unsafeReadLongLat(text: String, start: Int): LongLat = {
    // longitude start
    var pos = start

    while (text.charAt(pos) != '\r') pos += 1
    val long = text.substring(start, pos).toDouble

    // skip to latitude
    pos += 7

    val latStart = pos
    while (text.charAt(pos) != '\r') pos += 1
    val lat = text.substring(latStart, pos).toDouble

    LongLat(long, lat)
  }

  private[this] def coordinatesArrayStarts(text: String, pos: Int): Boolean =
    text.charAt(pos) == '*' && text.charAt(pos + 1) == '2' && text.charAt(pos + 2) == '\r' &&
      text.charAt(pos + 3) == '\n' && text.charAt(pos + 4) == '$' && text.charAt(pos + 5).isDigit &&
      text.charAt(pos + 6).isDigit && text.charAt(pos + 7) == '\r'

  private[this] def containsGeoHash(text: String): Boolean = {
    var pos    = 1
    var isHash = false
    val len    = text.length

    while (pos < len && !isHash) {
      isHash = text.charAt(pos) == ':'
      pos += 1
    }

    isHash
  }

  private[this] def containsInnerArray(text: String): Boolean = {
    // skip first length
    var pos     = 1
    var lengths = 0

    while (text.charAt(pos) != '$') {
      if (text.charAt(pos) == '*') lengths += 1
      pos += 1
    }

    lengths == 1
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

    while (text.charAt(pos) != '\r') pos += 1

    text.substring(1, pos).toLong
  }
}
