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

    private[this] def parse(text: String): Chunk[LongLat] = {
      val parts = text.split("\r\n")
      unsafeReadCoords(parts)
    }
  }

  case object GeoRadiusOutput extends Output[Chunk[GeoView]] {
    override protected def tryDecode(text: String) =
      if (text.startsWith("*"))
        parse(text)
      else
        throw ProtocolError(s"$text isn't scan output.")

    private[this] def parse(text: String): Chunk[GeoView] = {
      val parts  = text.split("\r\n")
      val len    = parts(0).substring(1).toInt
      val output = Array.ofDim[GeoView](len)
      var idx    = 0
      var pos    = 0

      if (parts(1).startsWith("*")) {
        pos += 3
        val coords       = unsafeReadCoords(parts)
        val containsHash = containsGeoHash(parts)
        if (coords.isEmpty && !containsHash)
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
            val longLat          = if (coords.isEmpty) None else Some(coords(idx))
            val containsDistance = if (parts(pos + 1).startsWith(":")) false else true
            if (!containsDistance) {
              pos += 1
              val hash = parts(pos).substring(1).toLong
              output(idx) = GeoView(member, None, Some(hash), longLat)
            } else {
              pos += 2
              val distance = parts(pos).toDouble
              pos += 1
              val hash     = parts(pos).substring(1).toLong
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
            val containsDistance = if (parts(pos + 2).charAt(0).isDigit) true else false
            if (containsDistance) {
              pos += 2
              val distance = parts(pos).toDouble
              output(idx) = GeoView(member, Some(distance), None, Some(coords(idx)))
            } else output(idx) = GeoView(member, None, None, Some(coords(idx)))
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

  private[this] def unsafeReadCoords(parts: Array[String]): Chunk[LongLat] = {
    var pos = 1
    var idx = 0
    val len = coordsCount(parts)

    if (len == 0) Chunk.empty
    else {
      val output = Array.ofDim[LongLat](len)
      while (pos < parts.length) {
        if (parts(pos) == "*2" && parts(pos + 1).startsWith("$20")) {
          pos += 2
          val longitude = parts(pos).toDouble
          pos += 2
          val latitude  = parts(pos).toDouble
          output(idx) = LongLat(longitude, latitude)
          idx += 1
        }
        pos += 1
      }

      Chunk.fromArray(output)
    }
  }

  private[this] def coordsCount(parts: Array[String]): Int = {
    var cnt = 0
    var pos = 1

    while (pos < parts.length) {
      if (parts(pos) == "*2" && parts(pos + 1).startsWith("$20")) cnt += 1
      pos += 1
    }

    cnt
  }

  private[this] def containsGeoHash(parts: Array[String]): Boolean = {
    var pos    = 1
    var isHash = false

    while (pos < parts.length && !isHash) {
      if (parts(pos).startsWith(":")) isHash = true
      pos += 1
    }

    isHash
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
