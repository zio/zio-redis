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

  case object KeyElemOutput extends Output[Option[(String, String)]] {
    protected def tryDecode(text: String): Option[(String, String)] =
      if (text.startsWith("*-1\r\n"))
        None
      else if (text.startsWith("*2\r\n"))
        Some(parse(text))
      else
        throw ProtocolError(s"$text isn't blPop output.")

    private[this] def parse(text: String): (String, String) = {
      val items = unsafeReadChunk(text, 0)
      (items(0), items(1))
    }
  }

  case object StringOutput extends Output[String] {
    protected def tryDecode(text: String): String =
      if (text.startsWith("$"))
        parse(text)
      else
        throw ProtocolError(s"$text isn't a simple string.")

    private[this] def parse(text: String): String =
      text.substring(2, text.length).filter(_ >= ' ')
  }

  case object MultiStringOutput extends Output[String] {
    protected def tryDecode(text: String): String =
      if (text.startsWith("$"))
        unsafeReadMultiString(text)
      else
        throw ProtocolError(s"$text isn't a string.")
  }

  case object MultiStringChunkOutput extends Output[Chunk[String]] {
    protected def tryDecode(text: String): Chunk[String] =
      if (text.startsWith("$-1"))
        Chunk.empty
      else if (text.startsWith("$"))
        Chunk(unsafeReadMultiString(text))
      else if (text.startsWith("*"))
        unsafeReadChunk(text, 0)
      else
        throw ProtocolError(s"$text isn't a string nor an array.")
  }

  case object ChunkOptionalMultiStringOutput extends Output[Chunk[Option[String]]] {
    protected def tryDecode(text: String): Chunk[Option[String]] =
      if (text.startsWith("*-1\r\n"))
        Chunk.empty
      else if (text.startsWith("*"))
        unsafeReadChunkOptionalString(text, 0)
      else
        throw ProtocolError(s"$text isn't an array.")
  }

  case object ChunkOptionalLongOutput extends Output[Chunk[Option[Long]]] {
    protected def tryDecode(text: String): Chunk[Option[Long]] =
      if (text.startsWith("*"))
        unsafeReadChunkOptionalLong(text, 0)
      else
        throw ProtocolError("$text isn't an array")
  }

  case object TypeOutput extends Output[RedisType] {
    protected def tryDecode(text: String): RedisType =
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
    protected def tryDecode(text: String) =
      if (text.startsWith("*"))
        parse(text)
      else
        throw ProtocolError(s"$text isn't scan output.")

    private[this] def parse(text: String): Chunk[LongLat] =
      unsafeReadCoordinates(text)
  }

  case object GeoRadiusOutput extends Output[Chunk[GeoView]] {
    protected def tryDecode(text: String) =
      if (text.startsWith("*"))
        parse(text)
      else
        throw ProtocolError(s"$text isn't scan output.")

    private[this] def parse(text: String): Chunk[GeoView] = {
      val len = unsafeReadNumber(text).toInt

      if (containsInnerArray(text)) {
        val pos          = skipToNext(text, 0)
        var output       = Array.ofDim[GeoView](len)
        val coordinates  = unsafeReadCoordinates(text)
        val containsHash = containsGeoHash(text)

        output =
          if (containsHash)
            viewWithDetails(text, pos, len, coordinates)
          else if (coordinates.isEmpty)
            viewWithDistance(text, pos, len)
          else
            view(text, pos, len, coordinates)

        Chunk.fromArray(output)
      } else unsafeReadMembers(text, len)
    }

    private[this] def viewWithDistance(text: String, start: Int, len: Int): Array[GeoView] = {
      var idx    = 0
      var pos    = start
      val output = Array.ofDim[GeoView](len)

      while (idx < len) {
        val member = unsafeReadString(text, pos)

        pos = skipToNext(text, pos)

        output(idx) = GeoView(member, Some(unsafeReadString(text, pos).toDouble), None, None)
        idx += 1

        if (idx < len)
          pos = skipToNext(text, pos)
      }

      output
    }

    private[this] def viewWithDetails(
      text: String,
      start: Int,
      len: Int,
      coordinates: Chunk[LongLat]
    ): Array[GeoView] = {
      var idx    = 0
      var pos    = start
      val output = Array.ofDim[GeoView](len)

      while (idx < len) {
        val member = unsafeReadString(text, pos)

        while (text.charAt(pos) != '\n') pos += 1
        pos += 1

        val longLat = if (coordinates.isEmpty) None else Some(coordinates(idx))

        output(idx) = if (text.charAt(pos) == ':') {
          pos += 1
          GeoView(member, None, Some(unsafeReadString(text, pos).toLong), longLat)
        } else {
          while (text.charAt(pos) != '\n') pos += 1
          pos += 1

          val distance = unsafeReadString(text, pos).toDouble

          while (text.charAt(pos) != ':') pos += 1
          pos += 1

          GeoView(member, Some(distance), Some(unsafeReadString(text, pos).toLong), longLat)
        }

        idx += 1
        if (idx < len)
          while (!text.charAt(pos).isLetter) pos += 1
      }

      output
    }

    private[this] def view(
      text: String,
      start: Int,
      len: Int,
      coordinates: Chunk[LongLat]
    ): Array[GeoView] = {
      var idx    = 0
      var pos    = start
      val output = Array.ofDim[GeoView](len)

      while (idx < len) {
        val member = unsafeReadString(text, pos)

        while (text.charAt(pos) != '\n') pos += 1
        pos += 1

        output(idx) =
          if (text.charAt(pos) != '$')
            GeoView(member, None, None, Some(coordinates(idx)))
          else {
            while (text.charAt(pos) != '\n') pos += 1
            pos += 1
            GeoView(member, Some(unsafeReadString(text, pos).toDouble), None, Some(coordinates(idx)))
          }

        idx += 1
        if (idx < len)
          while (!text.charAt(pos).isLetter) pos += 1
      }

      output
    }

    private[this] def unsafeReadMembers(text: String, len: Int): Chunk[GeoView] = {
      var idx    = 0
      var pos    = 0
      val output = Array.ofDim[GeoView](len)

      while (idx < len) {
        if (text.charAt(pos) == '$') {
          while (text.charAt(pos) != '\n') pos += 1
          pos += 1

          output(idx) = GeoView(unsafeReadString(text, pos), None, None, None)
          idx += 1
        }
        pos += 1
      }

      Chunk.fromArray(output)
    }

    private[this] def unsafeReadString(text: String, start: Int): String = {
      var pos = start + 1

      while (text.charAt(pos) != '\r') pos += 1

      text.substring(start, pos)
    }

    private[this] def skipToNext(text: String, start: Int): Int = {
      var pos = start

      while (text.charAt(pos) != '$') pos += 1
      while (text.charAt(pos) != '\n') pos += 1
      pos += 1

      pos
    }
  }

  case object KeyValueOutput extends Output[Map[String, String]] {
    protected def tryDecode(text: String) =
      if (text.startsWith("*"))
        parse(text)
      else
        throw ProtocolError(s"$text isn't a string.")

    private[this] def parse(text: String): Map[String, String] = {
      val data   = unsafeReadChunk(text, 0)
      val output = collection.mutable.Map.empty[String, String]
      val len    = data.length
      var pos    = 0

      while (pos < len) {
        output += data(pos) -> data(pos + 1)
        pos += 2
      }

      output.toMap
    }
  }

  case object IncrementOutput extends Output[Double] {
    protected def tryDecode(text: String) =
      if (text.startsWith("$"))
        parse(text)
      else
        throw ProtocolError(s"$text isn't a string.")

    private[this] def parse(text: String): Double = {
      var pos = 1

      while (text.charAt(pos) != '\n') pos += 1
      pos += 1

      var end = pos + 1
      while (text.charAt(end) != '\r') end += 1

      text.substring(pos, end).toDouble
    }
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
          // skip to payload
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

    while (pos < len)
      if (!coordinatesArrayStarts(text, pos))
        pos += 1
      else {
        cnt += 1
        pos += 9
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
    text.startsWith("*2\r\n$", pos) && text.charAt(pos + 5).isDigit &&
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
    // skip outer array length
    var pos      = 1
    var innerLen = 0

    while (text.charAt(pos) != '$') {
      if (text.charAt(pos) == '*') innerLen += 1
      pos += 1
    }

    innerLen == 1
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

  private[this] def unsafeReadChunkOptionalString(text: String, start: Int): Chunk[Option[String]] = {
    var pos = start + 1
    var len = 0

    while (text.charAt(pos) != '\r') {
      len = len * 10 + text.charAt(pos) - '0'
      pos += 1
    }

    pos += 3

    if (len == 0) Chunk.empty
    else {
      val data = Array.ofDim[Option[String]](len)
      var idx  = 0

      while (idx < len) {
        var itemLen = 0

        // if first char is '-', then it's NULL value
        if (text.charAt(pos) == '-') {
          data(idx) = None
          itemLen = 2
        } else {
          while (text.charAt(pos) != '\r') {
            itemLen = itemLen * 10 + text.charAt(pos) - '0'
            pos += 1
          }

          // skip to the first payload char
          pos += 2

          data(idx) = Some(text.substring(pos, pos + itemLen))
        }

        pos += itemLen + 3
        idx += 1
      }

      Chunk.fromArray(data)
    }
  }

  private[this] def unsafeReadChunkOptionalLong(text: String, start: Int): Chunk[Option[Long]] = {
    var pos = start + 1
    var len = 0

    while (text.charAt(pos) != '\r') {
      len = len * 10 + text.charAt(pos) - '0'
      pos += 1
    }

    pos += 2

    if (len == 0) Chunk.empty
    else {
      val data = Array.ofDim[Option[Long]](len)
      var idx  = 0

      while (idx < len) {
        var itemLen = 0

        // if element is multistring, it is null value
        if (text.charAt(pos) == '$') {
          data(idx) = None
          itemLen = 3
        } else {
          // skip ':' char
          pos += 1

          var value = 0L
          while (text.charAt(pos) != '\r') {
            value = value * 10 + text.charAt(pos) - '0'
            pos += 1
          }

          data(idx) = Some(value)
        }

        pos += itemLen + 2
        idx += 1
      }

      Chunk.fromArray(data)
    }
  }

  private[this] def unsafeReadNumber(text: String): Long = {
    var pos  = 1
    var res  = 0L
    var sign = 1

    if (text.charAt(pos) == '-') {
      sign = -1
      pos += 1
    }

    while (text.charAt(pos) != '\r') {
      res = res * 10 + text.charAt(pos) - '0'
      pos += 1
    }

    sign * res
  }

  private[this] def unsafeReadMultiString(text: String): String = {
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
