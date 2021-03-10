package zio.redis

import zio.Chunk
import zio.duration._

sealed trait Output[+A] {
  self =>

  private[redis] final def unsafeDecode(respValue: RespValue): A =
    respValue match {
      case RespValue.Error(msg) if msg.startsWith("ERR") =>
        throw RedisError.ProtocolError(msg.drop(3).trim)
      case RespValue.Error(msg) if msg.startsWith("WRONGTYPE") =>
        throw RedisError.WrongType(msg.drop(9).trim)
      case RespValue.Error(msg) if msg.startsWith("BUSYGROUP") =>
        throw RedisError.BusyGroup(msg.drop(9).trim)
      case RespValue.Error(msg) if msg.startsWith("NOGROUP") =>
        throw RedisError.NoGroup(msg.drop(7).trim)
      case RespValue.Error(msg) if msg.startsWith("NOSCRIPT") =>
        throw RedisError.NoScript(msg.drop(8).trim)
      case RespValue.Error(msg) if msg.startsWith("NOTBUSY") =>
        throw RedisError.NotBusy(msg.drop(7).trim)
      case RespValue.Error(msg) =>
        throw RedisError.ProtocolError(msg.trim)
      case success =>
        tryDecode(success)
    }

  protected def tryDecode(respValue: RespValue): A

  final def map[B](f: A => B): Output[B] =
    new Output[B] {
      protected def tryDecode(respValue: RespValue): B = f(self.tryDecode(respValue))
    }
}

object Output {

  import RedisError._

  case object RespValueOutput extends Output[RespValue] {
    protected def tryDecode(respValue: RespValue): RespValue = respValue
  }

  case object BoolOutput extends Output[Boolean] {
    protected def tryDecode(respValue: RespValue): Boolean =
      respValue match {
        case RespValue.Integer(0) => false
        case RespValue.Integer(1) => true
        case other                => throw ProtocolError(s"$other isn't a boolean")
      }
  }

  final case class ChunkOutput[R](output: Output[R]) extends Output[Chunk[R]] {
    protected def tryDecode(respValue: RespValue): Chunk[R] =
      respValue match {
        case RespValue.Array(values) => values.map(output.unsafeDecode)
        case other                   => throw ProtocolError(s"$other isn't an array")
      }
  }

  case object DoubleOutput extends Output[Double] {
    protected def tryDecode(respValue: RespValue): Double =
      respValue match {
        case RespValue.BulkString(bytes) => decodeDouble(bytes)
        case other                       => throw ProtocolError(s"$other isn't a double.")
      }
  }

  private object DurationOutput extends Output[Long] {
    protected def tryDecode(respValue: RespValue): Long =
      respValue match {
        case RespValue.Integer(-2L) => throw ProtocolError("Key not found.")
        case RespValue.Integer(-1L) => throw ProtocolError("Key has no expire.")
        case RespValue.Integer(n)   => n
        case other                  => throw ProtocolError(s"$other isn't a duration.")
      }
  }

  final val DurationMillisecondsOutput: Output[Duration] = DurationOutput.map(_.milliseconds)

  final val DurationSecondsOutput: Output[Duration] = DurationOutput.map(_.seconds)

  case object LongOutput extends Output[Long] {
    protected def tryDecode(respValue: RespValue): Long =
      respValue match {
        case RespValue.Integer(v) => v
        case other                => throw ProtocolError(s"$other isn't an integer")
      }
  }

  final case class OptionalOutput[+A](output: Output[A]) extends Output[Option[A]] {
    protected def tryDecode(respValue: RespValue): Option[A] =
      respValue match {
        case RespValue.NullBulkString | RespValue.NullArray => None
        case other                                          => Some(output.tryDecode(other))
      }
  }

  case object ScanOutput extends Output[(Long, Chunk[String])] {
    protected def tryDecode(respValue: RespValue): (Long, Chunk[String]) =
      respValue match {
        case RespValue.ArrayValues(cursor @ RespValue.BulkString(_), RespValue.Array(items)) =>
          val strings = items.map {
            case s @ RespValue.BulkString(_) => s.asString
            case other                       => s"$other is not a bulk string"
          }
          (cursor.asLong, strings)
        case other =>
          throw ProtocolError(s"$other isn't scan output")
      }
  }

  case object KeyElemOutput extends Output[Option[(String, String)]] {
    protected def tryDecode(respValue: RespValue): Option[(String, String)] =
      respValue match {
        case RespValue.NullArray =>
          None
        case RespValue.ArrayValues(a @ RespValue.BulkString(_), b @ RespValue.BulkString(_)) =>
          Some((a.asString, b.asString))
        case other => throw ProtocolError(s"$other isn't blPop output")
      }
  }

  case object StringOutput extends Output[String] {
    protected def tryDecode(respValue: RespValue): String =
      respValue match {
        case RespValue.SimpleString(s) => s
        case other                     => throw ProtocolError(s"$other isn't a simple string")
      }
  }

  case object MultiStringOutput extends Output[String] {
    protected def tryDecode(respValue: RespValue): String =
      respValue match {
        case s @ RespValue.BulkString(_) => s.asString
        case other                       => throw ProtocolError(s"$other isn't a bulk string")
      }
  }

  case object BulkStringOutput extends Output[Chunk[Byte]] {
    protected def tryDecode(respValue: RespValue): Chunk[Byte] =
      respValue match {
        case RespValue.BulkString(value) => value
        case other                       => throw ProtocolError(s"$other isn't a bulk string")
      }
  }

  case object SingleOrMultiStringOutput extends Output[String] {
    protected def tryDecode(respValue: RespValue): String =
      respValue match {
        case RespValue.SimpleString(s)   => s
        case s @ RespValue.BulkString(_) => s.asString
        case other                       => throw ProtocolError(s"$other isn't a bulk string")
      }
  }

  case object MultiStringChunkOutput extends Output[Chunk[String]] {
    protected def tryDecode(respValue: RespValue): Chunk[String] =
      respValue match {
        case RespValue.NullBulkString =>
          Chunk.empty
        case s @ RespValue.BulkString(_) =>
          Chunk.single(s.asString)
        case RespValue.Array(elements) =>
          elements.map {
            case s @ RespValue.BulkString(_) => s.asString
            case other                       => throw ProtocolError(s"$other isn't a bulk string")
          }
        case other => throw ProtocolError(s"$other isn't a string nor an array")
      }
  }

  case object ChunkOptionalMultiStringOutput extends Output[Chunk[Option[String]]] {
    protected def tryDecode(respValue: RespValue): Chunk[Option[String]] =
      respValue match {
        case RespValue.NullArray => Chunk.empty
        case RespValue.Array(elements) =>
          elements.map {
            case RespValue.NullBulkString    => None
            case s @ RespValue.BulkString(_) => Some(s.asString)
            case other                       => throw ProtocolError(s"$other isn't null or a bulk string")
          }
        case other => throw ProtocolError(s"$other isn't an array")
      }
  }

  case object ChunkOptionalLongOutput extends Output[Chunk[Option[Long]]] {
    protected def tryDecode(respValue: RespValue): Chunk[Option[Long]] =
      respValue match {
        case RespValue.Array(elements) =>
          elements.map {
            case RespValue.NullBulkString   => None
            case RespValue.Integer(element) => Some(element)
            case other                      => throw ProtocolError(s"$other isn't an integer")
          }
        case other => throw ProtocolError(s"$other isn't an array")
      }
  }

  case object TypeOutput extends Output[RedisType] {
    protected def tryDecode(respValue: RespValue): RedisType =
      respValue match {
        case RespValue.SimpleString("string") => RedisType.String
        case RespValue.SimpleString("list")   => RedisType.List
        case RespValue.SimpleString("set")    => RedisType.Set
        case RespValue.SimpleString("zset")   => RedisType.SortedSet
        case RespValue.SimpleString("hash")   => RedisType.Hash
        case RespValue.SimpleString("stream") => RedisType.Stream
        case other                            => throw ProtocolError(s"$other isn't redis type.")
      }
  }

  case object UnitOutput extends Output[Unit] {
    protected def tryDecode(respValue: RespValue): Unit =
      respValue match {
        case RespValue.SimpleString("OK") => ()
        case other                        => throw ProtocolError(s"$other isn't unit.")
      }
  }

  case object GeoOutput extends Output[Chunk[Option[LongLat]]] {
    protected def tryDecode(respValue: RespValue): Chunk[Option[LongLat]] =
      respValue match {
        case RespValue.NullArray =>
          Chunk.empty
        case RespValue.Array(elements) =>
          elements.map {
            case RespValue.NullArray => None
            case RespValue.ArrayValues(RespValue.BulkString(long), RespValue.BulkString(lat)) =>
              Some(LongLat(decodeDouble(long), decodeDouble(lat)))
            case other =>
              throw ProtocolError(s"$other was not a longitude,latitude pair")
          }
        case other =>
          throw ProtocolError(s"$other isn't geo output")
      }
  }

  case object GeoRadiusOutput extends Output[Chunk[GeoView]] {
    protected def tryDecode(respValue: RespValue): Chunk[GeoView] =
      respValue match {
        case RespValue.Array(elements) =>
          elements.map {
            case s @ RespValue.BulkString(_) =>
              GeoView(s.asString, None, None, None)
            case RespValue.ArrayValues(name @ RespValue.BulkString(_), infos @ _*) =>
              val distance = infos.collectFirst { case RespValue.BulkString(bytes) => decodeDouble(bytes) }

              val hash = infos.collectFirst { case RespValue.Integer(i) => i }

              val position = infos.collectFirst {
                case RespValue.ArrayValues(RespValue.BulkString(long), RespValue.BulkString(lat)) =>
                  LongLat(decodeDouble(long), decodeDouble(lat))
              }

              GeoView(name.asString, distance, hash, position)

            case other => throw ProtocolError(s"$other is not a geo radious output")
          }

        case other => throw ProtocolError(s"$other is not an array")
      }
  }

  final case class KeyValueOutput[K, V](outK: Output[K], outV: Output[V]) extends Output[Map[K, V]] {
    protected def tryDecode(respValue: RespValue): Map[K, V] =
      respValue match {
        case RespValue.Array(elements) if elements.length % 2 == 0 =>
          val output = collection.mutable.Map.empty[K, V]
          val len    = elements.length
          var pos    = 0

          while (pos < len) {
            val key   = outK.unsafeDecode(elements(pos))
            val value = outV.unsafeDecode(elements(pos + 1))
            output += key -> value
            pos += 2
          }

          output.toMap
        case array @ RespValue.Array(_) =>
          throw ProtocolError(s"$array doesn't have an even number of elements")
        case other =>
          throw ProtocolError(s"$other isn't an array")
      }
  }

  case object StreamOutput extends Output[Map[String, Map[String, String]]] {
    protected def tryDecode(respValue: RespValue): Map[String, Map[String, String]] =
      respValue match {
        case RespValue.NullArray => Map.empty[String, Map[String, String]]
        case RespValue.Array(entities) =>
          val output = collection.mutable.Map.empty[String, Map[String, String]]
          entities.foreach {
            case RespValue.Array(Seq(id @ RespValue.BulkString(_), value)) =>
              output += (id.asString -> KeyValueOutput(MultiStringOutput, MultiStringOutput).unsafeDecode(value))
            case other =>
              throw ProtocolError(s"$other isn't a valid array")
          }

          output.toMap
        case other => throw ProtocolError(s"$other isn't an array")
      }
  }

  case object XPendingOutput extends Output[PendingInfo] {
    protected def tryDecode(respValue: RespValue): PendingInfo =
      respValue match {
        case RespValue.Array(Seq(RespValue.Integer(total), f, l, ps)) =>
          val first = OptionalOutput(MultiStringOutput).unsafeDecode(f)
          val last  = OptionalOutput(MultiStringOutput).unsafeDecode(l)

          val pairs = ps match {
            case RespValue.NullArray    => Chunk.empty
            case RespValue.Array(value) => value
            case other                  => throw ProtocolError(s"$other isn't an array")
          }

          val consumers = collection.mutable.Map.empty[String, Long]

          pairs.foreach {
            case RespValue.Array(Seq(consumer @ RespValue.BulkString(_), total @ RespValue.BulkString(_))) =>
              consumers += (consumer.asString -> total.asLong)
            case _ =>
              throw ProtocolError(s"Consumers doesn't have 2 elements")
          }

          PendingInfo(total, first, last, consumers.toMap)

        case array @ RespValue.Array(_) =>
          throw ProtocolError(s"$array doesn't have valid format")

        case other =>
          throw ProtocolError(s"$other isn't an array")
      }
  }

  case object PendingMessagesOutput extends Output[Chunk[PendingMessage]] {
    protected def tryDecode(respValue: RespValue): Chunk[PendingMessage] =
      respValue match {
        case RespValue.Array(messages) =>
          messages.collect {
            case RespValue.Array(
                  Seq(
                    id @ RespValue.BulkString(_),
                    owner @ RespValue.BulkString(_),
                    RespValue.Integer(lastDelivered),
                    RespValue.Integer(counter)
                  )
                ) =>
              PendingMessage(id.asString, owner.asString, lastDelivered.millis, counter)
            case other =>
              throw ProtocolError(s"$other isn't an array with four elements")
          }

        case other =>
          throw ProtocolError(s"$other isn't an array")
      }
  }

  case object XReadOutput extends Output[Map[String, Map[String, Map[String, String]]]] {
    protected def tryDecode(respValue: RespValue): Map[String, Map[String, Map[String, String]]] =
      respValue match {
        case RespValue.NullArray =>
          Map.empty[String, Map[String, Map[String, String]]]
        case RespValue.Array(streams) =>
          val output = collection.mutable.Map.empty[String, Map[String, Map[String, String]]]
          streams.foreach {
            case RespValue.Array(Seq(id @ RespValue.BulkString(_), value)) =>
              output += (id.asString -> StreamOutput.unsafeDecode(value))
            case other =>
              throw ProtocolError(s"$other isn't an array with two elements")
          }

          output.toMap
        case other =>
          throw ProtocolError(s"$other isn't an array")
      }
  }

  case object SetOutput extends Output[Boolean] {
    protected def tryDecode(respValue: RespValue): Boolean =
      respValue match {
        case RespValue.NullBulkString  => false
        case RespValue.SimpleString(_) => true
        case other                     => throw ProtocolError(s"$other isn't a valid set response")
      }
  }

  private def decodeDouble(bytes: Chunk[Byte]): Double = {
    val text = RespValue.decode(bytes)
    try text.toDouble
    catch {
      case _: NumberFormatException => throw ProtocolError(s"'$text' isn't a double.")
    }
  }
}
