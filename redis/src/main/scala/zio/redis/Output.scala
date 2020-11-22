package zio.redis

import zio.Chunk
import zio.duration._

sealed trait Output[+A] {

  self =>

  private[redis] final def unsafeDecode(respValue: RespValue): A =
    respValue match {
      case RespValue.Error(msg) if msg.startsWith("ERR")       =>
        throw RedisError.ProtocolError(msg.drop(3).trim())
      case RespValue.Error(msg) if msg.startsWith("WRONGTYPE") =>
        throw RedisError.WrongType(msg.drop(9).trim())
      case RespValue.Error(msg) if msg.startsWith(("MOVED"))   =>
        throw RedisError.Moved(msg.trim())
      case RespValue.Error(msg)                                =>
        throw RedisError.ProtocolError(msg.trim)
      case success                                             =>
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

  private def decodeDouble(bytes: Chunk[Byte]): Double = {
    val text = RespValue.decodeString(bytes)
    try text.toDouble
    catch {
      case _: NumberFormatException => throw ProtocolError(s"'$text' isn't a double.")
    }
  }

  case object BoolOutput extends Output[Boolean] {

    override protected def tryDecode(respValue: RespValue): Boolean =
      respValue match {
        case RespValue.Integer(0) => false
        case RespValue.Integer(1) => true
        case other                => throw ProtocolError(s"$other isn't a boolean")
      }

  }

  case object ChunkOutput extends Output[Chunk[String]] {

    override protected def tryDecode(respValue: RespValue): Chunk[String] =
      respValue match {
        case RespValue.Array(values) =>
          values.map {
            case s @ RespValue.BulkString(_) => s.asString
            case other                       => throw ProtocolError(s"$other is not a bulk string")
          }
        case other                   =>
          throw ProtocolError(s"$other isn't an array")
      }

  }

  case object DoubleOutput extends Output[Double] {

    override protected def tryDecode(respValue: RespValue): Double =
      respValue match {
        case RespValue.BulkString(bytes) => decodeDouble(bytes)
        case other                       => throw ProtocolError(s"$other isn't a double.")
      }

  }

  private object DurationOutput extends Output[Long] {
    override protected def tryDecode(respValue: RespValue): Long =
      respValue match {
        case RespValue.Integer(-2L) => throw ProtocolError("Key not found.")
        case RespValue.Integer(-1L) => throw ProtocolError("Key has no expire.")
        case RespValue.Integer(n)   => n
        case other                  => throw ProtocolError(s"$other isn't a duration.")
      }
  }

  val DurationMillisecondsOutput: Output[Duration] = DurationOutput.map(_.milliseconds)

  val DurationSecondsOutput: Output[Duration] = DurationOutput.map(_.seconds)

  case object LongOutput extends Output[Long] {

    override protected def tryDecode(respValue: RespValue): Long =
      respValue match {
        case RespValue.Integer(v) => v
        case other                => throw ProtocolError(s"$other isn't an integer")
      }

  }

  final case class OptionalOutput[+A](output: Output[A]) extends Output[Option[A]] {

    override protected def tryDecode(respValue: RespValue): Option[A] =
      respValue match {
        case RespValue.NullValue => None
        case other               => Some(output.tryDecode(other))
      }

  }

  case object ScanOutput extends Output[(String, Chunk[String])] {

    override protected def tryDecode(respValue: RespValue): (String, Chunk[String]) =
      respValue match {
        case RespValue.ArrayValues(cursor @ RespValue.BulkString(_), RespValue.Array(items)) =>
          val strings = items.map {
            case s @ RespValue.BulkString(_) => s.asString
            case other                       => s"$other is not a bulk string"
          }
          (cursor.asString, strings)
        case other                                                                           =>
          throw ProtocolError(s"$other isn't scan output")
      }

  }

  case object KeyElemOutput extends Output[Option[(String, String)]] {

    override protected def tryDecode(respValue: RespValue): Option[(String, String)] =
      respValue match {
        case RespValue.NullValue                                                             =>
          None
        case RespValue.ArrayValues(a @ RespValue.BulkString(_), b @ RespValue.BulkString(_)) =>
          Some((a.asString, b.asString))
        case other                                                                           => throw ProtocolError(s"$other isn't blPop output")
      }

  }

  case object StringOutput extends Output[String] {

    override protected def tryDecode(respValue: RespValue): String =
      respValue match {
        case RespValue.SimpleString(s) => s
        case other                     => throw ProtocolError(s"$other isn't a simple string")
      }

  }

  case object MultiStringOutput extends Output[String] {

    override protected def tryDecode(respValue: RespValue): String =
      respValue match {
        case s @ RespValue.BulkString(_) => s.asString
        case other                       => throw ProtocolError(s"$other isn't a bulk string")
      }

  }

  case object BulkStringOutput extends Output[Chunk[Byte]] {
    override protected def tryDecode(respValue: RespValue): Chunk[Byte] =
      respValue match {
        case RespValue.BulkString(value) => value
        case other                       => throw ProtocolError(s"$other isn't a bulk string")
      }
  }

  case object MultiStringChunkOutput extends Output[Chunk[String]] {

    override protected def tryDecode(respValue: RespValue): Chunk[String] =
      respValue match {
        case RespValue.NullValue         =>
          Chunk.empty
        case s @ RespValue.BulkString(_) =>
          Chunk.single(s.asString)
        case RespValue.Array(elements)   =>
          elements.map {
            case s @ RespValue.BulkString(_) => s.asString
            case other                       => throw ProtocolError(s"$other isn't a bulk string")
          }
        case other                       => throw ProtocolError(s"$other isn't a string nor an array")
      }

  }

  case object ChunkOptionalMultiStringOutput extends Output[Chunk[Option[String]]] {
    protected def tryDecode(respValue: RespValue): Chunk[Option[String]] =
      respValue match {
        case RespValue.NullValue       => Chunk.empty
        case RespValue.Array(elements) =>
          elements.map {
            case s @ RespValue.BulkString(_) => Some(s.asString)
            case RespValue.NullValue         => None
            case other                       => throw ProtocolError(s"$other isn't null or a bulk string")
          }
        case other                     => throw ProtocolError(s"$other isn't an array")
      }
  }

  case object ChunkOptionalLongOutput extends Output[Chunk[Option[Long]]] {
    protected def tryDecode(respValue: RespValue): Chunk[Option[Long]] =
      respValue match {
        case RespValue.Array(elements) =>
          elements.map {
            case RespValue.Integer(element) => Some(element)
            case RespValue.NullValue        => None
            case other                      => throw ProtocolError(s"$other isn't an integer")
          }
        case other                     => throw ProtocolError(s"$other isn't an array")
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

  case object GeoOutput extends Output[Chunk[LongLat]] {
    protected def tryDecode(respValue: RespValue): Chunk[LongLat] =
      respValue match {
        case RespValue.Array(elements) =>
          elements.map {
            case RespValue.ArrayValues(RespValue.BulkString(long), RespValue.BulkString(lat)) =>
              LongLat(decodeDouble(long), decodeDouble(lat))
            case other                                                                        =>
              throw ProtocolError(s"$other was not a longitude,latitude pair")
          }
        case RespValue.NullValue       =>
          Chunk.empty
        case other                     =>
          throw ProtocolError(s"$other isn't geo output")
      }
  }

  case object GeoRadiusOutput extends Output[Chunk[GeoView]] {
    protected def tryDecode(respValue: RespValue): Chunk[GeoView] =
      respValue match {
        case RespValue.Array(elements) =>
          elements.map {
            case s @ RespValue.BulkString(_)                                       =>
              GeoView(s.asString, None, None, None)
            case RespValue.ArrayValues(name @ RespValue.BulkString(_), infos @ _*) =>
              val distance = infos.collectFirst {
                case RespValue.BulkString(bytes) => decodeDouble(bytes)
              }
              val hash     = infos.collectFirst {
                case RespValue.Integer(i) => i
              }
              val position = infos.collectFirst {
                case RespValue.ArrayValues(RespValue.BulkString(long), RespValue.BulkString(lat)) =>
                  LongLat(decodeDouble(long), decodeDouble(lat))
              }
              GeoView(name.asString, distance, hash, position)
            case other                                                             =>
              throw ProtocolError(s"$other is not a geo radious output")
          }
        case other                     => throw ProtocolError(s"$other is not an array")
      }
  }

  case object KeyValueOutput extends Output[Map[String, String]] {
    protected def tryDecode(respValue: RespValue): Map[String, String] =
      respValue match {
        case RespValue.Array(elements) if elements.length % 2 == 0 =>
          val output = collection.mutable.Map.empty[String, String]
          val len    = elements.length
          var pos    = 0
          while (pos < len) {
            (elements(pos), elements(pos + 1)) match {
              case (key @ RespValue.BulkString(_), value @ RespValue.BulkString(_)) =>
                output += key.asString -> value.asString
              case _                                                                =>
            }
            pos += 2
          }
          output.toMap
        case array @ RespValue.Array(_) =>
          throw ProtocolError(s"$array doesn't have an even number of elements")
        case other =>
          throw ProtocolError(s"$other isn't an array")
      }
  }

  case object ClusterNodesOutput extends Output[Chunk[ClusterNode]] {
    protected def tryDecode(respValue: RespValue): Chunk[ClusterNode] =
      respValue match {
        case bs @ RespValue.BulkString(_) =>
          Chunk.fromArray(bs.asString.split('\n')).map { value =>
            val fields = value.split(' ')
            if (fields.length < 8) throw ProtocolError(s"$value has less than 8 fields")
            else {
              val Array(id, address, flagsF, masterF, pingSentF, pongRecvF, configEpochF, linkStateF) =
                fields.slice(0, 8)
              // [ip]:port@cport
              val (ip, port, cport) = {
                val colIdx = address.indexOf(':')
                val atIdx  = address.indexOf('@')
                if (colIdx < 0 || atIdx < 0 || colIdx >= atIdx)
                  throw ProtocolError(s"$address has an unexpected format")
                else {
                  val ip    = address.substring(0, colIdx) // FIXME maybe parse this ip
                  val port  =
                    try address.substring(colIdx + 1, atIdx).toInt
                    catch {
                      case _: NumberFormatException =>
                        throw ProtocolError(s"$address has an invalid port number")
                    }
                  val cport =
                    try address.substring(atIdx + 1, address.length()).toInt
                    catch {
                      case _: NumberFormatException =>
                        throw ProtocolError(s"$address has an invalid cport number")
                    }
                  (ip, port, cport)
                }
              }

              val flags       = Chunk.fromArray(flagsF.split(','))
              val master      = if (masterF == "-") None else Some(masterF)
              val pingSent    =
                try pingSentF.toLong
                catch {
                  case _: NumberFormatException =>
                    throw ProtocolError(s"ping-sent: $pingSentF is not a valid number")
                }
              val pongRecv    =
                try pongRecvF.toLong
                catch {
                  case _: NumberFormatException =>
                    throw ProtocolError(s"pong-recv: $pongRecvF is not a valid number")
                }
              val configEpoch =
                try configEpochF.toInt
                catch {
                  case _: NumberFormatException =>
                    throw ProtocolError(s"config-epoch: $configEpochF is not a valid number")
                }
              val linkState   = linkStateF match {
                case "connected"    => LinkState.Connected
                case "disconnected" => LinkState.Disconnected
                case other          => throw ProtocolError(s"link-state: $other was unexpected")
              }
              val slots       = Chunk.fromArray(fields.drop(8)).map(Slot.fromStringUnsafe)
              ClusterNode(
                id = id,
                ip = ip,
                port = port,
                cport = cport,
                flags = flags,
                master = master,
                pingSent = pingSent,
                pongRecv = pongRecv,
                configEpoch = configEpoch,
                linkState = linkState,
                slots = slots
              )

            }
          }
        case other                        =>
          throw ProtocolError(s"$other is not a string")
      }
  }

  case object ClusterInfoOutput extends Output[ClusterInfo] {
    protected def tryDecode(respValue: RespValue): ClusterInfo = {
      val kv            = KeyValueOutput.unsafeDecode(respValue)
      val state         =
        kv.getOrElse("cluster_state", throw ProtocolError("missing cluster_state in cluster info")) match {
          case "ok"   => ClusterState.Ok
          case "fail" => ClusterState.Fail
          case other  => throw ProtocolError(s"$other is not a valid value for cluster_state")
        }
      val slotsAssigned =
        try kv
          .getOrElse(
            "cluster_slots_assigned",
            throw ProtocolError("missing cluster_slots_assigned in cluster info")
          )
          .toInt
        catch {
          case _: NumberFormatException => throw ProtocolError("cluster_slots_assigned is not a number")
        }
      val slotsOk       =
        try kv.getOrElse("cluster_slots_ok", throw ProtocolError("missing cluster_slots_ok in cluster info")).toInt
        catch {
          case _: NumberFormatException => throw ProtocolError("cluster_slots_ok is not a number")
        }
      val slotsPfail    =
        try kv
          .getOrElse("cluster_slots_pfail", throw ProtocolError("missing cluster_slots_pfail in cluster info"))
          .toInt
        catch {
          case _: NumberFormatException => throw ProtocolError("cluster_slots_pfail is not a number")
        }

      val slotsFail             =
        try kv
          .getOrElse("cluster_slots_fail", throw ProtocolError("missing cluster_slots_fail in cluster info"))
          .toInt
        catch {
          case _: NumberFormatException => throw ProtocolError("cluster_slots_fail is not a number")
        }
      val knownNodes            =
        try kv
          .getOrElse("cluster_known_nodes", throw ProtocolError("missing cluster_known_nodes in cluster info"))
          .toInt
        catch {
          case _: NumberFormatException => throw ProtocolError("cluster_known_nodes is not a number")
        }
      val size                  =
        try kv.getOrElse("cluster_size", throw ProtocolError("missing cluster_size in cluster info")).toInt
        catch {
          case _: NumberFormatException => throw ProtocolError("cluster_size is not a number")
        }
      val currentEpoch          =
        try kv
          .getOrElse("cluster_current_epoch", throw ProtocolError("missing cluster_current_epoch in cluster info"))
          .toInt
        catch {
          case _: NumberFormatException => throw ProtocolError("cluster_current_epoch is not a number")
        }
      val myEpoch               =
        try kv.getOrElse("cluster_my_epoch", throw ProtocolError("missing cluster_my_epoch in cluster info")).toInt
        catch {
          case _: NumberFormatException => throw ProtocolError("cluster_my_epoch is not a number")
        }
      val statsMessagesSent     =
        try kv
          .getOrElse(
            "cluster_stats_messages_sent",
            throw ProtocolError("missing cluster_stats_messages_sent in cluster info")
          )
          .toInt
        catch {
          case _: NumberFormatException => throw ProtocolError("cluster_stats_messages_sent is not a number")
        }
      val statsMessagesReceived =
        try kv
          .getOrElse(
            "cluster_stats_messages_received",
            throw ProtocolError("missing cluster_stats_messages_received in cluster info")
          )
          .toInt
        catch {
          case _: NumberFormatException => throw ProtocolError("cluster_stats_messages_received is not a number")
        }
      ClusterInfo(
        state = state,
        slotsAssigned = slotsAssigned,
        slotsOk = slotsOk,
        slotsPfail = slotsPfail,
        slotsFail = slotsFail,
        knownNodes = knownNodes,
        size = size,
        currentEpoch = currentEpoch,
        myEpoch = myEpoch,
        statsMessagesSent = statsMessagesSent,
        statsMessagesReceived = statsMessagesReceived
      )
    }

  }
}
