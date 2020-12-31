package zio.redis

import java.nio.charset.StandardCharsets

import zio._
import zio.stream._

sealed trait RespValue extends Product with Serializable { self =>
  import RespValue._
  import RespValue.internal.{ Headers, NullString, CrLf }

  final def serialize: Chunk[Byte] =
    self match {
      case Null       => NullString
      case SimpleString(s) => Headers.SimpleString +: encode(s)
      case Error(s)        => Headers.Error +: encode(s)
      case Integer(i)      => Headers.Integer +: encode(i.toString)

      case BulkString(bytes) =>
        Headers.BulkString +: (encode(bytes.length.toString) ++ bytes ++ CrLf)

      case Array(elements) =>
        val data = elements.foldLeft[Chunk[Byte]](Chunk.empty)(_ ++ _.serialize)
        Headers.Array +: (encode(elements.size.toString) ++ data)
    }

  private[this] def encode(s: String): Chunk[Byte] =
    Chunk.fromArray(s.getBytes(StandardCharsets.US_ASCII)) ++ CrLf
}

object RespValue {
  final case class SimpleString(value: String) extends RespValue

  final case class Error(value: String) extends RespValue

  final case class Integer(value: Long) extends RespValue

  final case class BulkString(value: Chunk[Byte]) extends RespValue {
    private[redis] def asString: String = decodeString(value)

    private[redis] def asLong: Long = internal.unsafeReadLong(asString, 0)
  }

  final case class Array(values: Chunk[RespValue]) extends RespValue

  case object Null extends RespValue

  object ArrayValues {
    def unapplySeq(v: RespValue): Option[Seq[RespValue]] =
      v match {
        case Array(values) => Some(values)
        case _             => None
      }
  }

  def array(values: RespValue*): Array = Array(Chunk.fromIterable(values))

  def bulkString(s: String): BulkString = BulkString(Chunk.fromArray(s.getBytes(StandardCharsets.UTF_8)))

  def decodeString(bytes: Chunk[Byte]): String = new String(bytes.toArray, StandardCharsets.UTF_8)

  private[redis] final val Cr: Byte = '\r'

  private[redis] final val Lf: Byte = '\n'

  private[redis] final val Deserializer: Transducer[RedisError.ProtocolError, Byte, RespValue] = {
    import internal.State

    val processLine =
      Transducer
        .fold[String, State](State.Start)(_.inProgress)(_ feed _)
        .mapM {
          case State.Done(value) => IO.succeedNow(value)
          case State.Failed      => IO.fail(RedisError.ProtocolError("Invalid data received."))
          case other             => IO.dieMessage(s"Deserialization bug, should not get $other")
        }

    Transducer.utf8Decode >>> Transducer.splitLines >>> processLine
  }

  private object internal {
    object Headers {
      final val SimpleString: Byte = '+'
      final val Error: Byte        = '-'
      final val Integer: Byte      = ':'
      final val BulkString: Byte   = '$'
      final val Array: Byte        = '*'
    }

    final val CrLf: Chunk[Byte]       = Chunk(Cr, Lf)
    final val NullArray: String       = "*-1"
    final val NullValue: String       = "$-1"
    final val NullString: Chunk[Byte] = Chunk.fromArray("$-1\r\n".getBytes(StandardCharsets.US_ASCII))

    sealed trait State { self =>
      import State._

      final def inProgress: Boolean =
        self match {
          case Done(_) | Failed => false
          case _                => true
        }

      final def feed(line: String): State =
        self match {
          case Start if line == NullValue || line == NullArray => Done(Null)

          case Start if line.nonEmpty =>
            line.head match {
              case Headers.SimpleString => Done(SimpleString(line.tail))
              case Headers.Error        => Done(Error(line.tail))
              case Headers.Integer      => Done(Integer(unsafeReadLong(line, 0)))
              case Headers.BulkString   => ExpectingBulk
              case Headers.Array =>
                val size = unsafeReadLong(line, 0).toInt

                if (size > 0)
                  CollectingArray(size, Chunk.empty, Start.feed)
                else
                  Done(Array(Chunk.empty))
            }

          case CollectingArray(rem, vals, next) =>
            next(line) match {
              case Done(v) if rem > 1 => CollectingArray(rem - 1, vals :+ v, Start.feed)
              case Done(v)            => Done(Array(vals :+ v))
              case state              => CollectingArray(rem, vals, state.feed)
            }

          case ExpectingBulk => Done(bulkString(line))
          case _             => Failed
        }
    }

    object State {
      case object Start                                                                         extends State
      case object ExpectingBulk                                                                 extends State
      case object Failed                                                                        extends State
      final case class CollectingArray(rem: Int, vals: Chunk[RespValue], next: String => State) extends State
      final case class Done(value: RespValue)                                                   extends State
    }

    def unsafeReadLong(text: String, startFrom: Int): Long = {
      var pos = startFrom
      var res = 0L
      var neg = false

      if (text.charAt(pos) == '-') {
        neg = true
        pos += 1
      }

      val len = text.length

      while (pos < len) {
        res = res * 10 + text.charAt(pos) - '0'
        pos += 1
      }

      if (neg) -res else res
    }
  }
}
