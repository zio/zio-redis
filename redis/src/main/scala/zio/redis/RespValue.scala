package zio.redis

import java.nio.charset.StandardCharsets

import zio.stream._
import zio._

sealed trait RespValue extends Any { self =>
  import RespValue._

  final def serialize: Chunk[Byte] =
    self match {
      case NullValue       => NullString
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
  final case class SimpleString(value: String) extends AnyVal with RespValue

  final case class Error(value: String) extends AnyVal with RespValue

  final case class Integer(value: Long) extends AnyVal with RespValue

  final case class BulkString(value: Chunk[Byte]) extends AnyVal with RespValue {
    def asString: String = decodeString(value)
  }

  final case class Array(values: Chunk[RespValue]) extends AnyVal with RespValue

  case object NullValue extends RespValue

  object ArrayValues {
    def unapplySeq(v: RespValue): Option[Seq[RespValue]] =
      v match {
        case Array(values) => Some(values)
        case _             => None
      }
  }

  /*
   * - take a byte chunk
   * - analyze header
   * - crunch the remaining => State(header, remaining)
   * - when terminal value is read materialize the state
   */
  final val Deserializer: Transducer[RedisError.ProtocolError, Chunk[Byte], RespValue] = {
    sealed trait State

    object State {
      case object InProgress  extends State
      final case class Done() extends State
    }

    Transducer.foldLeft[Chunk[Byte], State](State.InProgress) { (state, bytes) =>
      ???
    }
  }

  def array(values: RespValue*): Array = Array(Chunk.fromIterable(values))

  def bulkString(s: String): BulkString = BulkString(Chunk.fromArray(s.getBytes(StandardCharsets.UTF_8)))

  def decodeString(bytes: Chunk[Byte]): String = new String(bytes.toArray, StandardCharsets.UTF_8)

  private object Headers {
    final val SimpleString: Byte = '+'
    final val Error: Byte        = '-'
    final val Integer: Byte      = ':'
    final val BulkString: Byte   = '$'
    final val Array: Byte        = '*'
  }

  private[redis] final val Cr: Byte = '\r'

  private[redis] final val Lf: Byte = '\n'

  private final val CrLf = Chunk(Cr, Lf)

  private final val NullString = Chunk.fromArray("$-1\r\n".getBytes(StandardCharsets.US_ASCII))
}
