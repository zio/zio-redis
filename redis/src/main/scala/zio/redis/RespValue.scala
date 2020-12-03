package zio.redis

import java.nio.charset.StandardCharsets

import zio.stream.Sink
import zio.{ Chunk, IO }

sealed trait RespValue extends Any { self =>

  import RespValue._

  final def serialize: Chunk[Byte] =
    self match {
      case NullValue       => NullString
      case SimpleString(s) => Headers.SimpleString +: simpleString(s)
      case Error(s)        => Headers.Error +: simpleString(s)
      case Integer(i)      => Headers.Integer +: simpleString(i.toString)
      case BulkString(bytes) =>
        Headers.BulkString +: (simpleString(bytes.length.toString) ++ bytes ++ CrLf)
      case Array(elements) =>
        val data = elements.foldLeft[Chunk[Byte]](Chunk.empty)(_ ++ _.serialize)
        Headers.Array +: (simpleString(elements.size.toString) ++ data)
    }

  private[this] def simpleString(s: String): Chunk[Byte] =
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

  def array(values: RespValue*): Array = Array(Chunk.fromIterable(values))

  def bulkString(s: String): BulkString = BulkString(Chunk.fromArray(s.getBytes(StandardCharsets.UTF_8)))

  def decodeString(bytes: Chunk[Byte]): String = new String(bytes.toArray, StandardCharsets.UTF_8)

  private object Headers {
    val SimpleString = '+'.toByte
    val Error        = '-'.toByte
    val Integer      = ':'.toByte
    val BulkString   = '$'.toByte
    val Array        = '*'.toByte
  }

  private[redis] final val Cr = '\r'.toByte

  private[redis] final val Lf = '\n'.toByte

  private final val CrLf = Chunk(Cr, Lf)

  private final val NullString = Chunk.fromArray("$-1\r\n".getBytes(StandardCharsets.US_ASCII))

  private sealed trait State { self =>
    import State._

    final def notFinished: Boolean =
      self match {
        case InProgress(_) | CrSeen(_) => true
        case Complete(_) | Failed      => false
      }
  }

  private object State {
    final case class InProgress(chunk: Chunk[Char]) extends State
    final case class CrSeen(chunk: Chunk[Char])     extends State
    final case class Complete(chunk: Chunk[Char])   extends State
    case object Failed                              extends State
  }

  val SimpleStringDeserializer: Sink[RedisError.ProtocolError, Byte, Byte, String] = {
    import State._
    Sink
      .fold[Byte, State](InProgress(Chunk.empty))(_.notFinished) { (acc, b) =>
        acc match {
          case InProgress(chunk) if b == Cr => CrSeen(chunk)
          case InProgress(_) if b == Lf     => Failed
          case InProgress(chunk)            => InProgress(chunk :+ b.toChar)
          case CrSeen(chunk) if b == Lf     => Complete(chunk)
          case _                            => Failed
        }
      }
      .mapM {
        case Complete(chunk)                    => IO.succeed(chunk.mkString)
        case Failed                             => IO.fail(RedisError.ProtocolError("Invalid data string"))
        case InProgress(chunk) if chunk.isEmpty => IO.fail(RedisError.ProtocolError("Expected data missing"))
        case other                              => IO.dieMessage(s"Bug in deserialization, should not get: $other")
      }
  }

  val IntDeserializer: Sink[RedisError.ProtocolError, Byte, Byte, Long] =
    SimpleStringDeserializer.mapM { s =>
      IO.effect(s.toLong).refineOrDie { case _: NumberFormatException =>
        RedisError.ProtocolError(s"'$s' is not a valid integer")
      }
    }

  val BulkStringDeserializer: Sink[RedisError.ProtocolError, Byte, Byte, RespValue] =
    IntDeserializer.flatMap {
      case size if size >= 0 =>
        for {
          bytes <- Sink.take[Byte](size.toInt)
          _     <- Sink.take[Byte](2)
        } yield BulkString(bytes)
      case -1 =>
        Sink.succeed(NullValue)
      case other =>
        Sink.fail(RedisError.ProtocolError(s"Invalid bulk string length: $other"))
    }

  val ArrayDeserializer: Sink[RedisError.ProtocolError, Byte, Byte, RespValue] = {
    def help(
      count: Int,
      elements: Chunk[RespValue]
    ): Sink[RedisError.ProtocolError, Byte, Byte, Chunk[RespValue]] =
      if (count > 0) Deserializer.flatMap(element => help(count - 1, elements :+ element)) else Sink.succeed(elements)

    IntDeserializer.flatMap {
      case -1   => Sink.succeed(NullValue)
      case size => help(size.toInt, Chunk.empty).map(Array)
    }
  }

  val Deserializer: Sink[RedisError.ProtocolError, Byte, Byte, RespValue] =
    Sink.take[Byte](1).flatMap { header =>
      header.head match {
        case Headers.SimpleString => SimpleStringDeserializer.map(SimpleString)
        case Headers.Error        => SimpleStringDeserializer.map(Error)
        case Headers.Integer      => IntDeserializer.map(Integer)
        case Headers.BulkString   => BulkStringDeserializer
        case Headers.Array        => ArrayDeserializer
        case other =>
          Sink.fail[RedisError.ProtocolError, Byte](RedisError.ProtocolError(s"Invalid initial byte: $other"))
      }
    }

}
