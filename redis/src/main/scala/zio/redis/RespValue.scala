package zio.redis

import java.nio.charset.StandardCharsets

import zio.stream.{ Sink, ZSink }
import zio.{ Chunk, IO, Ref }

sealed trait RespValue extends Any {

  self =>

  import RespValue._

  final def serialize: Chunk[Byte] = {
    def simpleString(s: String) = Chunk.fromArray(s.getBytes(StandardCharsets.US_ASCII)) ++ CrLf

    self match {
      case SimpleString(s)   =>
        Header.simpleString +: simpleString(s)
      case Error(s)          =>
        Header.error +: simpleString(s)
      case Integer(i)        =>
        Header.integer +: simpleString(i.toString)
      case BulkString(bytes) =>
        Header.bulkString +: (simpleString(bytes.length.toString) ++ bytes ++ CrLf)
      case Array(elements)   =>
        Header.array +: (simpleString(elements.size.toString) ++ elements.foldLeft[Chunk[Byte]](Chunk.empty)(
          (acc, elem) => acc ++ elem.serialize
        ))
      case NullValue         =>
        NullString
    }
  }

  override final def toString: String =
    self match {
      case SimpleString(s)  => s
      case Error(s)         => s
      case Integer(i)       => i.toString
      case bulk: BulkString => bulk.asString
      case Array(elements)  => elements.mkString(",")
      case NullValue        => "null"
    }

}

object RespValue {

  private object Header {
    val simpleString = '+'.toByte
    val error        = '-'.toByte
    val integer      = ':'.toByte
    val bulkString   = '$'.toByte
    val array        = '*'.toByte
  }

  private[redis] val Cr = '\r'.toByte

  private[redis] val Lf = '\n'.toByte

  private val CrLf = Chunk(Cr, Lf)

  private val NullString = Chunk.fromArray("$-1\r\n".getBytes(StandardCharsets.US_ASCII))

  final case class SimpleString(value: String) extends AnyVal with RespValue

  final case class Error(value: String) extends AnyVal with RespValue

  final case class Integer(value: Long) extends AnyVal with RespValue

  final case class BulkString(value: Chunk[Byte]) extends AnyVal with RespValue {

    def asString: String = decodeString(value)

  }

  final case class Array(values: Chunk[RespValue]) extends AnyVal with RespValue

  case object NullValue extends RespValue

  def array(values: RespValue*): Array = Array(Chunk.fromIterable(values))

  def bulkString(s: String): BulkString = BulkString(Chunk.fromArray(s.getBytes(StandardCharsets.UTF_8)))

  def decodeString(bytes: Chunk[Byte]): String = new String(bytes.toArray, StandardCharsets.UTF_8)

  sealed trait State {

    self =>

    import State._

    final def finished: Boolean =
      self match {
        case InProgress(_) | CrSeen(_) => false
        case Complete(_) | Failed      => true
      }
  }

  object State {

    final case class InProgress(chunk: Chunk[Char]) extends State

    final case class CrSeen(chunk: Chunk[Char]) extends State

    final case class Complete(chunk: Chunk[Char]) extends State

    case object Failed extends State

  }

  val simpleStringDeserialize: Sink[RedisError.ProtocolError, Byte, Byte, String] = {
    import State._
    Sink
      .fold[Byte, State](InProgress(Chunk.empty))(!_.finished) { (acc, b) =>
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

  val intDeserialize: Sink[RedisError.ProtocolError, Byte, Byte, Long] =
    simpleStringDeserialize.mapM { s =>
      IO.effect(s.toLong).refineOrDie {
        case _: NumberFormatException => RedisError.ProtocolError(s"'$s' is not a valid integer")
      }
    }

  val bulkStringDeserialize: Sink[RedisError.ProtocolError, Byte, Byte, RespValue] =
    intDeserialize.flatMap {
      case size if size >= 0 =>
        for {
          bytes <- sinkTake[Byte](size.toInt)
          _     <- sinkTake[Byte](2) // crlf terminator
        } yield BulkString(bytes)
      case -1                =>
        Sink.succeed(NullValue)
      case other             =>
        Sink.fail(RedisError.ProtocolError(s"Invalid bulk string length: $other"))
    }

  val arrayDeserialize: Sink[RedisError.ProtocolError, Byte, Byte, RespValue] = {
    def help(
      count: Int,
      elements: Chunk[RespValue]
    ): Sink[RedisError.ProtocolError, Byte, Byte, Chunk[RespValue]] =
      if (count > 0) deserialize.flatMap(element => help(count - 1, elements :+ element)) else Sink.succeed(elements)

    intDeserialize.flatMap {
      case -1   => Sink.succeed(NullValue)
      case size => help(size.toInt, Chunk.empty).map(Array)
    }
  }

  val deserialize: Sink[RedisError.ProtocolError, Byte, Byte, RespValue] =
    sinkTake[Byte](1).flatMap { header =>
      header.head match {
        case Header.simpleString => simpleStringDeserialize.map(SimpleString)
        case Header.error        => simpleStringDeserialize.map(Error)
        case Header.integer      => intDeserialize.map(Integer)
        case Header.bulkString   => bulkStringDeserialize
        case Header.array        => arrayDeserialize
        case other               =>
          Sink.fail[RedisError.ProtocolError, Byte](RedisError.ProtocolError(s"Invalid initial byte: $other"))
      }
    }

  object ArrayValues {
    def unapplySeq(v: RespValue): Option[Seq[RespValue]] =
      v match {
        case Array(values) => Some(values)
        case _             => None
      }
  }

  /**
   * Fixed version of `ZSink.take`.
   *
   * This will be removed once https://github.com/zio/zio/pull/4342 is available.
   */
  private def sinkTake[I](n: Int): ZSink[Any, Nothing, I, I, Chunk[I]] =
    ZSink {
      for {
        state <- Ref.make[Chunk[I]](Chunk.empty).toManaged_
        push   = (is: Option[Chunk[I]]) =>
                 state.get.flatMap { take =>
                   is match {
                     case Some(ch) =>
                       val idx = n - take.length
                       if (idx <= ch.length) {
                         val (chunk, leftover) = ch.splitAt(idx)
                         state.set(Chunk.empty) *> ZSink.Push.emit(take ++ chunk, leftover)
                       } else
                         state.set(take ++ ch) *> ZSink.Push.more
                     case None     =>
                       if (n >= 0) ZSink.Push.emit(take, Chunk.empty)
                       else ZSink.Push.emit(Chunk.empty, take)
                   }
                 }
      } yield push
    }

}
