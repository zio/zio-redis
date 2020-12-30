package zio.redis

import java.nio.charset.StandardCharsets

import zio._
import zio.stream._

sealed trait RespValue extends Product with Serializable { self =>
  import RespValue._
  import RespValue.internal.{ Headers, NullString, CrLf }

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
  final case class SimpleString(value: String) extends RespValue

  final case class Error(value: String) extends RespValue

  final case class Integer(value: Long) extends RespValue

  final case class BulkString(value: Chunk[Byte]) extends RespValue {
    def asString: String = decodeString(value)
  }

  final case class Array(values: Chunk[RespValue]) extends RespValue

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

  private[redis] final val Cr: Byte = '\r'

  private[redis] final val Lf: Byte = '\n'

  private[redis] final val Deserializer: Transducer[RedisError.ProtocolError, Byte, RespValue] = {
    import internal.State

    // TODO: handle NumberFormatException
    // TODO: remove utf8Decode transducer
    // val lineSplitter: Transducer[Nothing, Byte, Byte] =
    // ZTransducer {
    //   ZRef.makeManaged[(Option[Byte], Boolean)]((None, false)).map { stateRef =>
    //     {
    //       case None =>
    //         stateRef.getAndSet((None, false)).flatMap {
    //           case (None, _)      => ZIO.succeedNow(Chunk.empty)
    //           case (Some(str), _) => ZIO.succeedNow(Chunk(str))
    //         }

    //       case Some(strings) =>
    //         stateRef.modify { case (leftover, wasSplitCRLF) =>
    //           val buf    = scala.collection.mutable.ArrayBuffer[Byte]()
    //           var inCRLF = wasSplitCRLF
    //           var carry  = leftover getOrElse 0.toByte

    //           strings.foreach { string =>
    //             val concat = carry + string

    //             if (concat.length() > 0) {
    //               var i =
    //                 // If we had a split CRLF, we start reading
    //                 // from the last character of the leftover (which was the '\r')
    //                 if (inCRLF && carry.length > 0) carry.length - 1
    //                 // Otherwise we just skip over the entire previous leftover as
    //                 // it doesn't contain a newline.
    //                 else carry.length
    //               var sliceStart = 0

    //               while (i < concat.length()) {
    //                 if (concat(i) == '\n') {
    //                   buf += concat.substring(sliceStart, i)
    //                   i += 1
    //                   sliceStart = i
    //                 } else if (concat(i) == '\r' && (i + 1) < concat.length && concat(i + 1) == '\n') {
    //                   buf += concat.substring(sliceStart, i)
    //                   i += 2
    //                   sliceStart = i
    //                 } else if (concat(i) == '\r' && i == concat.length - 1) {
    //                   inCRLF = true
    //                   i += 1
    //                 } else {
    //                   i += 1
    //                 }
    //               }

    //               carry = concat.substring(sliceStart, concat.length)
    //             }
    //           }

    //           (Chunk.fromArray(buf.toArray), (if (carry.length() > 0) Some(carry) else None, inCRLF))
    //         }
    //     }
    //   }
    // }
 
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

    final val CrLf: Chunk[Byte] = Chunk(Cr, Lf)

    final val Null: String = "$-1"

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
          case Start if line == Null => State.Done(NullValue)

          case Start if line.nonEmpty =>
            line.head match {
              case Headers.SimpleString => Done(SimpleString(line.tail))
              case Headers.Error        => Done(Error(line.tail))
              case Headers.Integer      => Done(Integer(unsafeReadLong(line)))
              case Headers.BulkString   => ExpectingBulk
              case Headers.Array        => CollectingArray(unsafeReadLong(line).toInt, Chunk.empty, Start.feed)
            }

          case CollectingArray(rem, vals, next) if rem > 0 =>
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
    
    def unsafeReadLong(text: String): Long = {
      var pos = 1
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
