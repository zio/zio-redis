package zio.redis

import java.nio.charset.Charset

import zio.blocking.Blocking
import zio.nio.channels.AsynchronousSocketChannel
import zio.nio.{ SocketAddress, StandardSocketOptions }
import zio.redis.Parse.{ Implicits => Parsers }
import zio.stream.{ ZStream, ZStreamChunk }
import zio._

import scala.language.implicitConversions

private[redis] trait Commands {

  val ERROR  = '-'
  val SINGLE = '+'
  val QUEUED = "QUEUED".getBytes("utf-8")
  val ARRAY  = '*'
  val BULK   = '$'
  val OK     = "OK".getBytes("utf-8")
  val INT    = ':'

  val LS = Chunk.fromArray("\r\n".getBytes("utf-8"))

  def multiBulk(args: Seq[Array[Byte]]): Chunk[Byte] =
    args.foldLeft(Chunk.fromIterable("*%d".format(args.size).getBytes) ++ LS) {
      case (b, arg) =>
        b ++ Chunk.fromArray("$%d".format(arg.length).getBytes) ++ LS ++ Chunk.fromArray(arg) ++ LS
    }

}

/**
 * A reply reader protocol for
 */
private[redis] trait Reply extends Commands {
  type Reply[T] = PartialFunction[(Char, Array[Byte]), RIO[RedisIO, T]]

  def receive[T](reply: Reply[T]): ZIO[RedisIO, Throwable, T]

  val singleLineReply: Reply[Option[Array[Byte]]] = {
    case (SINGLE | INT, value) => Task.effectTotal(Some(value))
  }

  val integerReply: Reply[Option[Int]] = {
    case (INT, s)                               => Task.succeed(Some(Parsers.parseInt(s)))
    case (BULK, s) if Parsers.parseInt(s) == -1 => Task.succeed(None)
  }

  val longReply: Reply[Option[Long]] = {
    case (INT, s)                               => Task.effectTotal(Some(Parsers.parseLong(s)))
    case (BULK, s) if Parsers.parseInt(s) == -1 => Task.succeed(None)
  }

  val bulkReply: Reply[Option[Array[Byte]]] = {
    case (BULK, value) =>
      Parsers.parseInt(value) match {
        case -1 => Task.succeed(None)
        case n =>
          ZIO.accessM[RedisIO] { io =>
            for {
              line <- (io.readCounted(n) <* io.readLine)
            } yield Some(line.toArray)
          }
      }
  }

  val multiBulkReply: Reply[List[Array[Byte]]] = {
    case (ARRAY, body) =>
      Parsers.parseInt(body) match {
        case -1 => Task.succeed(List.empty)
        case n =>
          ZIO
            .collectAll(List.fill(n)(receive(bulkReply orElse singleLineReply)))
            .map(_.flatten)
      }
  }

  def queuedReplyInt: Reply[Option[Int]] = {
    case (SINGLE, QUEUED) => Task.succeed(Some(Int.MaxValue))
  }

  def queuedReplyLong: Reply[Option[Long]] = {
    case (SINGLE, QUEUED) => Task.succeed(Some(Long.MaxValue))
  }

  def queuedReplyList: Reply[Option[List[Option[Array[Byte]]]]] = {
    case (SINGLE, QUEUED) => Task.succeed(Some(List(Some(QUEUED))))
  }

  val errReply: Reply[Nothing] = {
    case (ERROR, s) => ZIO.fail(new Exception(Parsers.parseString(s)))
    case x          => ZIO.fail(new Exception("Protocol error: Got " + x + " as initial reply byte"))
  }

  def asList[T](implicit parse: Parse[T]): ZIO[RedisIO, Throwable, List[T]] = receive(multiBulkReply).map(_.map(parse))

  def asListPairs[A, B](implicit parseA: Parse[A], parseB: Parse[B]): ZIO[RedisIO, Throwable, List[Option[(A, B)]]] =
    receive(multiBulkReply).map(_.grouped(2).flatMap {
      case List(a, b) => Iterator.single(Some((parseA(a), parseB(b))))
      case _          => Iterator.single(None)
    }.toList)

  def asString: RIO[RedisIO, Option[String]] = receive(singleLineReply).map(_.map(Parsers.parseString))

  def asBulk[T](implicit parse: Parse[T]): RIO[RedisIO, Option[T]] = receive(bulkReply).map(_.map(parse))

  def asBulkWithTime[T](implicit parse: Parse[T]): RIO[RedisIO, Option[T]] =
    receive(bulkReply orElse multiBulkReply).map {
      case Some(bytes: Array[Byte]) => Some(parse(bytes))
      case _                        => None
    }

  def asInt: RIO[RedisIO, Option[Int]]   = receive(integerReply orElse queuedReplyInt)
  def asLong: RIO[RedisIO, Option[Long]] = receive(longReply orElse queuedReplyLong)

  def asBoolean: RIO[RedisIO, Boolean] = receive(integerReply orElse singleLineReply) map {
    case Some(n: Int) => n > 0
    case Some(s: Array[Byte]) =>
      Parsers.parseString(s) match {
        case "OK"     => true
        case "QUEUED" => true
        case _        => false
      }
    case _ => false
  }

}

object Parse {
  private val utf8 = Charset.forName("utf-8")

  def apply[T](f: (Array[Byte]) => T) = new Parse[T](f)

  object Implicits {
    implicit val parseString: Parse[String]         = Parse[String](new String(_, utf8))
    implicit val parseByteArray: Parse[Array[Byte]] = Parse[Array[Byte]](x => x)
    implicit val parseInt: Parse[Int]               = Parse[Int](new String(_, utf8).toInt)
    implicit val parseLong: Parse[Long]             = Parse[Long](new String(_, utf8).toLong)
    implicit val parseDouble: Parse[Double]         = Parse[Double](new String(_, utf8).toDouble)
  }

  implicit val parseDefault: Parse[String] = Parse[String](new String(_, utf8))

  val parseStringSafe: Parse[String] = Parse[String](xs =>
    new String(xs.iterator.flatMap {
      case x if x > 31 && x < 127 => Iterator.single(x.toChar)
      case 10                     => "\\n".iterator
      case 13                     => "\\r".iterator
      case x                      => "\\x%02x".format(x).iterator
    }.toArray)
  )
}

class Parse[A](val f: (Array[Byte]) => A) extends (Array[Byte] => A) {
  def apply(in: Array[Byte]): A = f(in)
}

trait Protocol extends Reply {

  def receive[T](reply: Reply[T]): ZIO[RedisIO, Throwable, T] =
    ZIO.accessM[RedisIO] { io =>
      for {
        line <- io.readLine
        body <- (reply orElse errReply)((line(0).toChar, line.drop(1).toArray))
      } yield body
    }

}

class RedisIO(channel: AsynchronousSocketChannel) {

  def write(data: Chunk[Byte]): Task[Int] =
    channel.write(data)

  // TODO Close the socket if the read fails
  def readLine: Task[Chunk[Byte]] =
    ZStreamChunk(ZStream.fromEffect(channel.read(1)).forever)
      .takeWhile(_ != '\r')
      .flattenChunks
      .runCollect
      .map(Chunk.fromIterable) <* readCounted(1) // Chew off the last \n character

  // TODO Close the socket if the read fails
  def readCounted(count: Int): Task[Chunk[Byte]] =
    channel.read(count)

}

object RedisIO {

  def make[R](host: String, port: Int): ZManaged[R with Blocking, Throwable, RedisIO] =
    AsynchronousSocketChannel().mapM { channel =>
      for {
        address <- SocketAddress.inetSocketAddress(host, port)
        _       <- channel.connect(address)
        _       <- channel.setOption[java.lang.Boolean](StandardSocketOptions.SO_KEEPALIVE, true)
        _       <- channel.setOption[java.lang.Boolean](StandardSocketOptions.TCP_NODELAY, true)
      } yield new RedisIO(channel)
    }
}

trait Redis extends Reply with Commands with Protocol with HashApi {

  private val utf8 = Charset.forName("utf8")

  def send[A](command: String, args: RedisArgMagnet*)(result: ZIO[RedisIO, Throwable, A]): ZIO[RedisIO, Throwable, A] =
    ZIO.accessM[RedisIO] { io =>
      io.write(multiBulk(command.getBytes(utf8) +: args.map(_.bytes))) *> result
    }

}

object R extends Redis

sealed trait RedisArgMagnet {
  def bytes: Array[Byte]
}

object RedisArgMagnet {

  def apply(arg: RedisArgMagnet): RedisArgMagnet = arg

  private case class StringArgMagnet(str: String) extends RedisArgMagnet {
    def bytes: Array[Byte] = str.getBytes("UTF-8")
  }

  private case class ByteArrayMagnet(ba: Array[Byte]) extends RedisArgMagnet {
    val bytes = ba
  }

  implicit def stringMagnet(str: String): RedisArgMagnet =
    StringArgMagnet(str)

  implicit def byteArrayMagnet(str: Array[Byte]): RedisArgMagnet =
    ByteArrayMagnet(str)

}
