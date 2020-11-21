package zio.redis

import java.io.{ EOFException, IOException }
import java.net.{ InetAddress, InetSocketAddress, SocketAddress, StandardSocketOptions }
import java.nio.ByteBuffer
import java.nio.channels.{ AsynchronousSocketChannel, Channel, CompletionHandler }

import zio._
import zio.logging._
import zio.stream.Stream

private[redis] object ByteStream {
  trait Service {
    def read: Stream[IOException, Byte]
    def write(chunk: Chunk[Byte]): IO[IOException, Unit]
  }

  def live(host: String, port: Int): ZLayer[Logging, RedisError.IOError, ByteStream] =
    live(new InetSocketAddress(host, port))

  def live(address: => SocketAddress): ZLayer[Logging, RedisError.IOError, ByteStream] = connect(address)

  def loopback(port: Int = RedisExecutor.DefaultPort): ZLayer[Logging, RedisError.IOError, ByteStream] =
    live(new InetSocketAddress(InetAddress.getLoopbackAddress, port))

  private[this] def connect(address: => SocketAddress): ZLayer[Logging, RedisError.IOError, ByteStream] =
    ZLayer.fromManaged {
      val managed =
        for {
          address     <- UIO(address).toManaged_
          makeBuffer   = IO.effectTotal(ByteBuffer.allocateDirect(ResponseBufferSize))
          readBuffer  <- makeBuffer.toManaged_
          writeBuffer <- makeBuffer.toManaged_
          channel     <- openChannel(address)
        } yield new Connection(readBuffer, writeBuffer, channel)

      managed.mapError(RedisError.IOError)
    }

  private[this] final val ResponseBufferSize = 1024

  private[this] def completionHandler[A](k: IO[IOException, A] => Unit): CompletionHandler[A, Any] =
    new CompletionHandler[A, Any] {
      def completed(result: A, u: Any): Unit = k(IO.succeedNow(result))

      def failed(t: Throwable, u: Any): Unit =
        t match {
          case e: IOException => k(IO.fail(e))
          case _              => k(IO.die(t))
        }
    }

  private[this] def closeWith[A](channel: Channel)(op: CompletionHandler[A, Any] => Any): IO[IOException, A] =
    IO.effectAsyncInterrupt { k =>
      op(completionHandler(k))
      Left(IO.effect(channel.close()).ignore)
    }

  private[this] def openChannel(address: SocketAddress): ZManaged[Logging, IOException, AsynchronousSocketChannel] =
    Managed.fromAutoCloseable {
      for {
        logger  <- ZIO.service[Logger[String]]
        channel <- IO.effect {
                     val channel = AsynchronousSocketChannel.open()
                     channel.setOption(StandardSocketOptions.SO_KEEPALIVE, Boolean.box(true))
                     channel.setOption(StandardSocketOptions.TCP_NODELAY, Boolean.box(true))
                     channel
                   }
        _       <- closeWith[Void](channel)(channel.connect(address, null, _))
        _       <- logger.info("Connected to the redis server.")
      } yield channel
    }.refineToOrDie[IOException]

  private[this] final class Connection(
    readBuffer: ByteBuffer,
    writeBuffer: ByteBuffer,
    channel: AsynchronousSocketChannel
  ) extends Service {

    val read: Stream[IOException, Byte] =
      Stream.repeatEffectChunkOption {
        (for {
          _     <- IO.effectTotal(readBuffer.clear())
          _     <- closeWith[Integer](channel)(channel.read(readBuffer, null, _)).filterOrFail(_ >= 0)(new EOFException())
          chunk <- IO.effectTotal {
                     readBuffer.flip()
                     val count = readBuffer.remaining()
                     val array = Array.ofDim[Byte](count)
                     readBuffer.get(array)
                     Chunk.fromArray(array)
                   }
        } yield chunk).mapError {
          case _: EOFException => None
          case e: IOException  => Some(e)
        }
      }

    def write(chunk: Chunk[Byte]): IO[IOException, Unit] =
      IO.when(chunk.nonEmpty) {
        IO.effectTotal {
          writeBuffer.clear()
          val (c, remainder) = chunk.splitAt(writeBuffer.capacity())
          writeBuffer.put(c.toArray)
          writeBuffer.flip()
          remainder
        }.flatMap { remainder =>
          closeWith[Integer](channel)(channel.write(writeBuffer, null, _))
            .repeatWhileM(_ => IO.effectTotal(writeBuffer.hasRemaining))
            .zipRight(write(remainder))
        }
      }
  }
}
