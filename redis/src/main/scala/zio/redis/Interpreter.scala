package zio.redis

import java.io.IOException
import java.net.{ InetSocketAddress, StandardSocketOptions }
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.atomic.AtomicBoolean

import zio._
import zio.blocking._

import scala.collection.mutable.ArrayBuilder

trait Interpreter {
  type RedisExecutor = Has[RedisExecutor.Service]

  object RedisExecutor {

    val ResponseBufferSize = 1024

    trait Service {
      def execute(command: Chunk[String]): IO[RedisError, String]
    }

    def live(host: String, port: Int): ZLayer[Blocking, IOException, RedisExecutor] =
      ZLayer.fromServiceManaged { env =>
        for {
          connection <- connect(host, port)
          _          <- env.blocking(connection.receive.forever).forkManaged
        } yield new Service {
          def execute(command: Chunk[String]): IO[RedisError, String] = connection.send(command).flatMap(_.await)
        }
      }

    private[this] def connect(host: String, port: Int): Managed[IOException, Connection] = {
      val makeChannel =
        IO {
          val channel = SocketChannel.open(new InetSocketAddress(host, port))
          channel.configureBlocking(false)
          channel.setOption(StandardSocketOptions.SO_KEEPALIVE, Boolean.box(true))
          channel.setOption(StandardSocketOptions.TCP_NODELAY, Boolean.box(true))
          channel
        }

      val connection =
        for {
          channel         <- Managed.fromAutoCloseable(makeChannel)
          pendingRequests <- Queue.unbounded[Request].toManaged_
          readinessFlag    = new AtomicBoolean(true)
          response         = ByteBuffer.allocate(ResponseBufferSize)
        } yield new Connection(channel, pendingRequests, readinessFlag, response)

      connection.refineToOrDie[IOException]
    }

    private[this] final class Connection(
      channel: SocketChannel,
      pendingRequests: Queue[Request],
      readinessFlag: AtomicBoolean,
      response: ByteBuffer
    ) {
      val receive: UIO[Any] =
        UIO.effectSuspendTotal(if (readinessFlag.compareAndSet(true, false)) executeNext else UIO.unit)

      def send(command: Chunk[String]): UIO[Promise[RedisError, String]] =
        Promise.make[RedisError, String].flatMap(p => pendingRequests.offer(Request(command, p)).as(p))

      private val executeNext: UIO[Any] =
        pendingRequests.take.map { request =>
          val exchange =
            IO.effect {
              try {
                unsafeSend(request.command)
                unsafeReceive()
              } catch {
                case t: Throwable => throw RedisError.ProtocolError(t.getMessage)
              } finally readinessFlag.set(true)
            }

          request.result.unsafeDone(exchange.refineToOrDie[RedisError])
        }

      private def unsafeReceive(): String = {
        val builder      = ArrayBuilder.make[Array[Byte]]
        var readBytes    = 0
        var responseSize = 0

        // TODO: handle -1
        while (readBytes == 0 || readBytes == ResponseBufferSize) {
          channel.read(response)
          response.flip()

          readBytes = response.remaining()
          responseSize += readBytes

          val chunk = Array.ofDim[Byte](readBytes)

          response.get(chunk)

          builder += chunk

          response.clear()
        }

        val chunks = builder.result()
        val data   = Array.ofDim[Byte](responseSize)
        var i      = 0
        var j      = 0

        while (i < chunks.length) {
          val chunk = chunks(i)

          System.arraycopy(chunk, 0, data, j, chunk.length)

          i += 1
          j += chunk.length
        }

        new String(data, UTF_8)
      }

      private def unsafeSend(command: Chunk[String]): Unit = {
        val data     = command.mkString
        val envelope = s"*${command.length}\r\n$data"
        val buffer   = ByteBuffer.wrap(envelope.getBytes(UTF_8))

        while (buffer.hasRemaining())
          channel.write(buffer)
      }
    }
  }

  private[this] sealed case class Request(command: Chunk[String], result: Promise[RedisError, String])
}
