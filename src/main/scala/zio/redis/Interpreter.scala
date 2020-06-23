package zio.redis

import java.io.IOException
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Arrays
import java.util.concurrent.atomic.AtomicBoolean

import zio._

trait Interpreter {
  type RedisExecutor = Has[RedisExecutor.Service]

  object RedisExecutor {
    trait Service {
      def execute(command: Chunk[String]): IO[RedisError, String]
    }

    def live(host: String, port: Int): Layer[IOException, RedisExecutor] =
      ZLayer.fromManaged {
        for {
          connection <- connect(host, port)
          _          <- connection.receive.forever.forkManaged
        } yield new Service {
          def execute(command: Chunk[String]): IO[RedisError, String] = connection.send(command).flatMap(_.await)
        }
      }

    private[this] def connect(host: String, port: Int): Managed[IOException, Connection] = {
      val makeChannel =
        IO {
          val channel = SocketChannel.open(new InetSocketAddress(host, port))
          channel.configureBlocking(false)
          channel
        }

      val connection =
        for {
          channel         <- Managed.fromAutoCloseable(makeChannel)
          pendingRequests <- Queue.unbounded[Request].toManaged_
          readinessFlag    = new AtomicBoolean(true)
          response         = ByteBuffer.allocate(1024)
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
        UIO.effectSuspendTotal {
          val ready = readinessFlag.compareAndSet(true, false)

          if (ready)
            executeNext
          else
            UIO.unit
        }

      def send(command: Chunk[String]): UIO[Promise[RedisError, String]] =
        Promise.make[RedisError, String].flatMap(p => pendingRequests.offer(Request(command, p)).as(p))

      private val executeNext: UIO[Any] =
        pendingRequests.take.flatMap { request =>
          val exchange =
            IO {
              unsafeSend(request.command)
              unsafeReceive()
            }

          val completer =
            exchange
              .catchAll(RedisError.make)
              .ensuring(UIO(readinessFlag.set(true)))

          request.result.completeWith(completer)
        }

      private def unsafeReceive(): String = {
        val sb        = new StringBuilder
        var readBytes = 0

        // TODO: handle -1
        while (readBytes == 0) {
          channel.read(response)
          response.flip()

          readBytes = response.limit()

          sb ++= new String(Arrays.copyOf(response.array(), readBytes), UTF_8)

          response.clear()
        }

        sb.toString
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
