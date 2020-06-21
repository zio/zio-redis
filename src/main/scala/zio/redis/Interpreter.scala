package zio.redis

import java.io.IOException
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Arrays

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
          val ch = SocketChannel.open(new InetSocketAddress(host, port))
          ch.configureBlocking(false)
          ch
        }

      val connection =
        for {
          channel <- Managed.fromAutoCloseable(makeChannel)
          queue   <- Queue.unbounded[Request].toManaged_
          inbox   <- UIO(ByteBuffer.allocate(1024)).toManaged_
        } yield new Connection(channel, inbox, queue)

      connection.refineToOrDie[IOException]
    }

    private[this] final class Connection(
      channel: SocketChannel,
      response: ByteBuffer,
      queue: Queue[Request]
    ) {
      val receive: UIO[Any] =
        queue.take.flatMap { request =>
          val exchange =
            IO {
              unsafeSend(request.command)
              unsafeReceive()
            }

          request.result.completeWith(exchange.catchAll(RedisError.make))
        }

      def send(command: Chunk[String]): UIO[Promise[RedisError, String]] =
        Promise.make[RedisError, String].flatMap(p => queue.offer(Request(command, p)).as(p))

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

        sb.toString.trim()
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
