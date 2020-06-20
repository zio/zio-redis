package zio.redis

import java.io.IOException
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.channels.SocketChannel

import zio._

trait Interpreter {
  type RedisExecutor = Has[RedisExecutor.Service]

  object RedisExecutor {
    trait Service {
      def execute(command: Chunk[String]): IO[RedisError, String]
    }

    def live(host: String, port: Int): Layer[IOException, RedisExecutor] = {
      import internal._

      val connect = IO.effect(unsafeConnect(host, port)).refineToOrDie[IOException]

      ZLayer.fromManaged {
        for {
          channel <- ZManaged.fromAutoCloseable(connect)
          queue   <- Queue.unbounded[Request].toManaged_
          _       <- dequeue(queue, channel).forever.forkManaged
        } yield new Service {
          def execute(command: Chunk[String]): IO[RedisError, String] = enqueue(command, queue).flatMap(_.await)
        }
      }
    }

    private[this] object internal {
      type Request = (Chunk[String], Promise[RedisError, String])

      def dequeue(queue: Queue[Request], channel: SocketChannel): UIO[Any] =
        queue.take.flatMap {
          case (command, result) =>
            val exchange =
              IO.effect {
                unsafeSend(command, channel)
                unsafeReceive(channel)
              }

            result.completeWith(exchange.catchAll(RedisError.make))
        }

      def enqueue(command: Chunk[String], queue: Queue[Request]): UIO[Promise[RedisError, String]] =
        Promise.make[RedisError, String].flatMap(p => queue.offer((command, p)).as(p))

      def unsafeConnect(host: String, port: Int): SocketChannel = {
        val channel = SocketChannel.open(new InetSocketAddress(host, port))

        channel.configureBlocking(false)

        channel
      }

      def unsafeSend(command: Chunk[String], channel: SocketChannel): Unit = {
        val data     = command.mkString
        val envelope = s"*${command.length}\r\n$data"
        val buffer   = ByteBuffer.wrap(envelope.getBytes(UTF_8))

        while (buffer.hasRemaining())
          channel.write(buffer)
      }

      def unsafeReceive(channel: SocketChannel): String = {
        val buffer = ByteBuffer.allocate(1024)
        val sb     = new StringBuilder

        while (channel.read(buffer) > 0) {
          sb ++= new String(buffer.array(), UTF_8)
          buffer.clear()
        }

        sb.toString
      }
    }
  }
}
