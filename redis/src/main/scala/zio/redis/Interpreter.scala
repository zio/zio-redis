package zio.redis

import java.io.IOException
import java.net.SocketAddress

import zio._
import zio.logging._
import zio.redis.RedisError.ProtocolError
import zio.stm._
import zio.stream.Stream

trait Interpreter {

  import Interpreter._

  type RedisExecutor = Has[RedisExecutor.Service]

  object RedisExecutor {
    trait Service {
      def execute(command: Chunk[RespValue.BulkString]): IO[RedisError, RespValue]
    }

    def live(address: => SocketAddress): ZLayer[Logging, RedisError.IOError, RedisExecutor] =
      (ZLayer.identity[Logging] ++ ByteStream.live(address)) >>> StreamedExecutor

    def live(host: String, port: Int = DefaultPort): ZLayer[Logging, RedisError.IOError, RedisExecutor] =
      (ZLayer.identity[Logging] ++ ByteStream.live(host, port)) >>> StreamedExecutor

    def loopback(port: Int = DefaultPort): ZLayer[Logging, RedisError.IOError, RedisExecutor] =
      (ZLayer.identity[Logging] ++ ByteStream.loopback(port)) >>> StreamedExecutor

    val test: ZLayer[Any, Nothing, RedisExecutor] = ZLayer.succeed(new InMemory())

    private[redis] final val DefaultPort = 6379

    private[this] final val RequestQueueSize = 16

    private[this] final val StreamedExecutor =
      ZLayer.fromServicesManaged[ByteStream.Service, Logger[String], Any, RedisError.IOError, RedisExecutor.Service] {
        (byteStream: ByteStream.Service, logging: Logger[String]) =>
          for {
            reqQueue <- Queue.bounded[Request](RequestQueueSize).toManaged_
            resQueue <- Queue.unbounded[Promise[RedisError, RespValue]].toManaged_
            live      = new Live(reqQueue, resQueue, byteStream, logging)
            _        <- live.run.forkManaged
          } yield live
      }

    private final class Live(
      reqQueue: Queue[Request],
      resQueue: Queue[Promise[RedisError, RespValue]],
      byteStream: ByteStream.Service,
      logger: Logger[String]
    ) extends Service {

      def execute(command: Chunk[RespValue.BulkString]): IO[RedisError, RespValue] =
        Promise
          .make[RedisError, RespValue]
          .flatMap(promise => reqQueue.offer(Request(command, promise)) *> promise.await)

      private def send: IO[RedisError, Unit] =
        reqQueue.takeBetween(1, Int.MaxValue).flatMap { reqs =>
          val bytes = Chunk.fromIterable(reqs).flatMap(req => RespValue.Array(req.command).serialize)
          byteStream
            .write(bytes)
            .mapError(RedisError.IOError)
            .tapBoth(
              e => IO.foreach_(reqs)(_.promise.fail(e)),
              _ => IO.foreach_(reqs)(req => resQueue.offer(req.promise))
            )
        }

      private def runReceive(inStream: Stream[IOException, Byte]): IO[RedisError, Unit] =
        inStream
          .mapError(RedisError.IOError)
          .transduce(RespValue.deserialize.toTransducer)
          .foreach(response => resQueue.take.flatMap(_.succeed(response)))

      /**
       * Opens a connection to the server and launches send and receive operations.
       * All failures are retried by opening a new connection.
       * Only exits by interruption or defect.
       */
      def run: IO[RedisError, Unit] =
        (send.forever race runReceive(byteStream.read)).tapError { e =>
          logger.warn(s"Reconnecting due to error: $e") *> resQueue.takeAll.flatMap(IO.foreach_(_)(_.fail(e)))
        }.retryWhile(Function.const(true))
          .tapError(e => logger.error(s"Executor exiting: $e"))
    }

    private final class InMemory() extends Service {
      def execute(command: Chunk[RespValue.BulkString]): zio.IO[RedisError, RespValue] =
        for {
          name   <- ZIO.fromOption(command.headOption).orElseFail(ProtocolError("Malformed command."))
          result <- runCommand(name.asString, command.tail).commit
        } yield result

      private[this] def runCommand(name: String, input: Chunk[RespValue.BulkString]): STM[RedisError, RespValue] =
        name match {
          case api.Connection.Ping.name =>
            STM.succeedNow {
              if (input.isEmpty)
                RespValue.bulkString("PONG")
              else
                input.head
            }

          case _                        => STM.fail(RedisError.ProtocolError(s"Command not supported by test executor: $name"))
        }
    }
  }
}

private[redis] object Interpreter {
  private final case class Request(command: Chunk[RespValue.BulkString], promise: Promise[RedisError, RespValue])
}
