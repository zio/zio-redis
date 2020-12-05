package zio.redis

import zio._
import zio.logging._

object RedisExecutor {
  trait Service {
    def execute(command: Chunk[RespValue.BulkString]): IO[RedisError, RespValue]
  }

  lazy val live: ZLayer[Logging with Has[RedisConfig], RedisError.IOError, RedisExecutor] =
    ZLayer.identity[Logging] ++ ByteStream.live >>> StreamedExecutor

  lazy val local: ZLayer[Logging, RedisError.IOError, RedisExecutor] =
    ZLayer.identity[Logging] ++ ByteStream.default >>> StreamedExecutor

  lazy val test: URLayer[zio.random.Random, RedisExecutor] = TestExecutor.live

  private[this] final case class Request(command: Chunk[RespValue.BulkString], promise: Promise[RedisError, RespValue])

  private[this] final val True: Any => Boolean = _ => true

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

  private[this] final class Live(
    reqQueue: Queue[Request],
    resQueue: Queue[Promise[RedisError, RespValue]],
    byteStream: ByteStream.Service,
    logger: Logger[String]
  ) extends Service {

    def execute(command: Chunk[RespValue.BulkString]): IO[RedisError, RespValue] =
      Promise
        .make[RedisError, RespValue]
        .flatMap(promise => reqQueue.offer(Request(command, promise)) *> promise.await)

    /**
     * Opens a connection to the server and launches send and receive operations.
     * All failures are retried by opening a new connection.
     * Only exits by interruption or defect.
     */
    val run: IO[RedisError, Unit] =
      (send.forever race receive)
        .tapError(e => logger.warn(s"Reconnecting due to error: $e") *> drainWith(e))
        .retryWhile(True)
        .tapError(e => logger.error(s"Executor exiting: $e"))

    private def drainWith(e: RedisError): UIO[Unit] = resQueue.takeAll.flatMap(IO.foreach_(_)(_.fail(e)))

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

    private def receive: IO[RedisError, Unit] =
      byteStream.read
        .mapError(RedisError.IOError)
        .transduce(RespValue.Deserializer)
        .foreach(response => resQueue.take.flatMap(_.succeed(response)))

  }
}
