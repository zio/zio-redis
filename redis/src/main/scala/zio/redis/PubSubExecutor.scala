package zio.redis

import zio.stream.{Stream, ZStream}
import zio.{Chunk, IO, Queue, Schedule, ZIO, ZLayer}

trait PubSubExecutor {
  def execute(command: Chunk[RespValue.BulkString]): Stream[RedisError, RespValue]
}

class PubSubExecutorImpl(
  queue: Queue[RespValue],
  connection: RedisConnection
) extends PubSubExecutor {

  def execute(command: Chunk[RespValue.BulkString]): Stream[RedisError, RespValue] =
    ZStream
      .fromZIO(send(command).as(baseStream))
      .flatten

  val run: IO[RedisError, AnyVal] =
    ZIO.logTrace(s"$this Executable reader has been started") *>
      (receive
        .repeat[Any, Long](Schedule.forever))
        .tapError(e => ZIO.logWarning(s"Reconnecting due to error: $e"))
        .retryWhile(_ => true)
        .tapError(e => ZIO.logError(s"Executor exiting: $e"))

  private val baseStream = ZStream.fromQueue(queue)

  private def send(command: Chunk[RespValue.BulkString]): IO[RedisError, Unit] = {
    val bytes = RespValue.Array(command).serialize
    connection
      .write(bytes)
      .unit
      .mapError(RedisError.IOError(_))
  }

  private def receive: IO[RedisError, Unit] =
    connection.read
      .mapError(RedisError.IOError(_))
      .via(RespValue.decoder)
      .collectSome
      .foreach(queue.offer(_))
}

object PubSubExecutor {
  lazy val layer: ZLayer[RedisConfig, RedisError.IOError, PubSubExecutor] =
    RedisConnectionLive.layer >>> pubSubExecutorLayer

  lazy val local: ZLayer[Any, RedisError.IOError, PubSubExecutor] =
    RedisConnectionLive.default >>> pubSubExecutorLayer

  private val pubSubExecutorLayer =
    ZLayer.scoped(
      for {
        connection <- ZIO.service[RedisConnection]
        queue      <- Queue.unbounded[RespValue]
        executor    = new PubSubExecutorImpl(queue, connection)
        _          <- executor.run.forkScoped
        _          <- logScopeFinalizer(s"$executor PubSubExecutor is closed")
      } yield new PubSubExecutorImpl(queue, connection)
    )
}
