/*
 * Copyright 2021 John A. De Goes and the ZIO contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.redis

import zio._
import zio.redis.SingleNodeExecutor._

final class SingleNodeExecutor(
  reqQueue: Queue[Request],
  resQueue: Queue[Promise[RedisError, RespValue]],
  connection: RedisConnection
) extends RedisExecutor {

  // TODO NodeExecutor doesn't throw connection errors, timeout errors, it is hanging forever
  def execute(command: RespCommand): IO[RedisError, RespValue] =
    Promise
      .make[RedisError, RespValue]
      .flatMap(promise => reqQueue.offer(Request(command.args.map(_.value), promise)) *> promise.await)

  /**
   * Opens a connection to the server and launches send and receive operations. All failures are retried by opening a
   * new connection. Only exits by interruption or defect.
   */
  val run: IO[RedisError, AnyVal] =
    ZIO.logTrace(s"$this Executable sender and reader has been started") *>
      (send.repeat[Any, Long](Schedule.forever) race receive)
        .tapError(e => ZIO.logWarning(s"Reconnecting due to error: $e") *> drainWith(e))
        .retryWhile(True)
        .tapError(e => ZIO.logError(s"Executor exiting: $e"))

  private def drainWith(e: RedisError): UIO[Unit] = resQueue.takeAll.flatMap(ZIO.foreachDiscard(_)(_.fail(e)))

  private def send: IO[RedisError.IOError, Option[Unit]] =
    reqQueue.takeBetween(1, RequestQueueSize).flatMap { reqs =>
      val buffer = ChunkBuilder.make[Byte]()
      val it     = reqs.iterator

      while (it.hasNext) {
        val req = it.next()
        buffer ++= RespValue.Array(req.command).serialize
      }

      val bytes = buffer.result()

      connection
        .write(bytes)
        .mapError(RedisError.IOError(_))
        .tapBoth(
          e => ZIO.foreachDiscard(reqs)(_.promise.fail(e)),
          _ => ZIO.foreachDiscard(reqs)(req => resQueue.offer(req.promise))
        )
    }

  private def receive: IO[RedisError, Unit] =
    connection.read
      .mapError(RedisError.IOError(_))
      .via(RespValue.decoder)
      .collectSome
      .foreach(response => resQueue.take.flatMap(_.succeed(response)))

}

object SingleNodeExecutor {

  lazy val layer: ZLayer[RedisConnection, RedisError.IOError, RedisExecutor] =
    ZLayer.scoped {
      for {
        connection <- ZIO.service[RedisConnection]
        executor   <- create(connection)
      } yield executor
    }

  final case class Request(command: Chunk[RespValue.BulkString], promise: Promise[RedisError, RespValue])

  private final val True: Any => Boolean = _ => true

  private final val RequestQueueSize = 16

  private[redis] def create(connection: RedisConnection): URIO[Scope, SingleNodeExecutor] =
    for {
      reqQueue <- Queue.bounded[Request](RequestQueueSize)
      resQueue <- Queue.unbounded[Promise[RedisError, RespValue]]
      executor  = new SingleNodeExecutor(reqQueue, resQueue, connection)
      _        <- executor.run.forkScoped
      _        <- logScopeFinalizer(s"$executor Node Executor is closed")
    } yield executor

}
