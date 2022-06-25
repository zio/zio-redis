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

package zio.redis.executor.node

import zio._
import zio.logging._
import zio.redis._
import zio.redis.executor.node.RedisNodeExecutorLive._
import zio.redis.executor.{RedisConnection, RedisExecutor}

final class RedisNodeExecutorLive(
  reqQueue: Queue[Request],
  resQueue: Queue[Promise[RedisError, RespValue]],
  connection: RedisConnection,
  logger: Logger[String]
) extends RedisExecutor {

  def execute(command: Chunk[RespValue.BulkString]): IO[RedisError, RespValue] =
    Promise
      .make[RedisError, RespValue]
      .flatMap(promise => reqQueue.offer(Request(command, promise)) *> promise.await)

  /**
   * Opens a connection to the server and launches send and receive operations. All failures are retried by opening a
   * new connection. Only exits by interruption or defect.
   */
  val run: IO[RedisError, Unit] =
    (send.forever race receive)
      .tapError(e => logger.warn(s"Reconnecting due to error: $e") *> drainWith(e))
      .retryWhile(True)
      .tapError(e => logger.error(s"Executor exiting: $e"))

  private def drainWith(e: RedisError): UIO[Unit] = resQueue.takeAll.flatMap(IO.foreach_(_)(_.fail(e)))

  private def send: IO[RedisError, Unit] =
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
        .mapError(RedisError.IOError)
        .tapBoth(
          e => IO.foreach_(reqs)(_.promise.fail(e)),
          _ => IO.foreach_(reqs)(req => resQueue.offer(req.promise))
        )
    }

  private def receive: IO[RedisError, Unit] =
    connection.read
      .mapError(RedisError.IOError)
      .transduce(RespValue.Decoder)
      .foreach(response => resQueue.take.flatMap(_.succeed(response)))

}

object RedisNodeExecutorLive {

  final val layer
    : ZLayer[Any with Has[RedisConnection] with Has[Logger[String]], RedisError.IOError, Has[RedisExecutor]] =
    ZLayer
      .fromServicesManaged[RedisConnection, Logger[String], Any, RedisError.IOError, RedisExecutor] {
        (connection, logger) =>
          create(connection, logger)
      }

  private final case class Request(command: Chunk[RespValue.BulkString], promise: Promise[RedisError, RespValue])

  private final val True: Any => Boolean = _ => true

  private final val RequestQueueSize = 16

  private[redis] def create(
    connection: RedisConnection,
    logger: Logger[String]
  ): ZManaged[Any, Nothing, RedisNodeExecutorLive] =
    for {
      reqQueue <- Queue.bounded[Request](RequestQueueSize).toManaged_
      resQueue <- Queue.unbounded[Promise[RedisError, RespValue]].toManaged_
      live      = new RedisNodeExecutorLive(reqQueue, resQueue, connection, logger)
      _        <- live.run.forkManaged
    } yield live
}
