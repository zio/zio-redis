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
import zio.clock.Clock
import zio.logging._

trait RedisExecutor {
  def execute(command: Chunk[RespValue.BulkString]): IO[RedisError, RespValue]
}

object RedisExecutor {

  lazy val live: ZLayer[Logging with Has[RedisConfig], RedisError.IOError, Has[RedisExecutor]] =
    ZLayer.identity[Logging] ++ ByteStream.live >>> StreamedExecutor

  lazy val local: ZLayer[Logging, RedisError.IOError, Has[RedisExecutor]] =
    ZLayer.identity[Logging] ++ ByteStream.default >>> StreamedExecutor

  lazy val test: URLayer[zio.random.Random with Clock, Has[RedisExecutor]] =
    TestExecutor.live

  private[this] final case class Request(command: Chunk[RespValue.BulkString], promise: Promise[RedisError, RespValue])

  private[this] final val True: Any => Boolean = _ => true

  private[this] final val RequestQueueSize = 16

  private[this] final val StreamedExecutor =
    ZLayer
      .fromServicesManaged[ByteStream.Service, Logger[String], Any, RedisError.IOError, RedisExecutor] {
        (byteStream, logging) =>
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
        .transduce(RespValue.Decoder)
        .foreach(response => resQueue.take.flatMap(_.succeed(response)))

  }
}
