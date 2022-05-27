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

import zio.{Clock, Random, _}

import java.io.IOException

trait RedisExecutor {
  def execute(command: Chunk[RespValue.BulkString]): IO[RedisError, RespValue]
}

object RedisExecutor {

  lazy val live: ZLayer[RedisConfig, RedisError.IOError, RedisExecutor] =
    ByteStream.live >>> StreamedExecutor

  lazy val local: ZLayer[Any, RedisError.IOError, RedisExecutor] =
    ByteStream.default >>> StreamedExecutor

  lazy val test: URLayer[Random with Clock, RedisExecutor] =
    TestExecutor.live

  private[this] final case class Request(command: Chunk[RespValue.BulkString], promise: Promise[RedisError, RespValue])

  private[this] final val True: Any => Boolean = _ => true

  private[this] final val RequestQueueSize = 16

  private[this] final val StreamedExecutor: ZLayer[ByteStream.Service, Nothing, Live] = {
    ZLayer.scoped {
      for {
        byteStream <- ZIO.service[ByteStream.Service]
        reqQueue <- Queue.bounded[Request](RequestQueueSize)
        resQueue <- Queue.unbounded[Promise[RedisError, RespValue]]
        live = new Live(reqQueue, resQueue, byteStream)
        _ <- live.run.forkScoped
      } yield live
    }
  }

  private[this] final class Live(
    reqQueue: Queue[Request],
    resQueue: Queue[Promise[RedisError, RespValue]],
    byteStream: ByteStream.Service
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

        byteStream
          .write(bytes)
          .mapError(RedisError.IOError)
          .tapBoth(
            e => ZIO.foreachDiscard(reqs)(_.promise.fail(e)),
            _ => ZIO.foreachDiscard(reqs)(req => resQueue.offer(req.promise))
          )
      }

    private def receive: IO[RedisError, Unit] = {
      byteStream.read
        .transduce(RespValue.Decoder)
        .foreach(response => resQueue.take.flatMap(_.succeed(response)))
        .mapError {
          // FIXME
          case io: IOException => RedisError.IOError(io)
          case rex: RedisError => rex
          case ex              => RedisError.ProtocolError(ex.getLocalizedMessage)
        }
    }
  }
}
