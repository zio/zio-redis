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

package zio.redis.internal

import zio._
import zio.redis.internal.SingleNodeExecutor._
import zio.redis.{RedisConfig, RedisError}

private[redis] final class SingleNodeExecutor private (
  connection: RedisConnection,
  requests: Queue[Request],
  responses: Queue[Promise[RedisError, RespValue]]
) extends SingleNodeRunner
    with RedisExecutor {

  // TODO NodeExecutor doesn't throw connection errors, timeout errors, it is hanging forever
  def execute(command: RespCommand): IO[RedisError, RespValue] =
    Promise
      .make[RedisError, RespValue]
      .flatMap(promise => requests.offer(Request(command.args.map(_.value), promise)) *> promise.await)

  def onError(e: RedisError): UIO[Unit] = responses.takeAll.flatMap(ZIO.foreachDiscard(_)(_.fail(e)))

  def send: IO[RedisError.IOError, Unit] =
    requests.takeAll.flatMap { requests =>
      val bytes =
        requests
          .foldLeft(new ChunkBuilder.Byte())((builder, req) => builder ++= RespValue.Array(req.command).asBytes)
          .result()

      connection
        .write(bytes)
        .mapError(RedisError.IOError(_))
        .tapBoth(
          e => ZIO.foreachDiscard(requests)(_.promise.fail(e)),
          _ => ZIO.foreachDiscard(requests)(req => responses.offer(req.promise))
        )
        .unit
    }

  def receive: IO[RedisError, Unit] =
    connection.read
      .mapError(RedisError.IOError(_))
      .via(RespValue.Decoder)
      .collectSome
      .foreach(response => responses.take.flatMap(_.succeed(response)))

}

private[redis] object SingleNodeExecutor {
  lazy val layer: ZLayer[RedisConfig, RedisError.IOError, RedisExecutor] =
    RedisConnection.layer >>> makeLayer

  lazy val local: ZLayer[Any, RedisError.IOError, RedisExecutor] =
    RedisConnection.local >>> makeLayer

  def create(connection: RedisConnection): URIO[Scope, SingleNodeExecutor] =
    for {
      requests  <- Queue.bounded[Request](RequestQueueSize)
      responses <- Queue.unbounded[Promise[RedisError, RespValue]]
      executor   = new SingleNodeExecutor(connection, requests, responses)
      _         <- executor.run.forkScoped
      _         <- logScopeFinalizer(s"$executor Node Executor is closed")
    } yield executor

  private final case class Request(command: Chunk[RespValue.BulkString], promise: Promise[RedisError, RespValue])

  private def makeLayer: ZLayer[RedisConnection, RedisError.IOError, RedisExecutor] =
    ZLayer.scoped(ZIO.serviceWithZIO[RedisConnection](create))
}
