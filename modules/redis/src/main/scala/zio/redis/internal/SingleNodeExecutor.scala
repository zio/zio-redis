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
import zio.redis.Input.AuthInput
import zio.redis.Output.UnitOutput
import zio.redis.internal.SingleNodeExecutor._
import zio.redis.{RedisConfig, RedisError}

private[redis] final class SingleNodeExecutor private (
  config: RedisConfig,
  connection: RedisConnection,
  requests: Queue[Request],
  responses: Queue[Promise[RedisError, RespValue]],
  requestQueueSize: Int
) extends SingleNodeRunner
    with RedisExecutor {

  // onError try to reopen a new connection
  def execute(command: RespCommand): UIO[IO[RedisError, RespValue]] =
    Promise
      .make[RedisError, RespValue]
      .flatMap(promise => requests.offer(Request(command.args.map(_.value), promise)).as(promise.await))

  val auth: ZIO[Any, Nothing, Unit] = ZIO.foreachDiscard(config.auth) { auth =>
    val cmd = RedisCommand("AUTH", AuthInput, UnitOutput).resp(zio.redis.Auth(auth.username, auth.password))
    execute(cmd).unit
  }

  def onError(e: RedisError): UIO[Unit] =
    responses.takeAll.flatMap(ZIO.foreachDiscard(_)(_.fail(e))) *>
      connection.reconnect.zipRight(auth).catchAll { e =>
        ZIO.logError(s"Unable to reconnect to redis server: ${e}")
      }

  def send: IO[RedisError.IOError, Unit] =
    requests.takeBetween(1, requestQueueSize).flatMap { requests =>
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

  def local: ZLayer[Any, RedisError.IOError, RedisExecutor] =
    ZLayer.make[RedisExecutor](ZLayer.succeed(RedisConfig.Local), layer)

  def create: URIO[Scope & RedisConfig & RedisConnection, SingleNodeExecutor] =
    for {
      config          <- ZIO.service[RedisConfig]
      requestQueueSize = config.requestQueueSize
      connection      <- ZIO.service[RedisConnection]
      requests        <- Queue.bounded[Request](requestQueueSize)
      responses       <- Queue.unbounded[Promise[RedisError, RespValue]]
      executor         = new SingleNodeExecutor(config, connection, requests, responses, requestQueueSize)
      _               <- executor.run.forkScoped
      _               <- executor.auth
      _               <- logScopeFinalizer(s"$executor Node Executor is closed")
    } yield executor

  private final case class Request(command: Chunk[RespValue.BulkString], promise: Promise[RedisError, RespValue])

  private def makeLayer: ZLayer[RedisConnection & RedisConfig, RedisError.IOError, RedisExecutor] =
    ZLayer.scoped(create)
}
