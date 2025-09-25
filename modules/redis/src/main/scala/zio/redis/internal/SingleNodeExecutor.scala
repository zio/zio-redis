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
  responses: Queue[Promise[RedisError, RespValue]],
  requestQueueSize: Int
) extends SingleNodeRunner
    with RedisExecutor {

  // TODO NodeExecutor doesn't throw connection errors, timeout errors, it is hanging forever
  def execute(command: RespCommand): UIO[IO[RedisError, RespValue]] =
    for {
      promiseId <- Random.nextUUID.map(_.toString.take(8))
      _         <- ZIO.logDebug(s"[REDIS-EXEC-$promiseId] Creating promise for command: ${command.args.head.value}")
      promise   <- Promise.make[RedisError, RespValue]
      _         <- requests.size.flatMap(size => ZIO.logDebug(s"[REDIS-EXEC-$promiseId] Current requests queue size: $size/$requestQueueSize"))
      _         <- requests.offer(Request(command.args.map(_.value), promise, promiseId))
      _         <- requests.size.flatMap(size => ZIO.logDebug(s"[REDIS-EXEC-$promiseId] After offer, requests queue size: $size/$requestQueueSize"))
      _         <- ZIO.logDebug(s"[REDIS-EXEC-$promiseId] Request queued, starting promise.await")
      result     = promise.await.tap(_ => ZIO.logDebug(s"[REDIS-EXEC-$promiseId] Promise completed successfully"))
                    .tapError(e => ZIO.logWarning(s"[REDIS-EXEC-$promiseId] Promise failed: $e"))
    } yield result

  def onError(e: RedisError): UIO[Unit] =
    for {
      _                <- ZIO.logWarning(s"[REDIS-ERROR] SingleNodeExecutor error occurred: $e")
      pendingResponses <- responses.takeAll
      _                <- ZIO.logWarning(s"[REDIS-ERROR] Failing ${pendingResponses.length} pending responses")
      _                <- ZIO.foreachDiscard(pendingResponses)(_.fail(e))
      _                <- ZIO.logWarning(s"[REDIS-ERROR] Error handling completed")
    } yield ()

  def send: IO[RedisError.IOError, Unit] =
    for {
      queueSizeBefore <- requests.size
      _               <- ZIO.logDebug(s"[REDIS-SEND] About to take requests from queue (current size: $queueSizeBefore)")
      requestsToSend  <- requests.takeBetween(1, requestQueueSize)
      queueSizeAfter  <- requests.size
      _               <- ZIO.logDebug(s"[REDIS-SEND] Processing ${requestsToSend.length} requests (queue size: $queueSizeBefore -> $queueSizeAfter): ${requestsToSend.map(_.promiseId).mkString(",")}")
      bytes           = requestsToSend
                          .foldLeft(new ChunkBuilder.Byte())((builder, req) => builder ++= RespValue.Array(req.command).asBytes)
                          .result()
      _              <- ZIO.logDebug(s"[REDIS-SEND] Writing ${bytes.length} bytes to connection")
      result         <- connection
                          .write(bytes)
                          .mapError(RedisError.IOError(_))
                          .tapBoth(
                            e => ZIO.logError(s"[REDIS-SEND] Write failed, failing ${requestsToSend.length} promises: $e") *>
                                 ZIO.foreachDiscard(requestsToSend)(_.promise.fail(e)),
                            _ => responses.size.flatMap(responseQueueSize =>
                                   ZIO.logDebug(s"[REDIS-SEND] Write successful, moving ${requestsToSend.length} promises to response queue (current response queue size: $responseQueueSize)")
                                 ) *>
                                 ZIO.foreachDiscard(requestsToSend)(req =>
                                   responses.offer(req.promise).tap(_ => ZIO.logDebug(s"[REDIS-SEND] Promise ${req.promiseId} moved to response queue"))
                                 )
                          )
                          .unit
    } yield result

  def receive: IO[RedisError, Unit] =
    connection.read
      .mapError(RedisError.IOError(_))
      .via(RespValue.Decoder)
      .collectSome
      .foreach { response =>
        responses.size.flatMap(responseQueueSize =>
          ZIO.logDebug(s"[REDIS-RECV] Received response, taking promise from queue (current response queue size: $responseQueueSize)")
        ) *>
        responses.take.tap(_ => ZIO.logDebug(s"[REDIS-RECV] Completing promise with response"))
                      .flatMap(_.succeed(response))
                      .tap(_ => ZIO.logDebug(s"[REDIS-RECV] Promise completed successfully")) *>
        responses.size.flatMap(responseQueueSize =>
          ZIO.logDebug(s"[REDIS-RECV] After completion, response queue size: $responseQueueSize")
        )
      }

}

private[redis] object SingleNodeExecutor {
  lazy val layer: ZLayer[RedisConfig, RedisError.IOError, RedisExecutor] =
    RedisConnection.layer >>> makeLayer

  def local: ZLayer[Any, RedisError.IOError, RedisExecutor] =
    ZLayer.make[RedisExecutor](ZLayer.succeed(RedisConfig.Local), layer)

  def create(connection: RedisConnection): URIO[Scope & RedisConfig, SingleNodeExecutor] =
    for {
      requestQueueSize <- ZIO.serviceWith[RedisConfig](_.requestQueueSize)
      _                <- ZIO.logInfo(s"[REDIS-INIT] Creating SingleNodeExecutor with queue size: $requestQueueSize")
      requests         <- Queue.bounded[Request](requestQueueSize)
      responses        <- Queue.unbounded[Promise[RedisError, RespValue]]
      executor          = new SingleNodeExecutor(connection, requests, responses, requestQueueSize)
      _                <- ZIO.logInfo(s"[REDIS-INIT] Starting executor run fiber")
      _                <- executor.run.forkScoped
      _                <- ZIO.logInfo(s"[REDIS-INIT] SingleNodeExecutor initialized successfully")
      _                <- logScopeFinalizer(s"$executor Node Executor is closed")
    } yield executor

  private final case class Request(
    command: Chunk[RespValue.BulkString],
    promise: Promise[RedisError, RespValue],
    promiseId: String
  )

  private def makeLayer: ZLayer[RedisConnection & RedisConfig, RedisError.IOError, RedisExecutor] =
    ZLayer.scoped(ZIO.serviceWithZIO[RedisConnection](create))
}
