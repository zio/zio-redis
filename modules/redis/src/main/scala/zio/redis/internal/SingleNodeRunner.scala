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

import zio.redis.RedisError
import zio.redis.internal.SingleNodeRunner.True
import zio.{IO, Schedule, ZIO}

private[redis] trait SingleNodeRunner {
  def send: IO[RedisError.IOError, Unit]

  def receive: IO[RedisError, Unit]

  def onError(e: RedisError): IO[RedisError, Unit]

  /**
   * Opens a connection to the server and launches receive operations. All failures are retried by opening a new
   * connection. Only exits by interruption or defect.
   */
  private[internal] final val run: IO[RedisError, AnyVal] =
    ZIO.logInfo(s"[REDIS-RUNNER] Starting sender and receiver fibers") *>
      (send.repeat(Schedule.forever) race receive)
        .tapError(e => ZIO.logError(s"[REDIS-RUNNER] Fiber failed, triggering error handling and reconnect: $e") *> onError(e))
        .retryWhile(True)
        .tapError(e => ZIO.logError(s"[REDIS-RUNNER] Executor fiber exiting permanently: $e"))
}

private[redis] object SingleNodeRunner {
  final val True: Any => Boolean = _ => true
}
