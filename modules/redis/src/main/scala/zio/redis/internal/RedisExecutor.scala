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
import zio.{IO, UIO}

private[redis] trait RedisExecutor {
  def execute(command: RespCommand): UIO[IO[RedisError, RespValue]]
}

object RedisExecutor {
  type Async[+A] = IO[RedisError, IO[RedisError, A]]
  private[redis] def async[A](io: UIO[IO[RedisError, A]]) = io
  type Sync[+A] = IO[RedisError, A]
  private[redis] def sync[A](io: UIO[IO[RedisError, A]]) = io.flatten

}
