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

package zio

package object redis
    extends options.Connection
    with options.Geo
    with options.Keys
    with options.Shared
    with options.SortedSets
    with options.Strings
    with options.Lists
    with options.Streams
    with options.Scripting {

  /**
   * The identity type constructor: `Id[A]` is `A`. `subscribeSingle` uses it, so its stream emits bare values.
   */
  type Id[+A] = A

  /**
   * The asynchronous client. A command returns `UIO[IO[RedisError, A]]`. Running the outer effect queues the command
   * for sending, and the inner effect waits for its reply, so several commands can be sent before their replies are
   * read.
   *
   * {{{
   * import zio._
   * import zio.redis._
   *
   * def start(redis: AsyncRedis): UIO[IO[RedisError, Option[String]]] =
   *   redis.get("key").returning[String]
   * }}}
   */
  type AsyncRedis = GenRedis[GenRedis.Async]

  /**
   * The synchronous client. A command returns `IO[RedisError, A]`, which sends the command and waits for its reply.
   *
   * {{{
   * import zio._
   * import zio.redis._
   *
   * def read(redis: Redis): IO[RedisError, Option[String]] =
   *   redis.get("key").returning[String]
   * }}}
   */
  type Redis = GenRedis[GenRedis.Sync]
}
