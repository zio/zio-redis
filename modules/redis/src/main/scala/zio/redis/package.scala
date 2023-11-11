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

import zio.redis.internal.RedisExecutor

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

  type Id[+A] = A

  type Redis      = GenRedis[RedisExecutor.Sync]
  type AsyncRedis = GenRedis[RedisExecutor.Async]
}
