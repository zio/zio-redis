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

package zio.redis.commands

import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.schema.Schema

private[redis] trait HyperLogLog extends RedisEnvironment {
  import HyperLogLog._

  final def _pfAdd[K: Schema, V: Schema]: RedisCommand[(K, (V, List[V])), Boolean] =
    RedisCommand(
      PfAdd,
      Tuple2(ArbitraryKeyInput[K](), NonEmptyList(ArbitraryValueInput[V]())),
      BoolOutput,
      codec,
      executor
    )

  final def _pfCount[K: Schema]: RedisCommand[(K, List[K]), Long] =
    RedisCommand(PfCount, NonEmptyList(ArbitraryKeyInput[K]()), LongOutput, codec, executor)

  final def _pfMerge[K: Schema]: RedisCommand[(K, (K, List[K])), Unit] =
    RedisCommand(
      PfMerge,
      Tuple2(ArbitraryKeyInput[K](), NonEmptyList(ArbitraryKeyInput[K]())),
      UnitOutput,
      codec,
      executor
    )
}

private object HyperLogLog {
  final val PfAdd   = "PFADD"
  final val PfCount = "PFCOUNT"
  final val PfMerge = "PFMERGE"
}
