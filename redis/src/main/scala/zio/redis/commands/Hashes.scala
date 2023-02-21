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

import zio.Chunk
import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.schema.Schema

private[redis] trait Hashes extends RedisEnvironment {
  import Hashes._

  final def _hDel[K: Schema, F: Schema]: RedisCommand[(K, (F, List[F])), Long] =
    RedisCommand(
      HDel,
      Tuple2(ArbitraryKeyInput[K](), NonEmptyList(ArbitraryValueInput[F]())),
      LongOutput,
      codec,
      executor
    )

  final def _hExists[K: Schema, F: Schema]: RedisCommand[(K, F), Boolean] =
    RedisCommand(HExists, Tuple2(ArbitraryKeyInput[K](), ArbitraryValueInput[F]()), BoolOutput, codec, executor)

  final def _hGet[K: Schema, F: Schema, V: Schema]: RedisCommand[(K, F), Option[V]] = RedisCommand(
    HGet,
    Tuple2(ArbitraryKeyInput[K](), ArbitraryValueInput[F]()),
    OptionalOutput(ArbitraryOutput[V]()),
    codec,
    executor
  )

  final def _hGetAll[K: Schema, F: Schema, V: Schema]: RedisCommand[K, Map[F, V]] = RedisCommand(
    HGetAll,
    ArbitraryKeyInput[K](),
    KeyValueOutput(ArbitraryOutput[F](), ArbitraryOutput[V]()),
    codec,
    executor
  )

  final def _hIncrBy[K: Schema, F: Schema]: RedisCommand[(K, F, Long), Long] =
    RedisCommand(
      HIncrBy,
      Tuple3(ArbitraryKeyInput[K](), ArbitraryValueInput[F](), LongInput),
      LongOutput,
      codec,
      executor
    )

  final def _hIncrByFloat[K: Schema, F: Schema]: RedisCommand[(K, F, Double), Double] = RedisCommand(
    HIncrByFloat,
    Tuple3(ArbitraryKeyInput[K](), ArbitraryValueInput[F](), DoubleInput),
    DoubleOutput,
    codec,
    executor
  )

  final def _hKeys[K: Schema, F: Schema]: RedisCommand[K, Chunk[F]] =
    RedisCommand(HKeys, ArbitraryKeyInput[K](), ChunkOutput(ArbitraryOutput[F]()), codec, executor)

  final def _hLen[K: Schema]: RedisCommand[K, Long] =
    RedisCommand(HLen, ArbitraryKeyInput[K](), LongOutput, codec, executor)

  final def _hmGet[K: Schema, F: Schema, V: Schema]: RedisCommand[(K, (F, List[F])), Chunk[Option[V]]] = RedisCommand(
    HmGet,
    Tuple2(ArbitraryKeyInput[K](), NonEmptyList(ArbitraryValueInput[F]())),
    ChunkOutput(OptionalOutput(ArbitraryOutput[V]())),
    codec,
    executor
  )

  final def _hmSet[K: Schema, F: Schema, V: Schema]: RedisCommand[(K, ((F, V), List[(F, V)])), Unit] = RedisCommand(
    HmSet,
    Tuple2(ArbitraryKeyInput[K](), NonEmptyList(Tuple2(ArbitraryValueInput[F](), ArbitraryValueInput[V]()))),
    UnitOutput,
    codec,
    executor
  )

  final def _hScan[K: Schema, F: Schema, V: Schema]
    : RedisCommand[(K, Long, Option[Pattern], Option[Count]), (Long, Chunk[(F, V)])] = RedisCommand(
    HScan,
    Tuple4(ArbitraryKeyInput[K](), LongInput, OptionalInput(PatternInput), OptionalInput(CountInput)),
    Tuple2Output(ArbitraryOutput[Long](), ChunkTuple2Output(ArbitraryOutput[F](), ArbitraryOutput[V]())),
    codec,
    executor
  )

  final def _hSet[K: Schema, F: Schema, V: Schema]: RedisCommand[(K, ((F, V), List[(F, V)])), Long] = RedisCommand(
    HSet,
    Tuple2(ArbitraryKeyInput[K](), NonEmptyList(Tuple2(ArbitraryValueInput[F](), ArbitraryValueInput[V]()))),
    LongOutput,
    codec,
    executor
  )

  final def _hSetNx[K: Schema, F: Schema, V: Schema]: RedisCommand[(K, F, V), Boolean] = RedisCommand(
    HSetNx,
    Tuple3(ArbitraryKeyInput[K](), ArbitraryValueInput[F](), ArbitraryValueInput[V]()),
    BoolOutput,
    codec,
    executor
  )

  final def _hStrLen[K: Schema, F: Schema]: RedisCommand[(K, F), Long] =
    RedisCommand(HStrLen, Tuple2(ArbitraryKeyInput[K](), ArbitraryValueInput[F]()), LongOutput, codec, executor)

  final def _hVals[K: Schema, V: Schema]: RedisCommand[K, Chunk[V]] =
    RedisCommand(HVals, ArbitraryKeyInput[K](), ChunkOutput(ArbitraryOutput[V]()), codec, executor)

  final def _hRandField[K: Schema, V: Schema]: RedisCommand[K, Option[V]] =
    RedisCommand(HRandField, ArbitraryKeyInput[K](), OptionalOutput(ArbitraryOutput[V]()), codec, executor)

  final def _hRandFieldWithCount[K: Schema, V: Schema]: RedisCommand[(K, Long, Option[String]), Chunk[V]] =
    RedisCommand(
      HRandField,
      Tuple3(ArbitraryKeyInput[K](), LongInput, OptionalInput(StringInput)),
      ChunkOutput(ArbitraryOutput[V]()),
      codec,
      executor
    )
}

private object Hashes {
  final val HDel         = "HDEL"
  final val HExists      = "HEXISTS"
  final val HGet         = "HGET"
  final val HGetAll      = "HGETALL"
  final val HIncrBy      = "HINCRBY"
  final val HIncrByFloat = "HINCRBYFLOAT"
  final val HKeys        = "HKEYS"
  final val HLen         = "HLEN"
  final val HmGet        = "HMGET"
  final val HmSet        = "HMSET"
  final val HScan        = "HSCAN"
  final val HSet         = "HSET"
  final val HSetNx       = "HSETNX"
  final val HStrLen      = "HSTRLEN"
  final val HVals        = "HVALS"
  final val HRandField   = "HRANDFIELD"
}
