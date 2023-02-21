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
import zio.{Chunk, Duration}

private[redis] trait Lists extends RedisEnvironment {
  import Lists._

  final def _brPopLPush[S: Schema, D: Schema, V: Schema]: RedisCommand[(S, D, Duration), Option[V]] = RedisCommand(
    BrPopLPush,
    Tuple3(ArbitraryValueInput[S](), ArbitraryValueInput[D](), DurationSecondsInput),
    OptionalOutput(ArbitraryOutput[V]()),
    codec,
    executor
  )

  final def _lIndex[K: Schema, V: Schema]: RedisCommand[(K, Long), Option[V]] =
    RedisCommand(
      LIndex,
      Tuple2(ArbitraryKeyInput[K](), LongInput),
      OptionalOutput(ArbitraryOutput[V]()),
      codec,
      executor
    )

  final def _lLen[K: Schema]: RedisCommand[K, Long] =
    RedisCommand(LLen, ArbitraryKeyInput[K](), LongOutput, codec, executor)

  final def _lPop[K: Schema, V: Schema]: RedisCommand[K, Option[V]] =
    RedisCommand(LPop, ArbitraryKeyInput[K](), OptionalOutput(ArbitraryOutput[V]()), codec, executor)

  final def _lPush[K: Schema, V: Schema]: RedisCommand[(K, (V, List[V])), Long] =
    RedisCommand(
      LPush,
      Tuple2(ArbitraryKeyInput[K](), NonEmptyList(ArbitraryValueInput[V]())),
      LongOutput,
      codec,
      executor
    )

  final def _lPushX[K: Schema, V: Schema]: RedisCommand[(K, (V, List[V])), Long] =
    RedisCommand(
      LPushX,
      Tuple2(ArbitraryKeyInput[K](), NonEmptyList(ArbitraryValueInput[V]())),
      LongOutput,
      codec,
      executor
    )

  final def _lRange[K: Schema, V: Schema]: RedisCommand[(K, Range), Chunk[V]] =
    RedisCommand(LRange, Tuple2(ArbitraryKeyInput[K](), RangeInput), ChunkOutput(ArbitraryOutput[V]()), codec, executor)

  final def _lRem[K: Schema]: RedisCommand[(K, Long, String), Long] =
    RedisCommand(LRem, Tuple3(ArbitraryKeyInput[K](), LongInput, StringInput), LongOutput, codec, executor)

  final def _lSet[K: Schema, V: Schema]: RedisCommand[(K, Long, V), Unit] =
    RedisCommand(LSet, Tuple3(ArbitraryKeyInput[K](), LongInput, ArbitraryValueInput[V]()), UnitOutput, codec, executor)

  final def _lTrim[K: Schema]: RedisCommand[(K, Range), Unit] =
    RedisCommand(LTrim, Tuple2(ArbitraryKeyInput[K](), RangeInput), UnitOutput, codec, executor)

  final def _rPop[K: Schema, V: Schema]: RedisCommand[K, Option[V]] =
    RedisCommand(RPop, ArbitraryKeyInput[K](), OptionalOutput(ArbitraryOutput[V]()), codec, executor)

  final def _rPopLPush[S: Schema, D: Schema, V: Schema]: RedisCommand[(S, D), Option[V]] = RedisCommand(
    RPopLPush,
    Tuple2(ArbitraryValueInput[S](), ArbitraryValueInput[D]()),
    OptionalOutput(ArbitraryOutput[V]()),
    codec,
    executor
  )

  final def _rPush[K: Schema, V: Schema]: RedisCommand[(K, (V, List[V])), Long] =
    RedisCommand(
      RPush,
      Tuple2(ArbitraryKeyInput[K](), NonEmptyList(ArbitraryValueInput[V]())),
      LongOutput,
      codec,
      executor
    )

  final def _rPushX[K: Schema, V: Schema]: RedisCommand[(K, (V, List[V])), Long] =
    RedisCommand(
      RPushX,
      Tuple2(ArbitraryKeyInput[K](), NonEmptyList(ArbitraryValueInput[V]())),
      LongOutput,
      codec,
      executor
    )

  final def _blPop[K: Schema, V: Schema]: RedisCommand[((K, List[K]), Duration), Option[(K, V)]] = RedisCommand(
    BlPop,
    Tuple2(NonEmptyList(ArbitraryKeyInput[K]()), DurationSecondsInput),
    OptionalOutput(Tuple2Output(ArbitraryOutput[K](), ArbitraryOutput[V]())),
    codec,
    executor
  )

  final def _brPop[K: Schema, V: Schema]: RedisCommand[((K, List[K]), Duration), Option[(K, V)]] = RedisCommand(
    BrPop,
    Tuple2(NonEmptyList(ArbitraryKeyInput[K]()), DurationSecondsInput),
    OptionalOutput(Tuple2Output(ArbitraryOutput[K](), ArbitraryOutput[V]())),
    codec,
    executor
  )

  final def _lInsert[K: Schema, V: Schema]: RedisCommand[(K, Position, V, V), Long] = RedisCommand(
    LInsert,
    Tuple4(ArbitraryKeyInput[K](), PositionInput, ArbitraryValueInput[V](), ArbitraryValueInput[V]()),
    LongOutput,
    codec,
    executor
  )

  final def _lMove[S: Schema, D: Schema, V: Schema]: RedisCommand[(S, D, Side, Side), Option[V]] = RedisCommand(
    LMove,
    Tuple4(ArbitraryValueInput[S](), ArbitraryValueInput[D](), SideInput, SideInput),
    OptionalOutput(ArbitraryOutput[V]()),
    codec,
    executor
  )

  final def _blMove[S: Schema, D: Schema, V: Schema]
    : RedisCommand[(S, D, zio.redis.Side, zio.redis.Side, Duration), Option[V]] = RedisCommand(
    BlMove,
    Tuple5(ArbitraryValueInput[S](), ArbitraryValueInput[D](), SideInput, SideInput, DurationSecondsInput),
    OptionalOutput(ArbitraryOutput[V]()),
    codec,
    executor
  )

  final def _lPos[K: Schema, V: Schema]: RedisCommand[(K, V, Option[Rank], Option[ListMaxLen]), Option[Long]] =
    RedisCommand(
      LPos,
      Tuple4(
        ArbitraryKeyInput[K](),
        ArbitraryValueInput[V](),
        OptionalInput(RankInput),
        OptionalInput(ListMaxLenInput)
      ),
      OptionalOutput(LongOutput),
      codec,
      executor
    )

  final def _lPosCount[K: Schema, V: Schema]
    : RedisCommand[(K, V, Count, Option[zio.redis.Rank], Option[zio.redis.ListMaxLen]), Chunk[Long]] = RedisCommand(
    LPos,
    Tuple5(
      ArbitraryKeyInput[K](),
      ArbitraryValueInput[V](),
      CountInput,
      OptionalInput(RankInput),
      OptionalInput(ListMaxLenInput)
    ),
    ChunkOutput(LongOutput),
    codec,
    executor
  )
}

private object Lists {
  final val BrPopLPush = "BRPOPLPUSH"
  final val LIndex     = "LINDEX"
  final val LLen       = "LLEN"
  final val LPop       = "LPOP"
  final val LPush      = "LPUSH"
  final val LPushX     = "LPUSHX"
  final val LRange     = "LRANGE"
  final val LRem       = "LREM"
  final val LSet       = "LSET"
  final val LTrim      = "LTRIM"
  final val RPop       = "RPOP"
  final val RPopLPush  = "RPOPLPUSH"
  final val RPush      = "RPUSH"
  final val RPushX     = "RPUSHX"
  final val BlPop      = "BLPOP"
  final val BrPop      = "BRPOP"
  final val LInsert    = "LINSERT"
  final val LMove      = "LMOVE"
  final val BlMove     = "BLMOVE"
  final val LPos       = "LPOS"
}
