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

private[redis] trait Sets extends RedisEnvironment {
  import Sets._

  final def _sAdd[K: Schema, M: Schema]: RedisCommand[(K, (M, List[M])), Long] =
    RedisCommand(
      SAdd,
      Tuple2(ArbitraryKeyInput[K](), NonEmptyList(ArbitraryValueInput[M]())),
      LongOutput,
      codec,
      executor
    )

  final def _sCard[K: Schema]: RedisCommand[K, Long] =
    RedisCommand(SCard, ArbitraryKeyInput[K](), LongOutput, codec, executor)

  final def _sDiff[K: Schema, R: Schema]: RedisCommand[(K, List[K]), Chunk[R]] =
    RedisCommand(SDiff, NonEmptyList(ArbitraryKeyInput[K]()), ChunkOutput(ArbitraryOutput[R]()), codec, executor)

  final def _sDiffStore[D: Schema, K: Schema]: RedisCommand[(D, (K, List[K])), Long] = RedisCommand(
    SDiffStore,
    Tuple2(ArbitraryValueInput[D](), NonEmptyList(ArbitraryKeyInput[K]())),
    LongOutput,
    codec,
    executor
  )

  final def _sInter[K: Schema, R: Schema]: RedisCommand[(K, List[K]), Chunk[R]] =
    RedisCommand(SInter, NonEmptyList(ArbitraryKeyInput[K]()), ChunkOutput(ArbitraryOutput[R]()), codec, executor)

  final def _sInterStore[D: Schema, K: Schema]: RedisCommand[(D, (K, List[K])), Long] = RedisCommand(
    SInterStore,
    Tuple2(ArbitraryValueInput[D](), NonEmptyList(ArbitraryKeyInput[K]())),
    LongOutput,
    codec,
    executor
  )

  final def _sIsMember[K: Schema, M: Schema]: RedisCommand[(K, M), Boolean] =
    RedisCommand(SIsMember, Tuple2(ArbitraryKeyInput[K](), ArbitraryValueInput[M]()), BoolOutput, codec, executor)

  final def _sMembers[K: Schema, R: Schema]: RedisCommand[K, Chunk[R]] =
    RedisCommand(SMembers, ArbitraryKeyInput[K](), ChunkOutput(ArbitraryOutput[R]()), codec, executor)

  final def _sMove[S: Schema, D: Schema, M: Schema]: RedisCommand[(S, D, M), Boolean] = RedisCommand(
    SMove,
    Tuple3(ArbitraryValueInput[S](), ArbitraryValueInput[D](), ArbitraryValueInput[M]()),
    BoolOutput,
    codec,
    executor
  )

  final def _sPop[K: Schema, R: Schema]: RedisCommand[(K, Option[Long]), Chunk[R]] = RedisCommand(
    SPop,
    Tuple2(ArbitraryKeyInput[K](), OptionalInput(LongInput)),
    MultiStringChunkOutput(ArbitraryOutput[R]()),
    codec,
    executor
  )

  final def _sRandMember[K: Schema, R: Schema]: RedisCommand[(K, Option[Long]), Chunk[R]] = RedisCommand(
    SRandMember,
    Tuple2(ArbitraryKeyInput[K](), OptionalInput(LongInput)),
    MultiStringChunkOutput(ArbitraryOutput[R]()),
    codec,
    executor
  )

  final def _sRem[K: Schema, M: Schema]: RedisCommand[(K, (M, List[M])), Long] =
    RedisCommand(
      SRem,
      Tuple2(ArbitraryKeyInput[K](), NonEmptyList(ArbitraryValueInput[M]())),
      LongOutput,
      codec,
      executor
    )

  final def _sScan[K: Schema, R: Schema]: RedisCommand[(K, Long, Option[Pattern], Option[Count]), (Long, Chunk[R])] =
    RedisCommand(
      SScan,
      Tuple4(ArbitraryKeyInput[K](), LongInput, OptionalInput(PatternInput), OptionalInput(CountInput)),
      Tuple2Output(MultiStringOutput.map(_.toLong), ChunkOutput(ArbitraryOutput[R]())),
      codec,
      executor
    )

  final def _sUnion[K: Schema, R: Schema]: RedisCommand[(K, List[K]), Chunk[R]] =
    RedisCommand(SUnion, NonEmptyList(ArbitraryKeyInput[K]()), ChunkOutput(ArbitraryOutput[R]()), codec, executor)

  final def _sUnionStore[D: Schema, K: Schema]: RedisCommand[(D, (K, List[K])), Long] = RedisCommand(
    SUnionStore,
    Tuple2(ArbitraryValueInput[D](), NonEmptyList(ArbitraryKeyInput[K]())),
    LongOutput,
    codec,
    executor
  )
}

private object Sets {
  final val SAdd        = "SADD"
  final val SCard       = "SCARD"
  final val SDiff       = "SDIFF"
  final val SDiffStore  = "SDIFFSTORE"
  final val SInter      = "SINTER"
  final val SInterStore = "SINTERSTORE"
  final val SIsMember   = "SISMEMBER"
  final val SMembers    = "SMEMBERS"
  final val SMove       = "SMOVE"
  final val SPop        = "SPOP"
  final val SRandMember = "SRANDMEMBER"
  final val SRem        = "SREM"
  final val SScan       = "SSCAN"
  final val SUnion      = "SUNION"
  final val SUnionStore = "SUNIONSTORE"
}
