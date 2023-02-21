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

import java.time.Instant

private[redis] trait Strings extends RedisEnvironment {
  import Strings._

  final def _append[K: Schema, V: Schema]: RedisCommand[(K, V), Long] =
    RedisCommand(Append, Tuple2(ArbitraryKeyInput[K](), ArbitraryValueInput[V]()), LongOutput, codec, executor)

  final def _bitCount[K: Schema]: RedisCommand[(K, Option[Range]), Long] =
    RedisCommand(BitCount, Tuple2(ArbitraryKeyInput[K](), OptionalInput(RangeInput)), LongOutput, codec, executor)

  final def _bitField[K: Schema]: RedisCommand[(K, (BitFieldCommand, List[BitFieldCommand])), Chunk[Option[Long]]] =
    RedisCommand(
      BitField,
      Tuple2(ArbitraryKeyInput[K](), NonEmptyList(BitFieldCommandInput)),
      ChunkOutput(OptionalOutput(LongOutput)),
      codec,
      executor
    )

  final def _bitOp[D: Schema, S: Schema]: RedisCommand[(BitOperation, D, (S, List[S])), Long] =
    RedisCommand(
      BitOp,
      Tuple3(BitOperationInput, ArbitraryValueInput[D](), NonEmptyList(ArbitraryValueInput[S]())),
      LongOutput,
      codec,
      executor
    )

  final def _bitPos[K: Schema]: RedisCommand[(K, Boolean, Option[BitPosRange]), Long] = RedisCommand(
    BitPos,
    Tuple3(ArbitraryKeyInput[K](), BoolInput, OptionalInput(BitPosRangeInput)),
    LongOutput,
    codec,
    executor
  )

  final def _decr[K: Schema]: RedisCommand[K, Long] =
    RedisCommand(Decr, ArbitraryKeyInput[K](), LongOutput, codec, executor)

  final def _decrBy[K: Schema]: RedisCommand[(K, Long), Long] =
    RedisCommand(DecrBy, Tuple2(ArbitraryKeyInput[K](), LongInput), LongOutput, codec, executor)

  final def _get[K: Schema, R: Schema]: RedisCommand[K, Option[R]] =
    RedisCommand(Get, ArbitraryKeyInput[K](), OptionalOutput(ArbitraryOutput[R]()), codec, executor)

  final def _getBit[K: Schema]: RedisCommand[(K, Long), Long] =
    RedisCommand(GetBit, Tuple2(ArbitraryKeyInput[K](), LongInput), LongOutput, codec, executor)

  final def _getRange[K: Schema, R: Schema]: RedisCommand[(K, Range), Option[R]] = RedisCommand(
    GetRange,
    Tuple2(ArbitraryKeyInput[K](), RangeInput),
    OptionalOutput(ArbitraryOutput[R]()),
    codec,
    executor
  )

  final def _getSet[K: Schema, V: Schema, R: Schema]: RedisCommand[(K, V), Option[R]] = RedisCommand(
    GetSet,
    Tuple2(ArbitraryKeyInput[K](), ArbitraryValueInput[V]()),
    OptionalOutput(ArbitraryOutput[R]()),
    codec,
    executor
  )

  final def _getDel[K: Schema, R: Schema]: RedisCommand[K, Option[R]] =
    RedisCommand(GetDel, ArbitraryKeyInput[K](), OptionalOutput(ArbitraryOutput[R]()), codec, executor)

  final def _getEx[K: Schema, R: Schema]: RedisCommand[(K, Expire, Duration), Option[R]] =
    RedisCommand(GetEx, GetExInput[K](), OptionalOutput(ArbitraryOutput[R]()), codec, executor)

  final def _getExAt[K: Schema, R: Schema]: RedisCommand[(K, ExpiredAt, Instant), Option[R]] =
    RedisCommand(GetEx, GetExAtInput[K](), OptionalOutput(ArbitraryOutput[R]()), codec, executor)

  final def _getExDel[K: Schema, R: Schema]: RedisCommand[(K, Boolean), Option[R]] =
    RedisCommand(GetEx, GetExPersistInput[K](), OptionalOutput(ArbitraryOutput[R]()), codec, executor)

  final def _incr[K: Schema]: RedisCommand[K, Long] =
    RedisCommand(Incr, ArbitraryKeyInput[K](), LongOutput, codec, executor)

  final def _incrBy[K: Schema]: RedisCommand[(K, Long), Long] =
    RedisCommand(IncrBy, Tuple2(ArbitraryKeyInput[K](), LongInput), LongOutput, codec, executor)

  final def _incrByFloat[K: Schema]: RedisCommand[(K, Double), Double] =
    RedisCommand(IncrByFloat, Tuple2(ArbitraryKeyInput[K](), DoubleInput), DoubleOutput, codec, executor)

  final def _mGet[K: Schema, V: Schema]: RedisCommand[(K, List[K]), Chunk[Option[V]]] = RedisCommand(
    MGet,
    NonEmptyList(ArbitraryKeyInput[K]()),
    ChunkOutput(OptionalOutput(ArbitraryOutput[V]())),
    codec,
    executor
  )

  final def _mSet[K: Schema, V: Schema]: RedisCommand[((K, V), List[(K, V)]), Unit] =
    RedisCommand(
      MSet,
      NonEmptyList(Tuple2(ArbitraryKeyInput[K](), ArbitraryValueInput[V]())),
      UnitOutput,
      codec,
      executor
    )

  final def _mSetNx[K: Schema, V: Schema]: RedisCommand[((K, V), List[(K, V)]), Boolean] =
    RedisCommand(
      MSetNx,
      NonEmptyList(Tuple2(ArbitraryKeyInput[K](), ArbitraryValueInput[V]())),
      BoolOutput,
      codec,
      executor
    )

  final def _pSetEx[K: Schema, V: Schema]: RedisCommand[(K, Duration, V), Unit] = RedisCommand(
    PSetEx,
    Tuple3(ArbitraryKeyInput[K](), DurationMillisecondsInput, ArbitraryValueInput[V]()),
    UnitOutput,
    codec,
    executor
  )

  final def _set[K: Schema, V: Schema]
    : RedisCommand[(K, V, Option[Duration], Option[Update], Option[KeepTtl]), Boolean] =
    RedisCommand(
      Set,
      Tuple5(
        ArbitraryKeyInput[K](),
        ArbitraryValueInput[V](),
        OptionalInput(DurationTtlInput),
        OptionalInput(UpdateInput),
        OptionalInput(KeepTtlInput)
      ),
      SetOutput,
      codec,
      executor
    )

  final def _setBit[K: Schema]: RedisCommand[(K, Long, Boolean), Boolean] =
    RedisCommand(SetBit, Tuple3(ArbitraryKeyInput[K](), LongInput, BoolInput), BoolOutput, codec, executor)

  final def _setEx[K: Schema, V: Schema]: RedisCommand[(K, Duration, V), Unit] = RedisCommand(
    SetEx,
    Tuple3(ArbitraryKeyInput[K](), DurationSecondsInput, ArbitraryValueInput[V]()),
    UnitOutput,
    codec,
    executor
  )

  final def _setNx[K: Schema, V: Schema]: RedisCommand[(K, V), Boolean] =
    RedisCommand(SetNx, Tuple2(ArbitraryKeyInput[K](), ArbitraryValueInput[V]()), BoolOutput, codec, executor)

  final def _setRange[K: Schema, V: Schema]: RedisCommand[(K, Long, V), Long] = RedisCommand(
    SetRange,
    Tuple3(ArbitraryKeyInput[K](), LongInput, ArbitraryValueInput[V]()),
    LongOutput,
    codec,
    executor
  )

  final def _strLen[K: Schema]: RedisCommand[K, Long] =
    RedisCommand(StrLen, ArbitraryKeyInput[K](), LongOutput, codec, executor)

  final def _strAlgoLcs[K: Schema]: RedisCommand[(String, K, K, Option[StrAlgoLcsQueryType]), LcsOutput] = RedisCommand(
    StrAlgoLcs,
    Tuple4(
      ArbitraryValueInput[String](),
      ArbitraryKeyInput[K](),
      ArbitraryKeyInput[K](),
      OptionalInput(StralgoLcsQueryTypeInput)
    ),
    StrAlgoLcsOutput,
    codec,
    executor
  )
}

private object Strings {
  final val Append      = "APPEND"
  final val BitCount    = "BITCOUNT"
  final val BitField    = "BITFIELD"
  final val BitOp       = "BITOP"
  final val BitPos      = "BITPOS"
  final val Decr        = "DECR"
  final val DecrBy      = "DECRBY"
  final val Get         = "GET"
  final val GetBit      = "GETBIT"
  final val GetRange    = "GETRANGE"
  final val GetSet      = "GETSET"
  final val Incr        = "INCR"
  final val IncrBy      = "INCRBY"
  final val IncrByFloat = "INCRBYFLOAT"
  final val MGet        = "MGET"
  final val MSet        = "MSET"
  final val MSetNx      = "MSETNX"
  final val PSetEx      = "PSETEX"
  final val Set         = "SET"
  final val SetBit      = "SETBIT"
  final val SetEx       = "SETEX"
  final val SetNx       = "SETNX"
  final val SetRange    = "SETRANGE"
  final val StrLen      = "STRLEN"
  final val StrAlgoLcs  = "STRALGO LCS"
  final val GetDel      = "GETDEL"
  final val GetEx       = "GETEX"
}
