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

private[redis] trait Keys extends RedisEnvironment {
  import Keys.{Keys => _, _}

  final def _del[K: Schema]: RedisCommand[(K, List[K]), Long] =
    RedisCommand(Del, NonEmptyList(ArbitraryKeyInput[K]()), LongOutput, codec, executor)

  final def _dump[K: Schema]: RedisCommand[K, Chunk[Byte]] =
    RedisCommand(Dump, ArbitraryKeyInput[K](), BulkStringOutput, codec, executor)

  final def _exists[K: Schema]: RedisCommand[(K, List[K]), Long] =
    RedisCommand(Exists, NonEmptyList(ArbitraryKeyInput[K]()), LongOutput, codec, executor)

  final def _expire[K: Schema]: RedisCommand[(K, Duration), Boolean] =
    RedisCommand(Expire, Tuple2(ArbitraryKeyInput[K](), DurationSecondsInput), BoolOutput, codec, executor)

  final def _expireAt[K: Schema]: RedisCommand[(K, Instant), Boolean] =
    RedisCommand(ExpireAt, Tuple2(ArbitraryKeyInput[K](), TimeSecondsInput), BoolOutput, codec, executor)

  final def _keys[V: Schema]: RedisCommand[String, Chunk[V]] =
    RedisCommand(Keys.Keys, StringInput, ChunkOutput(ArbitraryOutput[V]()), codec, executor)

  final def _migrate[K: Schema]: RedisCommand[
    (String, Long, K, Long, Long, Option[Copy], Option[Replace], Option[Auth], Option[(K, List[K])]),
    String
  ] = RedisCommand(
    Migrate,
    Tuple9(
      StringInput,
      LongInput,
      ArbitraryKeyInput[K](),
      LongInput,
      LongInput,
      OptionalInput(CopyInput),
      OptionalInput(ReplaceInput),
      OptionalInput(AuthInput),
      OptionalInput(NonEmptyList(ArbitraryKeyInput[K]()))
    ),
    StringOutput,
    codec,
    executor
  )

  final def _move[K: Schema]: RedisCommand[(K, Long), Boolean] =
    RedisCommand(Move, Tuple2(ArbitraryKeyInput[K](), LongInput), BoolOutput, codec, executor)

  final def _persist[K: Schema]: RedisCommand[K, Boolean] =
    RedisCommand(Persist, ArbitraryKeyInput[K](), BoolOutput, codec, executor)

  final def _pExpire[K: Schema]: RedisCommand[(K, Duration), Boolean] =
    RedisCommand(PExpire, Tuple2(ArbitraryKeyInput[K](), DurationMillisecondsInput), BoolOutput, codec, executor)

  final def _pExpireAt[K: Schema]: RedisCommand[(K, Instant), Boolean] =
    RedisCommand(PExpireAt, Tuple2(ArbitraryKeyInput[K](), TimeMillisecondsInput), BoolOutput, codec, executor)

  final def _pTtl[K: Schema]: RedisCommand[K, Duration] =
    RedisCommand(PTtl, ArbitraryKeyInput[K](), DurationMillisecondsOutput, codec, executor)

  final def _randomKey[V: Schema]: RedisCommand[Unit, Option[V]] =
    RedisCommand(RandomKey, NoInput, OptionalOutput(ArbitraryOutput[V]()), codec, executor)

  final def _rename[K: Schema]: RedisCommand[(K, K), Unit] =
    RedisCommand(Rename, Tuple2(ArbitraryKeyInput[K](), ArbitraryKeyInput[K]()), UnitOutput, codec, executor)

  final def _renameNx[K: Schema]: RedisCommand[(K, K), Boolean] =
    RedisCommand(RenameNx, Tuple2(ArbitraryKeyInput[K](), ArbitraryKeyInput[K]()), BoolOutput, codec, executor)

  final def _restore[K: Schema]
    : RedisCommand[(K, Long, Chunk[Byte], Option[Replace], Option[AbsTtl], Option[IdleTime], Option[Freq]), Unit] =
    RedisCommand(
      Restore,
      Tuple7(
        ArbitraryKeyInput[K](),
        LongInput,
        ValueInput,
        OptionalInput(ReplaceInput),
        OptionalInput(AbsTtlInput),
        OptionalInput(IdleTimeInput),
        OptionalInput(FreqInput)
      ),
      UnitOutput,
      codec,
      executor
    )

  final def _scan[K: Schema]
    : RedisCommand[(Long, Option[Pattern], Option[Count], Option[RedisType]), (Long, Chunk[K])] = RedisCommand(
    Scan,
    Tuple4(LongInput, OptionalInput(PatternInput), OptionalInput(CountInput), OptionalInput(RedisTypeInput)),
    Tuple2Output(ArbitraryOutput[Long](), ChunkOutput(ArbitraryOutput[K]())),
    codec,
    executor
  )

  final def _sort[K: Schema, V: Schema]
    : RedisCommand[(K, Option[String], Option[Limit], Option[(String, List[String])], Order, Option[Alpha]), Chunk[V]] =
    RedisCommand(
      Sort,
      Tuple6(
        ArbitraryKeyInput[K](),
        OptionalInput(ByInput),
        OptionalInput(LimitInput),
        OptionalInput(NonEmptyList(GetInput)),
        OrderInput,
        OptionalInput(AlphaInput)
      ),
      ChunkOutput(ArbitraryOutput[V]()),
      codec,
      executor
    )

  final def _sortStore[K: Schema]: RedisCommand[
    (K, Option[String], Option[zio.redis.Limit], Option[(String, List[String])], zio.redis.Order, Option[Alpha], Store),
    Long
  ] = RedisCommand(
    SortStore,
    Tuple7(
      ArbitraryKeyInput[K](),
      OptionalInput(ByInput),
      OptionalInput(LimitInput),
      OptionalInput(NonEmptyList(GetInput)),
      OrderInput,
      OptionalInput(AlphaInput),
      StoreInput
    ),
    LongOutput,
    codec,
    executor
  )

  final def _touch[K: Schema]: RedisCommand[(K, List[K]), Long] =
    RedisCommand(Touch, NonEmptyList(ArbitraryKeyInput[K]()), LongOutput, codec, executor)

  final def _ttl[K: Schema]: RedisCommand[K, Duration] =
    RedisCommand(Ttl, ArbitraryKeyInput[K](), DurationSecondsOutput, codec, executor)

  final def _typeOf[K: Schema]: RedisCommand[K, zio.redis.RedisType] =
    RedisCommand(TypeOf, ArbitraryKeyInput[K](), TypeOutput, codec, executor)

  final def _unlink[K: Schema]: RedisCommand[(K, List[K]), Long] =
    RedisCommand(Unlink, NonEmptyList(ArbitraryKeyInput[K]()), LongOutput, codec, executor)

  final val _wait: RedisCommand[(Long, Long), Long] =
    RedisCommand(Wait, Tuple2(LongInput, LongInput), LongOutput, codec, executor)
}

private object Keys {
  final val Del       = "DEL"
  final val Dump      = "DUMP"
  final val Exists    = "EXISTS"
  final val Expire    = "EXPIRE"
  final val ExpireAt  = "EXPIREAT"
  final val Keys      = "KEYS"
  final val Migrate   = "MIGRATE"
  final val Move      = "MOVE"
  final val Persist   = "PERSIST"
  final val PExpire   = "PEXPIRE"
  final val PExpireAt = "PEXPIREAT"
  final val PTtl      = "PTTL"
  final val RandomKey = "RANDOMKEY"
  final val Rename    = "RENAME"
  final val RenameNx  = "RENAMENX"
  final val Restore   = "RESTORE"
  final val Scan      = "SCAN"
  final val Sort      = "SORT"
  final val SortStore = "SORT"
  final val Touch     = "TOUCH"
  final val Ttl       = "TTL"
  final val TypeOf    = "TYPE"
  final val Unlink    = "UNLINK"
  final val Wait      = "WAIT"
}
