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

private[redis] trait Streams extends RedisEnvironment {
  import Streams._

  final def _xAck[SK: Schema, G: Schema, I: Schema]: RedisCommand[(SK, G, (I, List[I])), Long] = RedisCommand(
    XAck,
    Tuple3(ArbitraryKeyInput[SK](), ArbitraryValueInput[G](), NonEmptyList(ArbitraryValueInput[I]())),
    LongOutput,
    codec,
    executor
  )

  final def _xAdd[SK: Schema, I: Schema, K: Schema, V: Schema, R: Schema]: RedisCommand[
    (SK, Option[StreamMaxLen], I, ((K, V), List[(K, V)])),
    R
  ] = RedisCommand(
    XAdd,
    Tuple4(
      ArbitraryKeyInput[SK](),
      OptionalInput(StreamMaxLenInput),
      ArbitraryValueInput[I](),
      NonEmptyList(Tuple2(ArbitraryKeyInput[K](), ArbitraryValueInput[V]()))
    ),
    ArbitraryOutput[R](),
    codec,
    executor
  )

  final def _xInfoStream[SK: Schema, RI: Schema, RK: Schema, RV: Schema]: RedisCommand[SK, StreamInfo[RI, RK, RV]] =
    RedisCommand(XInfoStream, ArbitraryKeyInput[SK](), StreamInfoOutput[RI, RK, RV](), codec, executor)

  final def _xInfoStreamFull[SK: Schema, RI: Schema, RK: Schema, RV: Schema]: RedisCommand[
    (SK, String),
    StreamInfoWithFull.FullStreamInfo[RI, RK, RV]
  ] = RedisCommand(
    XInfoStream,
    Tuple2(ArbitraryKeyInput[SK](), ArbitraryValueInput[String]()),
    StreamInfoFullOutput[RI, RK, RV](),
    codec,
    executor
  )

  final def _xInfoStreamFullWithCount[SK: Schema, RI: Schema, RK: Schema, RV: Schema]: RedisCommand[
    (SK, String, Count),
    zio.redis.StreamInfoWithFull.FullStreamInfo[RI, RK, RV]
  ] = RedisCommand(
    XInfoStream,
    Tuple3(ArbitraryKeyInput[SK](), ArbitraryValueInput[String](), CountInput),
    StreamInfoFullOutput[RI, RK, RV](),
    codec,
    executor
  )

  final def _xInfoGroups[SK: Schema]: RedisCommand[SK, Chunk[StreamGroupsInfo]] =
    RedisCommand(XInfoGroups, ArbitraryKeyInput[SK](), StreamGroupsInfoOutput, codec, executor)

  final def _xInfoConsumers[SK: Schema, SG: Schema]: RedisCommand[(SK, SG), Chunk[StreamConsumersInfo]] =
    RedisCommand(
      XInfoConsumers,
      Tuple2(ArbitraryKeyInput[SK](), ArbitraryValueInput[SG]()),
      StreamConsumersInfoOutput,
      codec,
      executor
    )

  final def _xAddWithMaxLen[SK: Schema, I: Schema, K: Schema, V: Schema, R: Schema]: RedisCommand[
    (SK, Option[zio.redis.StreamMaxLen], I, ((K, V), List[(K, V)])),
    R
  ] = RedisCommand(
    XAdd,
    Tuple4(
      ArbitraryKeyInput[SK](),
      OptionalInput(StreamMaxLenInput),
      ArbitraryValueInput[I](),
      NonEmptyList(Tuple2(ArbitraryKeyInput[K](), ArbitraryValueInput[V]()))
    ),
    ArbitraryOutput[R](),
    codec,
    executor
  )

  final def _xClaim[SK: Schema, SG: Schema, SC: Schema, I: Schema, RK: Schema, RV: Schema]: RedisCommand[
    (SK, SG, SC, Duration, (I, List[I]), Option[Duration], Option[Duration], Option[Long], Option[WithForce]),
    Chunk[StreamEntry[I, RK, RV]]
  ] = RedisCommand(
    XClaim,
    Tuple9(
      ArbitraryKeyInput[SK](),
      ArbitraryValueInput[SG](),
      ArbitraryValueInput[SC](),
      DurationMillisecondsInput,
      NonEmptyList(ArbitraryValueInput[I]()),
      OptionalInput(IdleInput),
      OptionalInput(TimeInput),
      OptionalInput(RetryCountInput),
      OptionalInput(WithForceInput)
    ),
    StreamEntriesOutput[I, RK, RV](),
    codec,
    executor
  )

  final def _xClaimWithJustId[SK: Schema, SG: Schema, SC: Schema, I: Schema, R: Schema]: RedisCommand[
    (
      SK,
      SG,
      SC,
      Duration,
      (I, List[I]),
      Option[Duration],
      Option[Duration],
      Option[Long],
      Option[WithForce],
      WithJustId
    ),
    Chunk[R]
  ] = RedisCommand(
    XClaim,
    Tuple10(
      ArbitraryKeyInput[SK](),
      ArbitraryValueInput[SG](),
      ArbitraryValueInput[SC](),
      DurationMillisecondsInput,
      NonEmptyList(ArbitraryValueInput[I]()),
      OptionalInput(IdleInput),
      OptionalInput(TimeInput),
      OptionalInput(RetryCountInput),
      OptionalInput(WithForceInput),
      WithJustIdInput
    ),
    ChunkOutput(ArbitraryOutput[R]()),
    codec,
    executor
  )

  final def _xDel[SK: Schema, I: Schema]: RedisCommand[(SK, (I, List[I])), Long] =
    RedisCommand(
      XDel,
      Tuple2(ArbitraryKeyInput[SK](), NonEmptyList(ArbitraryValueInput[I]())),
      LongOutput,
      codec,
      executor
    )

  final def _xGroupCreate[SK: Schema, SG: Schema, I: Schema]: RedisCommand[XGroupCommand.Create[SK, SG, I], Unit] =
    RedisCommand(XGroup, XGroupCreateInput[SK, SG, I](), UnitOutput, codec, executor)

  final def _xGroupSetId[SK: Schema, SG: Schema, I: Schema]
    : RedisCommand[zio.redis.XGroupCommand.SetId[SK, SG, I], Unit] =
    RedisCommand(XGroup, XGroupSetIdInput[SK, SG, I](), UnitOutput, codec, executor)

  final def _xGroupDestroy[SK: Schema, SG: Schema]: RedisCommand[zio.redis.XGroupCommand.Destroy[SK, SG], Boolean] =
    RedisCommand(XGroup, XGroupDestroyInput[SK, SG](), BoolOutput, codec, executor)

  final def _xGroupCreateConsumer[SK: Schema, SG: Schema, SC: Schema]
    : RedisCommand[zio.redis.XGroupCommand.CreateConsumer[SK, SG, SC], Boolean] =
    RedisCommand(XGroup, XGroupCreateConsumerInput[SK, SG, SC](), BoolOutput, codec, executor)

  final def _xGroupDelConsumer[SK: Schema, SG: Schema, SC: Schema]
    : RedisCommand[zio.redis.XGroupCommand.DelConsumer[SK, SG, SC], Long] =
    RedisCommand(XGroup, XGroupDelConsumerInput[SK, SG, SC](), LongOutput, codec, executor)

  final def _xLen[SK: Schema]: RedisCommand[SK, Long] =
    RedisCommand(XLen, ArbitraryKeyInput[SK](), LongOutput, codec, executor)

  final def _xPending[SK: Schema, SG: Schema]: RedisCommand[(SK, SG, Option[Duration]), PendingInfo] = RedisCommand(
    XPending,
    Tuple3(ArbitraryKeyInput[SK](), ArbitraryValueInput[SG](), OptionalInput(IdleInput)),
    XPendingOutput,
    codec,
    executor
  )

  final def _xPendingMessages[SK: Schema, SG: Schema, I: Schema, SC: Schema]: RedisCommand[
    (SK, SG, Option[Duration], I, I, Long, Option[SC]),
    Chunk[PendingMessage]
  ] = RedisCommand(
    XPending,
    Tuple7(
      ArbitraryKeyInput[SK](),
      ArbitraryValueInput[SG](),
      OptionalInput(IdleInput),
      ArbitraryValueInput[I](),
      ArbitraryValueInput[I](),
      LongInput,
      OptionalInput(ArbitraryValueInput[SC]())
    ),
    PendingMessagesOutput,
    codec,
    executor
  )

  final def _xRange[SK: Schema, I: Schema, RK: Schema, RV: Schema]: RedisCommand[
    (SK, I, I, Option[zio.redis.Count]),
    Chunk[zio.redis.StreamEntry[I, RK, RV]]
  ] = RedisCommand(
    XRange,
    Tuple4(ArbitraryKeyInput[SK](), ArbitraryValueInput[I](), ArbitraryValueInput[I](), OptionalInput(CountInput)),
    StreamEntriesOutput[I, RK, RV](),
    codec,
    executor
  )

  final def _xRangeWithCount[SK: Schema, I: Schema, RK: Schema, RV: Schema]: RedisCommand[
    (SK, I, I, Option[zio.redis.Count]),
    Chunk[zio.redis.StreamEntry[I, RK, RV]]
  ] = RedisCommand(
    XRange,
    Tuple4(ArbitraryKeyInput[SK](), ArbitraryValueInput[I](), ArbitraryValueInput[I](), OptionalInput(CountInput)),
    StreamEntriesOutput[I, RK, RV](),
    codec,
    executor
  )

  final def _xRead[SK: Schema, I: Schema, RK: Schema, RV: Schema]: RedisCommand[
    (Option[zio.redis.Count], Option[Duration], ((SK, I), Chunk[(SK, I)])),
    Chunk[StreamChunk[SK, I, RK, RV]]
  ] = RedisCommand(
    XRead,
    Tuple3(OptionalInput(CountInput), OptionalInput(BlockInput), StreamsInput[SK, I]()),
    ChunkOutput(StreamOutput[SK, I, RK, RV]()),
    codec,
    executor
  )

  final def _xReadGroup[SG: Schema, SC: Schema, SK: Schema, I: Schema, RK: Schema, RV: Schema]: RedisCommand[
    (SG, SC, Option[zio.redis.Count], Option[Duration], Option[NoAck], ((SK, I), Chunk[(SK, I)])),
    Chunk[zio.redis.StreamChunk[SK, I, RK, RV]]
  ] = RedisCommand(
    XReadGroup,
    Tuple6(
      ArbitraryValueInput[SG](),
      ArbitraryValueInput[SC](),
      OptionalInput(CountInput),
      OptionalInput(BlockInput),
      OptionalInput(NoAckInput),
      StreamsInput[SK, I]()
    ),
    ChunkOutput(StreamOutput[SK, I, RK, RV]()),
    codec,
    executor
  )

  final def _xRevRange[SK: Schema, I: Schema, RK: Schema, RV: Schema]: RedisCommand[
    (SK, I, I, Option[zio.redis.Count]),
    Chunk[zio.redis.StreamEntry[I, RK, RV]]
  ] = RedisCommand(
    XRevRange,
    Tuple4(ArbitraryKeyInput[SK](), ArbitraryValueInput[I](), ArbitraryValueInput[I](), OptionalInput(CountInput)),
    StreamEntriesOutput[I, RK, RV](),
    codec,
    executor
  )

  final def _xRevRangeWithCount[SK: Schema, I: Schema, RK: Schema, RV: Schema]
    : RedisCommand[(SK, I, I, Option[zio.redis.Count]), Chunk[zio.redis.StreamEntry[I, RK, RV]]] = RedisCommand(
    XRevRange,
    Tuple4(ArbitraryKeyInput[SK](), ArbitraryValueInput[I](), ArbitraryValueInput[I](), OptionalInput(CountInput)),
    StreamEntriesOutput[I, RK, RV](),
    codec,
    executor
  )

  final def _xTrim[SK: Schema]: RedisCommand[(SK, zio.redis.StreamMaxLen), Long] =
    RedisCommand(XTrim, Tuple2(ArbitraryKeyInput[SK](), StreamMaxLenInput), LongOutput, codec, executor)
}

private object Streams {
  final val XAck           = "XACK"
  final val XAdd           = "XADD"
  final val XClaim         = "XCLAIM"
  final val XDel           = "XDEL"
  final val XGroup         = "XGROUP"
  final val XInfoStream    = "XINFO STREAM"
  final val XInfoGroups    = "XINFO GROUPS"
  final val XInfoConsumers = "XINFO CONSUMERS"
  final val XLen           = "XLEN"
  final val XPending       = "XPENDING"
  final val XRange         = "XRANGE"
  final val XRead          = "XREAD"
  final val XReadGroup     = "XREADGROUP GROUP"
  final val XRevRange      = "XREVRANGE"
  final val XTrim          = "XTRIM"
}
