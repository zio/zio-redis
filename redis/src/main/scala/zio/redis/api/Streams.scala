package zio.redis.api

import java.time.Duration

import zio.{ Chunk, ZIO }
import zio.redis.Input._
import zio.redis.Output._
import zio.redis._

trait Streams {
  import Streams._
  import XGroupCommand._

  final def xAck(key: String, group: String, id: String, ids: String*): ZIO[RedisExecutor, RedisError, Long] =
    XAck.run((key, group, (id, ids.toList)))

  final def xAdd(
    key: String,
    id: String,
    pair: (String, String),
    pairs: (String, String)*
  ): ZIO[RedisExecutor, RedisError, String] =
    XAdd.run((key, id, (pair, pairs.toList)))

  final def xClaim(key: String, group: String, consumer: String, minIdleTime: Duration, id: String, ids: String*)(
    idle: Option[Idle] = None,
    time: Option[Time] = None,
    retryCount: Option[RetryCount] = None,
    withForce: Option[WithForce] = None
  ): ZIO[RedisExecutor, RedisError, Map[String, Map[String, String]]] =
    XClaim.run((key, group, consumer, minIdleTime, (id, ids.toList), idle, time, retryCount, withForce))

  final def xClaimWithJustId(
    key: String,
    group: String,
    consumer: String,
    minIdleTime: Duration,
    id: String,
    ids: String*
  )(
    idle: Option[Idle] = None,
    time: Option[Time] = None,
    retryCount: Option[RetryCount] = None,
    withForce: Option[WithForce] = None
  ): ZIO[RedisExecutor, RedisError, Chunk[String]] =
    XClainWithJustId.run(
      (key, group, consumer, minIdleTime, (id, ids.toList), idle, time, retryCount, withForce, WithJustId)
    )

  final def xDel(key: String, id: String, ids: String*): ZIO[RedisExecutor, RedisError, Long] =
    XDel.run((key, (id, ids.toList)))

  final def xGroupCreate(
    key: String,
    group: String,
    id: String,
    mkStream: Boolean = false
  ): ZIO[RedisExecutor, RedisError, String] =
    XGroupCreate.run(Create(key, group, id, mkStream))

  final def xGroupSetId(key: String, group: String, id: String): ZIO[RedisExecutor, RedisError, String] =
    XGroupSetId.run(SetId(key, group, id))

  final def xGroupDestroy(key: String, group: String): ZIO[RedisExecutor, RedisError, String] =
    XGroupDestroy.run(Destroy(key, group))

  final def xGroupCreateConsumer(key: String, group: String, consumer: String): ZIO[RedisExecutor, RedisError, String] =
    XGroupCreateConsumer.run(CreateConsumer(key, group, consumer))

  final def xGroupDelConsumer(key: String, group: String, consumer: String): ZIO[RedisExecutor, RedisError, String] =
    XGroupDelConsumer.run(DelConsumer(key, group, consumer))

  final def xLen(key: String): ZIO[RedisExecutor, RedisError, Long] =
    XLen.run(key)

  final def xPending(key: String, group: String): ZIO[RedisExecutor, RedisError, PendingInfo] =
    XPending.run((key, group))

  final def xPending(
    key: String,
    group: String,
    start: String,
    end: String,
    count: Long,
    consumer: Option[String] = None
  ): ZIO[RedisExecutor, RedisError, Chunk[PendingMessage]] =
    XPendingMessages.run((key, group, start, end, count, consumer))

  final def xRange(
    key: String,
    start: String,
    end: String
  ): ZIO[RedisExecutor, RedisError, Map[String, Map[String, String]]] =
    XRange.run((key, start, end, None))

  final def xRange(
    key: String,
    start: String,
    end: String,
    count: Long
  ): ZIO[RedisExecutor, RedisError, Map[String, Map[String, String]]] =
    XRange.run((key, start, end, Some(Count(count))))

  // TODO: change map to list of pairs everywhere
  final def xRead(count: Option[Long] = None, block: Option[Duration] = None)(
    stream: (String, String),
    streams: (String, String)*
  ): ZIO[RedisExecutor, RedisError, Map[String, Map[String, Map[String, String]]]] =
    XRead.run((count.map(Count), block.map(Block), (stream, Chunk.fromIterable(streams))))

  final def xReadGroup(group: String, consumer: String)(
    count: Option[Long] = None,
    block: Option[Duration] = None,
    noAck: Boolean = false
  )(
    stream: (String, String),
    streams: (String, String)*
  ): ZIO[RedisExecutor, RedisError, Map[String, Map[String, Map[String, String]]]] =
    XReadGroup.run(
      (Group(group, consumer), count.map(Count), block.map(Block), noAck, (stream, Chunk.fromIterable(streams)))
    )

  final def xRevRange(
    key: String,
    end: String,
    start: String
  ): ZIO[RedisExecutor, RedisError, Map[String, Map[String, String]]] =
    XRevRange.run((key, end, start, None))

  final def xRevRange(
    key: String,
    end: String,
    start: String,
    count: Long
  ): ZIO[RedisExecutor, RedisError, Map[String, Map[String, String]]] =
    XRevRange.run((key, end, start, Some(Count(count))))

  final def xTrim(key: String, count: Long, approximate: Boolean = false): ZIO[RedisExecutor, RedisError, Long] =
    XTrim.run((key, MaxLen(approximate, count)))
}

private object Streams {

  final val XAck = RedisCommand("XACK", Tuple3(StringInput, StringInput, NonEmptyList(StringInput)), LongOutput)

  final val XAdd = RedisCommand(
    "XADD",
    Tuple3(StringInput, StringInput, NonEmptyList(Tuple2(StringInput, StringInput))),
    MultiStringOutput
  )

  final val XClaim = RedisCommand(
    "XCLAIM",
    Tuple9(
      StringInput,
      StringInput,
      StringInput,
      DurationMillisecondsInput,
      NonEmptyList(StringInput),
      OptionalInput(IdleInput),
      OptionalInput(TimeInput),
      OptionalInput(RetryCountInput),
      OptionalInput(WithForceInput)
    ),
    StreamOutput
  )

  final val XClainWithJustId = RedisCommand(
    "XCLAIM",
    Tuple10(
      StringInput,
      StringInput,
      StringInput,
      DurationMillisecondsInput,
      NonEmptyList(StringInput),
      OptionalInput(IdleInput),
      OptionalInput(TimeInput),
      OptionalInput(RetryCountInput),
      OptionalInput(WithForceInput),
      WithJustIdInput
    ),
    ChunkOutput
  )

  final val XDel = RedisCommand("XDEL", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)

  final val XGroupCreate = RedisCommand("XGROUP", XGroupCreateInput, StringOutput)

  final val XGroupSetId = RedisCommand("XGROUP", XGroupSetIdInput, StringOutput)

  final val XGroupDestroy = RedisCommand("XGROUP", XGroupDestroyInput, StringOutput)

  final val XGroupCreateConsumer = RedisCommand("XGROUP", XGroupCreateConsumerInput, StringOutput)

  final val XGroupDelConsumer = RedisCommand("XGROUP", XGroupDelConsumerInput, StringOutput)

  // TODO: implement XINFO command

  final val XLen = RedisCommand("XLEN", StringInput, LongOutput)

  final val XPending = RedisCommand("XPENDING", Tuple2(StringInput, StringInput), XPendingOutput)

  final val XPendingMessages =
    RedisCommand(
      "XPENDING",
      Tuple6(StringInput, StringInput, StringInput, StringInput, LongInput, OptionalInput(StringInput)),
      PendingMessagesOutput
    )

  final val XRange =
    RedisCommand("XRANGE", Tuple4(StringInput, StringInput, StringInput, OptionalInput(CountInput)), StreamOutput)

  final val XRead =
    RedisCommand("XREAD", Tuple3(OptionalInput(CountInput), OptionalInput(BlockInput), StreamsInput), XReadOutput)

  final val XReadGroup = RedisCommand(
    "XREADGROUP",
    Tuple5(GroupInput, OptionalInput(CountInput), OptionalInput(BlockInput), NoAckInput, StreamsInput),
    XReadOutput
  )

  final val XRevRange =
    RedisCommand("XREVRANGE", Tuple4(StringInput, StringInput, StringInput, OptionalInput(CountInput)), StreamOutput)

  final val XTrim = RedisCommand("XTRIM", Tuple2(StringInput, MaxLenInput), LongOutput)
}
