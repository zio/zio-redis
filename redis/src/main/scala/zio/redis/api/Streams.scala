package zio.redis.api

import java.time.Duration

import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.{ Chunk, ZIO }

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
    XAdd.run((key, None, id, (pair, pairs.toList)))

  final def xAddWithMaxLen(
    key: String,
    id: String,
    count: Long,
    approximate: Boolean,
    pair: (String, String),
    pairs: (String, String)*
  ): ZIO[RedisExecutor, RedisError, String] =
    XAdd.run((key, Some(MaxLen(approximate, count)), id, (pair, pairs.toList)))

  final def xClaim(key: String, group: String, consumer: String, minIdleTime: Duration, id: String, ids: String*)(
    idle: Option[Duration] = None,
    time: Option[Duration] = None,
    retryCount: Option[Long] = None,
    force: Boolean = false
  ): ZIO[RedisExecutor, RedisError, Map[String, Map[String, String]]] =
    XClaim.run(
      (
        key,
        group,
        consumer,
        minIdleTime,
        (id, ids.toList),
        idle,
        time,
        retryCount,
        if (force) Some(WithForce) else None
      )
    )

  final def xClaimWithJustId(
    key: String,
    group: String,
    consumer: String,
    minIdleTime: Duration,
    id: String,
    ids: String*
  )(
    idle: Option[Duration] = None,
    time: Option[Duration] = None,
    retryCount: Option[Long] = None,
    force: Boolean = false
  ): ZIO[RedisExecutor, RedisError, Chunk[String]] =
    XClaimWithJustId.run(
      (
        key,
        group,
        consumer,
        minIdleTime,
        (id, ids.toList),
        idle,
        time,
        retryCount,
        if (force) Some(WithForce) else None,
        WithJustId
      )
    )

  final def xDel(key: String, id: String, ids: String*): ZIO[RedisExecutor, RedisError, Long] =
    XDel.run((key, (id, ids.toList)))

  final def xGroupCreate(
    key: String,
    group: String,
    id: String,
    mkStream: Boolean = false
  ): ZIO[RedisExecutor, RedisError, Unit] =
    XGroupCreate.run(Create(key, group, id, mkStream))

  final def xGroupSetId(key: String, group: String, id: String): ZIO[RedisExecutor, RedisError, Unit] =
    XGroupSetId.run(SetId(key, group, id))

  final def xGroupDestroy(key: String, group: String): ZIO[RedisExecutor, RedisError, Long] =
    XGroupDestroy.run(Destroy(key, group))

  final def xGroupCreateConsumer(key: String, group: String, consumer: String): ZIO[RedisExecutor, RedisError, Unit] =
    XGroupCreateConsumer.run(CreateConsumer(key, group, consumer))

  final def xGroupDelConsumer(key: String, group: String, consumer: String): ZIO[RedisExecutor, RedisError, Long] =
    XGroupDelConsumer.run(DelConsumer(key, group, consumer))

  final def xLen(key: String): ZIO[RedisExecutor, RedisError, Long] =
    XLen.run(key)

  final def xPending(key: String, group: String): ZIO[RedisExecutor, RedisError, PendingInfo] =
    XPending.run((key, group, None))

  final def xPending(key: String, group: String, idle: Duration): ZIO[RedisExecutor, RedisError, PendingInfo] =
    XPending.run((key, group, Some(idle)))

  final def xPending(
    key: String,
    group: String,
    start: String,
    end: String,
    count: Long,
    consumer: Option[String] = None,
    idle: Option[Duration] = None
  ): ZIO[RedisExecutor, RedisError, Chunk[PendingMessage]] =
    XPendingMessages.run((key, group, start, end, count, consumer, idle))

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

  final def xRead(count: Option[Long] = None, block: Option[Duration] = None)(
    stream: (String, String),
    streams: (String, String)*
  ): ZIO[RedisExecutor, RedisError, Map[String, Map[String, Map[String, String]]]] =
    XRead.run((count.map(Count), block, (stream, Chunk.fromIterable(streams))))

  final def xReadGroup(
    group: String,
    consumer: String,
    count: Option[Long] = None,
    block: Option[Duration] = None,
    noAck: Boolean = false
  )(
    stream: (String, String),
    streams: (String, String)*
  ): ZIO[RedisExecutor, RedisError, Map[String, Map[String, Map[String, String]]]] =
    XReadGroup.run(
      (
        Group(group, consumer),
        count.map(Count),
        block,
        if (noAck) Some(NoAck) else None,
        (stream, Chunk.fromIterable(streams))
      )
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
    Tuple4(StringInput, OptionalInput(MaxLenInput), StringInput, NonEmptyList(Tuple2(StringInput, StringInput))),
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

  final val XClaimWithJustId = RedisCommand(
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

  final val XGroupCreate = RedisCommand("XGROUP", XGroupCreateInput, UnitOutput)

  final val XGroupSetId = RedisCommand("XGROUP", XGroupSetIdInput, UnitOutput)

  final val XGroupDestroy = RedisCommand("XGROUP", XGroupDestroyInput, LongOutput)

  final val XGroupCreateConsumer = RedisCommand("XGROUP", XGroupCreateConsumerInput, UnitOutput)

  final val XGroupDelConsumer = RedisCommand("XGROUP", XGroupDelConsumerInput, LongOutput)

  // TODO: implement XINFO command

  final val XLen = RedisCommand("XLEN", StringInput, LongOutput)

  final val XPending =
    RedisCommand("XPENDING", Tuple3(StringInput, StringInput, OptionalInput(IdleInput)), XPendingOutput)

  final val XPendingMessages =
    RedisCommand(
      "XPENDING",
      Tuple7(
        StringInput,
        StringInput,
        StringInput,
        StringInput,
        LongInput,
        OptionalInput(StringInput),
        OptionalInput(IdleInput)
      ),
      PendingMessagesOutput
    )

  final val XRange =
    RedisCommand("XRANGE", Tuple4(StringInput, StringInput, StringInput, OptionalInput(CountInput)), StreamOutput)

  final val XRead =
    RedisCommand("XREAD", Tuple3(OptionalInput(CountInput), OptionalInput(BlockInput), StreamsInput), XReadOutput)

  final val XReadGroup = RedisCommand(
    "XREADGROUP",
    Tuple5(GroupInput, OptionalInput(CountInput), OptionalInput(BlockInput), OptionalInput(NoAckInput), StreamsInput),
    XReadOutput
  )

  final val XRevRange =
    RedisCommand("XREVRANGE", Tuple4(StringInput, StringInput, StringInput, OptionalInput(CountInput)), StreamOutput)

  final val XTrim = RedisCommand("XTRIM", Tuple2(StringInput, MaxLenInput), LongOutput)
}
