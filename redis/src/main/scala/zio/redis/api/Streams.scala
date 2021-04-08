package zio.redis.api

import zio.duration._
import zio.redis.Input.{ XInfoConsumersInput, _ }
import zio.redis.Output.{ StreamGroupsInfoOutput, _ }
import zio.redis._
import zio.{ Chunk, ZIO }

trait Streams {
  import Streams._
  import XGroupCommand._

  /**
   * Removes one or multiple messages from the pending entries list (PEL) of a stream consumer group.
   *
   * @param key ID of the stream
   * @param group ID of the consumer group
   * @param id ID of the message
   * @param ids IDs of the rest of the messages
   * @return the number of messages successfully acknowledged.
   */
  final def xAck(key: String, group: String, id: String, ids: String*): ZIO[RedisExecutor, RedisError, Long] =
    XAck.run((key, group, (id, ids.toList)))

  /**
   * Appends the specified stream entry to the stream at the specified key.
   *
   * @param key ID of the stream
   * @param id ID of the message
   * @param pair field and value pair
   * @param pairs rest of the field and value pairs
   * @return ID of the added entry.
   */
  final def xAdd(
    key: String,
    id: String,
    pair: (String, String),
    pairs: (String, String)*
  ): ZIO[RedisExecutor, RedisError, String] =
    XAdd.run((key, None, id, (pair, pairs.toList)))

  /**
   * An introspection command used in order to retrieve different information about the stream.
   *
   * @param key ID of the stream
   * @return General information about the stream stored at the specified key.
   */
  final def xInfoStream(key: String): ZIO[RedisExecutor, RedisError, StreamInfo] =
    XInfoStream.run(XInfoCommand.Stream(key))

  /**
   * Returns the entire state of the stream, including entries, groups, consumers and PELs.
   *
   * @param key ID of the stream
   * @return General information about the stream stored at the specified key.
   */
  final def xInfoStreamFull(
    key: String
  ): ZIO[RedisExecutor, RedisError, StreamInfoWithFull.FullStreamInfo] =
    XInfoStreamFull.run(XInfoCommand.Stream(key, Some(XInfoCommand.Full(None))))

  /**
   * Returns the entire state of the stream, including entries, groups, consumers and PELs.
   *
   * @param key ID of the stream
   * @param count limit the amount of stream/PEL entries that are returned (The first <count> entries are returned).
   * @return General information about the stream stored at the specified key.
   */
  final def xInfoStreamFull(
    key: String,
    count: Long
  ): ZIO[RedisExecutor, RedisError, StreamInfoWithFull.FullStreamInfo] =
    XInfoStreamFull.run(XInfoCommand.Stream(key, Some(XInfoCommand.Full(Some(count)))))

  /**
   * An introspection command used in order to retrieve different information about the group.
   *
   * @param key ID of the stream
   * @return List of consumer groups associated with the stream stored at the specified key.
   */
  final def xInfoGroups(key: String): ZIO[RedisExecutor, RedisError, Chunk[StreamGroupsInfo]] =
    XInfoGroups.run(XInfoCommand.Groups(key))

  /**
   * An introspection command used in order to retrieve different information about the consumers.
   *
   * @param key ID of the stream
   * @param group ID of the consumer group
   * @return List of every consumer in a specific consumer group.
   */
  final def xInfoConsumers(key: String, group: String): ZIO[RedisExecutor, RedisError, Chunk[StreamConsumersInfo]] =
    XInfoConsumers.run(XInfoCommand.Consumers(key, group))

  /**
   * Appends the specified stream entry to the stream at the specified key while limiting the size of the stream.
   *
   * @param key ID of the stream
   * @param id ID of the message
   * @param count maximum number of elements in a stream
   * @param approximate flag that indicates if a stream should be limited to the exact number of elements
   * @param pair field and value pair
   * @param pairs rest of the field and value pairs
   * @return ID of the added entry.
   */
  final def xAddWithMaxLen(key: String, id: String, count: Long, approximate: Boolean = false)(
    pair: (String, String),
    pairs: (String, String)*
  ): ZIO[RedisExecutor, RedisError, String] =
    XAdd.run((key, Some(StreamMaxLen(approximate, count)), id, (pair, pairs.toList)))

  /**
   * Changes the ownership of a pending message.
   *
   * @param key ID of the stream
   * @param group ID of the consumer group
   * @param consumer ID of the consumer
   * @param minIdleTime minimum idle time of a message
   * @param idle idle time (last time it was delivered) of the message that will be set
   * @param time same as idle but instead of a relative amount of milliseconds, it sets the idle time to a specific Unix time (in milliseconds)
   * @param retryCount retry counter of a message that will be set
   * @param force flag that indicates that a message doesn't have to be in a pending entries list (PEL)
   * @param id ID of a message
   * @param ids IDs of the rest of the messages
   * @return messages successfully claimed.
   */
  final def xClaim(
    key: String,
    group: String,
    consumer: String,
    minIdleTime: Duration,
    idle: Option[Duration] = None,
    time: Option[Duration] = None,
    retryCount: Option[Long] = None,
    force: Boolean = false
  )(id: String, ids: String*): ZIO[RedisExecutor, RedisError, Map[String, Map[String, String]]] =
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

  /**
   * Changes the ownership of a pending message.
   *
   * @param key ID of the stream
   * @param group ID of the consumer group
   * @param consumer ID of the consumer
   * @param minIdleTime minimum idle time of a message
   * @param idle idle time (last time it was delivered) of the message that will be set
   * @param time same as idle but instead of a relative amount of milliseconds, it sets the idle time to a specific Unix time (in milliseconds)
   * @param retryCount retry counter of a message that will be set
   * @param force flag that indicates that a message doesn't have to be in a pending entries list (PEL)
   * @param id ID of a message
   * @param ids IDs of the rest of the messages
   * @return IDs of the messages that are successfully claimed.
   */
  final def xClaimWithJustId(
    key: String,
    group: String,
    consumer: String,
    minIdleTime: Duration,
    idle: Option[Duration] = None,
    time: Option[Duration] = None,
    retryCount: Option[Long] = None,
    force: Boolean = false
  )(id: String, ids: String*): ZIO[RedisExecutor, RedisError, Chunk[String]] =
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

  /**
   * Removes the specified entries from a stream.
   *
   * @param key ID of the stream
   * @param id ID of the message
   * @param ids IDs of the rest of the messages
   * @return the number of entries deleted.
   */
  final def xDel(key: String, id: String, ids: String*): ZIO[RedisExecutor, RedisError, Long] =
    XDel.run((key, (id, ids.toList)))

  /**
   * Create a new consumer group associated with a stream.
   *
   * @param key ID of the stream
   * @param group ID of the consumer group
   * @param id ID of the last item in the stream to consider already delivered
   * @param mkStream ID of the last item in the stream to consider already delivered
   */
  final def xGroupCreate(
    key: String,
    group: String,
    id: String,
    mkStream: Boolean = false
  ): ZIO[RedisExecutor, RedisError, Unit] =
    XGroupCreate.run(Create(key, group, id, mkStream))

  /**
   * Set the consumer group last delivered ID to something else.
   *
   * @param key ID of the stream
   * @param group ID of the consumer group
   * @param id last delivered ID to set
   */
  final def xGroupSetId(key: String, group: String, id: String): ZIO[RedisExecutor, RedisError, Unit] =
    XGroupSetId.run(SetId(key, group, id))

  /**
   * Destroy a consumer group.
   *
   * @param key ID of the stream
   * @param group ID of the consumer group
   * @return flag that indicates if the deletion was successful.
   */
  final def xGroupDestroy(key: String, group: String): ZIO[RedisExecutor, RedisError, Boolean] =
    XGroupDestroy.run(Destroy(key, group))

  /**
   * Create a new consumer associated with a consumer group.
   *
   * @param key ID of the stream
   * @param group ID of the consumer group
   * @param consumer ID of the consumer
   */
  final def xGroupCreateConsumer(key: String, group: String, consumer: String): ZIO[RedisExecutor, RedisError, Unit] =
    XGroupCreateConsumer.run(CreateConsumer(key, group, consumer))

  /**
   * Remove a specific consumer from a consumer group.
   *
   * @param key ID of the stream
   * @param group ID of the consumer group
   * @param consumer ID of the consumer
   * @return the number of pending messages that the consumer had before it was deleted.
   */
  final def xGroupDelConsumer(key: String, group: String, consumer: String): ZIO[RedisExecutor, RedisError, Long] =
    XGroupDelConsumer.run(DelConsumer(key, group, consumer))

  /**
   * Fetches the number of entries inside a stream.
   *
   * @param key ID of the stream
   * @return the number of entries inside a stream
   */
  final def xLen(key: String): ZIO[RedisExecutor, RedisError, Long] =
    XLen.run(key)

  /**
   * Inspects the list of pending messages.
   *
   * @param key ID of the stream
   * @param group ID of the consumer group
   * @return summary about the pending messages in a given consumer group.
   */
  final def xPending(key: String, group: String): ZIO[RedisExecutor, RedisError, PendingInfo] =
    XPending.run((key, group, None))

  /**
   * Inspects the list of pending messages.
   *
   * @param key ID of the stream
   * @param group ID of the consumer group
   * @param idle idle time of a pending message by which message are filtered
   * @return summary about the pending messages in a given consumer group.
   */
  final def xPending(key: String, group: String, idle: Duration): ZIO[RedisExecutor, RedisError, PendingInfo] =
    XPending.run((key, group, Some(idle)))

  /**
   * Inspects the list of pending messages.
   *
   * @param key ID of the stream
   * @param group ID of the consumer group
   * @param start start of the range of IDs
   * @param end end of the range of IDs
   * @param count maximum number of messages returned
   * @param consumer ID of the consumer
   * @param idle idle time of a pending message by which message are filtered
   * @return detailed information for each message in the pending entries list.
   */
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

  /**
   * Fetches the stream entries matching a given range of IDs.
   *
   * @param key ID of the stream
   * @param start start of the range of IDs
   * @param end end of the range of IDs
   * @return the complete entries with IDs matching the specified range.
   */
  final def xRange(
    key: String,
    start: String,
    end: String
  ): ZIO[RedisExecutor, RedisError, Map[String, Map[String, String]]] =
    XRange.run((key, start, end, None))

  /**
   * Fetches the stream entries matching a given range of IDs.
   *
   * @param key ID of the stream
   * @param start start of the range of IDs
   * @param end end of the range of IDs
   * @param count maximum number of entries returned
   * @return the complete entries with IDs matching the specified range.
   */
  final def xRange(
    key: String,
    start: String,
    end: String,
    count: Long
  ): ZIO[RedisExecutor, RedisError, Map[String, Map[String, String]]] =
    XRange.run((key, start, end, Some(Count(count))))

  /**
   * Read data from one or multiple streams.
   *
   * @param count maximum number of elements returned per stream
   * @param block duration for which we want to block before timing out
   * @param stream pair that contains stream ID and the last ID that the consumer received for that stream
   * @param streams rest of the pairs
   * @return complete entries with an ID greater than the last received ID per stream.
   */
  final def xRead(count: Option[Long] = None, block: Option[Duration] = None)(
    stream: (String, String),
    streams: (String, String)*
  ): ZIO[RedisExecutor, RedisError, Map[String, Map[String, Map[String, String]]]] =
    XRead.run((count.map(Count), block, (stream, Chunk.fromIterable(streams))))

  /**
   * Read data from one or multiple streams using consumer group.
   *
   * @param group ID of the consumer group
   * @param consumer ID of the consumer
   * @param count maximum number of elements returned per stream
   * @param block duration for which we want to block before timing out
   * @param noAck flag that indicates that the read messages shouldn't be added to the pending entries list (PEL)
   * @param stream pair that contains stream ID and the last ID that the consumer received for that stream
   * @param streams rest of the pairs
   * @return complete entries with an ID greater than the last received ID per stream.
   */
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

  /**
   * Fetches the stream entries matching a given range of IDs in the reverse order.
   *
   * @param key ID of the stream
   * @param end end of the range of IDs
   * @param start start of the range of IDs
   * @return the complete entries with IDs matching the specified range in the reverse order.
   */
  final def xRevRange(
    key: String,
    end: String,
    start: String
  ): ZIO[RedisExecutor, RedisError, Map[String, Map[String, String]]] =
    XRevRange.run((key, end, start, None))

  /**
   * Fetches the stream entries matching a given range of IDs in the reverse order.
   *
   * @param key ID of the stream
   * @param end end of the range of IDs
   * @param start start of the range of IDs
   * @param count maximum number of entries returned
   * @return the complete entries with IDs matching the specified range in the reverse order.
   */
  final def xRevRange(
    key: String,
    end: String,
    start: String,
    count: Long
  ): ZIO[RedisExecutor, RedisError, Map[String, Map[String, String]]] =
    XRevRange.run((key, end, start, Some(Count(count))))

  /**
   * Trims the stream to a given number of items, evicting older items (items with lower IDs) if needed.
   *
   * @param key ID of the stream
   * @param count stream length
   * @param approximate flag that indicates if the stream length should be exactly count or few tens of entries more
   * @return the number of entries deleted from the stream.
   */
  final def xTrim(key: String, count: Long, approximate: Boolean = false): ZIO[RedisExecutor, RedisError, Long] =
    XTrim.run((key, StreamMaxLen(approximate, count)))
}

private object Streams {

  final val XAck: RedisCommand[(String, String, (String, List[String])), Long] =
    RedisCommand("XACK", Tuple3(StringInput, StringInput, NonEmptyList(StringInput)), LongOutput)

  final val XAdd
    : RedisCommand[(String, Option[StreamMaxLen], String, ((String, String), List[(String, String)])), String] =
    RedisCommand(
      "XADD",
      Tuple4(
        StringInput,
        OptionalInput(StreamMaxLenInput),
        StringInput,
        NonEmptyList(Tuple2(StringInput, StringInput))
      ),
      MultiStringOutput
    )

  final val XClaim: RedisCommand[
    (
      String,
      String,
      String,
      Duration,
      (String, List[String]),
      Option[Duration],
      Option[Duration],
      Option[Long],
      Option[WithForce]
    ),
    Map[String, Map[String, String]]
  ] = RedisCommand(
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

  final val XClaimWithJustId: RedisCommand[
    (
      String,
      String,
      String,
      Duration,
      (String, List[String]),
      Option[Duration],
      Option[Duration],
      Option[Long],
      Option[WithForce],
      WithJustId
    ),
    Chunk[String]
  ] = RedisCommand(
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

  final val XDel: RedisCommand[(String, (String, List[String])), Long] =
    RedisCommand("XDEL", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)

  final val XGroupCreate: RedisCommand[XGroupCommand.Create, Unit] =
    RedisCommand("XGROUP", XGroupCreateInput, UnitOutput)

  final val XGroupSetId: RedisCommand[XGroupCommand.SetId, Unit] = RedisCommand("XGROUP", XGroupSetIdInput, UnitOutput)

  final val XGroupDestroy: RedisCommand[XGroupCommand.Destroy, Boolean] =
    RedisCommand("XGROUP", XGroupDestroyInput, BoolOutput)

  final val XGroupCreateConsumer: RedisCommand[XGroupCommand.CreateConsumer, Unit] =
    RedisCommand("XGROUP", XGroupCreateConsumerInput, UnitOutput)

  final val XGroupDelConsumer: RedisCommand[XGroupCommand.DelConsumer, Long] =
    RedisCommand("XGROUP", XGroupDelConsumerInput, LongOutput)

  final val XInfoStreamFull: RedisCommand[XInfoCommand.Stream, StreamInfoWithFull.FullStreamInfo] =
    RedisCommand("XINFO", XInfoStreamInput, StreamInfoFullOutput)

  final val XInfoStream: RedisCommand[XInfoCommand.Stream, StreamInfo] =
    RedisCommand("XINFO", XInfoStreamInput, StreamInfoOutput)

  final val XInfoGroups: RedisCommand[XInfoCommand.Groups, Chunk[StreamGroupsInfo]] =
    RedisCommand("XINFO", XInfoGroupsInput, StreamGroupsInfoOutput)

  final val XInfoConsumers: RedisCommand[XInfoCommand.Consumers, Chunk[StreamConsumersInfo]] =
    RedisCommand("XINFO", XInfoConsumersInput, StreamConsumersInfoOutput)

  final val XLen: RedisCommand[String, Long] = RedisCommand("XLEN", StringInput, LongOutput)

  final val XPending: RedisCommand[(String, String, Option[Duration]), PendingInfo] =
    RedisCommand("XPENDING", Tuple3(StringInput, StringInput, OptionalInput(IdleInput)), XPendingOutput)

  final val XPendingMessages
    : RedisCommand[(String, String, String, String, Long, Option[String], Option[Duration]), Chunk[PendingMessage]] =
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

  final val XRange: RedisCommand[(String, String, String, Option[Count]), Map[String, Map[String, String]]] =
    RedisCommand("XRANGE", Tuple4(StringInput, StringInput, StringInput, OptionalInput(CountInput)), StreamOutput)

  final val XRead: RedisCommand[(Option[Count], Option[Duration], ((String, String), Chunk[(String, String)])), Map[
    String,
    Map[String, Map[String, String]]
  ]] =
    RedisCommand("XREAD", Tuple3(OptionalInput(CountInput), OptionalInput(BlockInput), StreamsInput), XReadOutput)

  final val XReadGroup: RedisCommand[
    (Group, Option[Count], Option[Duration], Option[NoAck], ((String, String), Chunk[(String, String)])),
    Map[String, Map[String, Map[String, String]]]
  ] = RedisCommand(
    "XREADGROUP",
    Tuple5(GroupInput, OptionalInput(CountInput), OptionalInput(BlockInput), OptionalInput(NoAckInput), StreamsInput),
    XReadOutput
  )

  final val XRevRange: RedisCommand[(String, String, String, Option[Count]), Map[String, Map[String, String]]] =
    RedisCommand("XREVRANGE", Tuple4(StringInput, StringInput, StringInput, OptionalInput(CountInput)), StreamOutput)

  final val XTrim: RedisCommand[(String, StreamMaxLen), Long] =
    RedisCommand("XTRIM", Tuple2(StringInput, StreamMaxLenInput), LongOutput)
}
