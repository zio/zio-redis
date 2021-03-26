package zio.redis.api

import zio.duration._
import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.{ Chunk, ZIO }

trait Lists {
  import Lists._

  /**
   * Pops an element from the list stored at source, pushes it to the list stored at destination; or block until one
   * is available. This is the blocking variant of [[zio.redis.api.Lists#rPopLPush]]
   *
   * @param source      the key identifier of the source list
   * @param destination the key identifier of the target list
   * @param timeout     the maximum time to wait for an element to be available.
   *                    A timeout of zero can be used to block indefinitely
   * @return the element being popped from source and pushed to destination.
   *         If timeout is reached, an empty reply is returned
   */
  final def brPopLPush(
    source: String,
    destination: String,
    timeout: Duration
  ): ZIO[RedisExecutor, RedisError, Option[String]] =
    BrPopLPush.run((source, destination, timeout))

  /**
   * Returns the element at index in the list stored at key
   *
   * @param key   they key identifier
   * @param index the requested index. It is zero-based, so 0 means the first element, 1 the second element and so on.
   *              Negative indices can be used to designate elements starting at the tail of the list
   * @return the requested element, or empty if the index is out of range
   */
  final def lIndex(key: String, index: Long): ZIO[RedisExecutor, RedisError, Option[String]] = LIndex.run((key, index))

  /**
   * Returns the length of the list stored at key
   *
   * @param key the key identifier. If key does not exist, it is interpreted as an empty list and 0 is returned
   * @return the length of the list at key
   */
  final def lLen(key: String): ZIO[RedisExecutor, RedisError, Long] = LLen.run(key)

  /**
   * Removes and returns the first element of the list stored at key
   *
   * @param key the key identifier
   * @return the value of the first element, or empty when key does not exist
   */
  final def lPop(key: String): ZIO[RedisExecutor, RedisError, Option[String]] = LPop.run(key)

  /**
   * Prepends one or multiple elements to the list stored at key. If key does not exist, it is created as empty list
   * before performing the push operations
   *
   * @param key      the key identifier
   * @param element  the first element to prepend
   * @param elements the rest of elements to prepend
   * @return the length of the list after the push operation
   */
  final def lPush(key: String, element: String, elements: String*): ZIO[RedisExecutor, RedisError, Long] =
    LPush.run((key, (element, elements.toList)))

  /**
   * Prepends an element to a list, only if the list exists. In contrary to [[zio.redis.api.Lists#lPush]], no
   * operation will be performed when key does not yet exist
   *
   * @param key      the key identifier
   * @param element  the first element to prepend
   * @param elements the rest of elements to prepends
   * @return the length of the list after the push operation
   */
  final def lPushX(key: String, element: String, elements: String*): ZIO[RedisExecutor, RedisError, Long] =
    LPushX.run((key, (element, elements.toList)))

  /**
   * Gets a range of elements from the list stored at key
   *
   * @param key   the key identifier
   * @param range the range of elements to retrieve. The range should be zero-based, with 0 being the first element of
   *              the list (the head of the list), 1 being the next element and so on
   * @return a chunk of elements in the specified range
   */
  final def lRange(key: String, range: Range): ZIO[RedisExecutor, RedisError, Chunk[String]] = LRange.run((key, range))

  /**
   * Removes the first count occurrences of element from the list stored at key.
   * The count argument influences the operation in the following ways:
   *  - count > 0: Remove elements equal to element moving from head to tail.
   *  - count < 0: Remove elements equal to element moving from tail to head.
   *  - count = 0: Remove all elements equal to element.
   *
   * @param key     the key identifier. Non-existing keys are treated like empty lists, so when key does not exist, the
   *                command will always return 0
   * @param count   the number of elements to remove
   * @param element the element to be removed
   * @return the number of removed elements
   */
  final def lRem(key: String, count: Long, element: String): ZIO[RedisExecutor, RedisError, Long] =
    LRem.run((key, count, element))

  /**
   * Sets the list element at index to element
   *
   * @param key     the key identifier
   * @param index   the requested index. The index is zero-based, so 0 means the first element, 1 the second element
   *                and so on
   * @param element the value to be inserted
   * @return the Unit value
   */
  final def lSet(key: String, index: Long, element: String): ZIO[RedisExecutor, RedisError, Unit] =
    LSet.run((key, index, element))

  /**
   * Trims an existing list so that it will contain only the specified range of elements
   *
   * @param key   the key identifier
   * @param range the range of elements to trim. The range should be zero-based, with 0 being the first element of the
   *              list (the head of the list), 1 being the next element and so on
   * @return the Unit value
   */
  final def lTrim(key: String, range: Range): ZIO[RedisExecutor, RedisError, Unit] = LTrim.run((key, range))

  /**
   * Removes and returns the last element in the list stored at key
   *
   * @param key the key identifier
   * @return the value of the last element, or empty when key does not exist
   */
  final def rPop(key: String): ZIO[RedisExecutor, RedisError, Option[String]] = RPop.run(key)

  /**
   * Atomically removes the last element in the list stored at source, prepends it to the list stored at destination
   * and returns it. If source and destination are the same, the operation is equivalent to removing the last element
   * from the list and pushing it as first element of the same list, so it can be considered as a list rotation command
   *
   * @param source      the key identifier of the source list
   * @param destination the key identifier of the destination list
   * @return the element being popped and pushed.
   *         If source does not exist, empty is returned and no operation is performed
   */
  final def rPopLPush(source: String, destination: String): ZIO[RedisExecutor, RedisError, Option[String]] =
    RPopLPush.run((source, destination))

  /**
   * Appends one or more elements to the list stored at key. If key does not exist, it is created as empty list before
   * performing the push operation
   *
   * @param key      the key identifier
   * @param element  the first element to append
   * @param elements the rest of elements to append
   * @return the length of the list after the push operation
   */
  final def rPush(key: String, element: String, elements: String*): ZIO[RedisExecutor, RedisError, Long] =
    RPush.run((key, (element, elements.toList)))

  /**
   * Appends on or multiple elements to the list stored at key, only if the list exists.
   * In contrary to [[zio.redis.api.Lists#rPush]], no operation will be performed when key does not yet exist
   *
   * @param key      the key identifier
   * @param element  the first element to append
   * @param elements the rest of elements to append
   * @return the length of the list after the push operation
   */
  final def rPushX(key: String, element: String, elements: String*): ZIO[RedisExecutor, RedisError, Long] =
    RPushX.run((key, (element, elements.toList)))

  /**
   * Removes and gets the first element in a list, or blocks until one is available. An element is popped from the head
   * of the first list that is non-empty, with the given keys being checked in the order that they are given
   *
   * @param key     the key identifier of the first list to be checked
   * @param keys    the key identifiers of the rest of the lists
   * @param timeout the maximum time to wait until an element is available.
   *                A timeout of zero can be used to block indefinitely
   * @return a tuple with the first element being the name of the key where an element was popped and the second element
   *         being the value of the popped element.
   *         An empty value is returned when no element could be popped and the timeout expired
   */
  final def blPop(key: String, keys: String*)(
    timeout: Duration
  ): ZIO[RedisExecutor, RedisError, Option[(String, String)]] =
    BlPop.run(((key, keys.toList), timeout))

  /**
   * Removes and gets the last element in a list, or block until one is available. An element is popped from the tail
   * of the first list that is non-empty, with the given keys being checked in the order that they are given
   *
   * @param key     the key identifier of the first list to be checked
   * @param keys    the key identifiers of the rest of the lists
   * @param timeout the maximum time to wait until an element is available.
   *                A timeout of zero can be used to block indefinitely
   * @return a tuple with the first element being the name of the key where an element was popped and the second element
   *         being the value of the popped element.
   *         An empty value is returned when no element could be popped and the timeout expired
   */
  final def brPop(key: String, keys: String*)(
    timeout: Duration
  ): ZIO[RedisExecutor, RedisError, Option[(String, String)]] =
    BrPop.run(((key, keys.toList), timeout))

  /**
   * Inserts element in the list stored at key either before or after the reference value pivot
   *
   * @param key      the key identifier
   * @param position the position in which the element will be inserted
   * @param pivot    the reference value
   * @param element  the value to be inserted
   * @return the length of the list after the insert operation, or -1 when the value pivot was not found
   */
  final def lInsert(
    key: String,
    position: Position,
    pivot: String,
    element: String
  ): ZIO[RedisExecutor, RedisError, Long] =
    LInsert.run((key, position, pivot, element))

  /**
   * Atomically returns and removes the first/last element (head/tail depending on the
   * wherefrom argument) of the list stored at source, and pushes the element at the
   * first/last element (head/tail depending on the whereto argument) of the list
   * stored at destination.
   *
   * @param source            the key where the element is removed
   * @param destination       the key where the element is inserted
   * @param sourceSide        the side where the element is removed
   * @param destinationSide   the side where the element is inserted
   * @return the element which is moved or nil when the source is empty
   */
  final def lMove(
    source: String,
    destination: String,
    sourceSide: Side,
    destinationSide: Side
  ): ZIO[RedisExecutor, RedisError, Option[String]] =
    LMove.run((source, destination, sourceSide, destinationSide))

  /**
   * BLMOVE is the blocking variant of LMOVE. When source contains elements, this command
   * behaves exactly like LMOVE. When used inside a MULTI/EXEC block, this command behaves
   * exactly like [[zio.redis.api.Lists#lMove]]. When source is empty, Redis will block the connection until another
   * client pushes to it or until timeout is reached. A timeout of zero can be used to
   * block indefinitely.
   *
   * @param source            the key where the element is removed
   * @param destination       the key where the element is inserted
   * @param sourceSide        the side where the element is removed
   * @param destinationSide   the side where the element is inserted
   * @param timeout           the timeout in seconds
   * @return the element which is moved or nil when the timeout is reached
   */
  final def blMove(
    source: String,
    destination: String,
    sourceSide: Side,
    destinationSide: Side,
    timeout: Duration
  ): ZIO[RedisExecutor, RedisError, Option[String]] =
    BlMove.run((source, destination, sourceSide, destinationSide, timeout))

  /**
   * The command returns the index of matching elements inside a Redis list. By default, when no
   * options are given, it will scan the list from head to tail, looking for the first match
   * of "element". If the element is found, its index (the zero-based position in the list)
   * is returned. Otherwise, if no match is found, NULL is returned.
   *
   * @param key       the key identifier
   * @param element   the element to search for
   * @param rank      the rank of the element
   * @param count     return up count element indexes
   * @param maxLen    limit the number of performed comparisons
   * @return Either an interger or an array depending on the count option
   */
  final def lPos(
    key: String,
    element: String,
    rank: Option[Rank] = None,
    maxLen: Option[ListMaxLen] = None
  ): ZIO[RedisExecutor, RedisError, Option[Long]] =
    LPos.run((key, element, rank, maxLen))

  /**
   * The command returns the index of matching elements inside a Redis list. By default, when no
   * options are given, it will scan the list from head to tail, looking for the first match
   * of "element". If the element is found, its index (the zero-based position in the list)
   * is returned. Otherwise, if no match is found, NULL is returned.
   *
   * @param key       the key identifier
   * @param element   the element to search for
   * @param rank      the rank of the element
   * @param count     return up count element indexes
   * @param maxLen    limit the number of performed comparisons
   * @return Either an interger or an array depending on the count option
   */
  final def lPosCount(
    key: String,
    element: String,
    count: Count,
    rank: Option[Rank] = None,
    maxLen: Option[ListMaxLen] = None
  ): ZIO[RedisExecutor, RedisError, Chunk[Long]] =
    LPosCount.run((key, element, count, rank, maxLen))
}

private[redis] object Lists {
  final val BrPopLPush: RedisCommand[(String, String, Duration), Option[String]] =
    RedisCommand(
      "BRPOPLPUSH",
      Tuple3(StringInput, StringInput, DurationSecondsInput),
      OptionalOutput(MultiStringOutput)
    )

  final val LIndex: RedisCommand[(String, Long), Option[String]] =
    RedisCommand("LINDEX", Tuple2(StringInput, LongInput), OptionalOutput(MultiStringOutput))

  final val LLen: RedisCommand[String, Long] = RedisCommand("LLEN", StringInput, LongOutput)

  final val LPop: RedisCommand[String, Option[String]] =
    RedisCommand("LPOP", StringInput, OptionalOutput(MultiStringOutput))

  final val LPush: RedisCommand[(String, (String, List[String])), Long] =
    RedisCommand("LPUSH", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)

  final val LPushX: RedisCommand[(String, (String, List[String])), Long] =
    RedisCommand("LPUSHX", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)

  final val LRange: RedisCommand[(String, Range), Chunk[String]] =
    RedisCommand("LRANGE", Tuple2(StringInput, RangeInput), ChunkOutput)

  final val LRem: RedisCommand[(String, Long, String), Long] =
    RedisCommand("LREM", Tuple3(StringInput, LongInput, StringInput), LongOutput)

  final val LSet: RedisCommand[(String, Long, String), Unit] =
    RedisCommand("LSET", Tuple3(StringInput, LongInput, StringInput), UnitOutput)

  final val LTrim: RedisCommand[(String, Range), Unit] =
    RedisCommand("LTRIM", Tuple2(StringInput, RangeInput), UnitOutput)

  final val RPop: RedisCommand[String, Option[String]] =
    RedisCommand("RPOP", StringInput, OptionalOutput(MultiStringOutput))

  final val RPopLPush: RedisCommand[(String, String), Option[String]] =
    RedisCommand("RPOPLPUSH", Tuple2(StringInput, StringInput), OptionalOutput(MultiStringOutput))

  final val RPush: RedisCommand[(String, (String, List[String])), Long] =
    RedisCommand("RPUSH", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)

  final val RPushX: RedisCommand[(String, (String, List[String])), Long] =
    RedisCommand("RPUSHX", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)

  final val BlPop: RedisCommand[((String, List[String]), Duration), Option[(String, String)]] =
    RedisCommand("BLPOP", Tuple2(NonEmptyList(StringInput), DurationSecondsInput), KeyElemOutput)

  final val BrPop: RedisCommand[((String, List[String]), Duration), Option[(String, String)]] =
    RedisCommand("BRPOP", Tuple2(NonEmptyList(StringInput), DurationSecondsInput), KeyElemOutput)

  final val LInsert: RedisCommand[(String, Position, String, String), Long] =
    RedisCommand("LINSERT", Tuple4(StringInput, PositionInput, StringInput, StringInput), LongOutput)

  final val LMove: RedisCommand[(String, String, Side, Side), Option[String]] =
    RedisCommand("LMOVE", Tuple4(StringInput, StringInput, SideInput, SideInput), OptionalOutput(MultiStringOutput))

  final val BlMove: RedisCommand[(String, String, Side, Side, Duration), Option[String]] =
    RedisCommand(
      "BLMOVE",
      Tuple5(StringInput, StringInput, SideInput, SideInput, DurationSecondsInput),
      OptionalOutput(MultiStringOutput)
    )

  final val LPos: RedisCommand[(String, String, Option[Rank], Option[ListMaxLen]), Option[Long]] =
    RedisCommand(
      "LPOS",
      Tuple4(
        StringInput,
        StringInput,
        OptionalInput(RankInput),
        OptionalInput(ListMaxLenInput)
      ),
      OptionalOutput(LongOutput)
    )

  final val LPosCount: RedisCommand[(String, String, Count, Option[Rank], Option[ListMaxLen]), Chunk[Long]] =
    RedisCommand(
      "LPOS",
      Tuple5(
        StringInput,
        StringInput,
        CountInput,
        OptionalInput(RankInput),
        OptionalInput(ListMaxLenInput)
      ),
      ChunkLongOutput
    )
}
