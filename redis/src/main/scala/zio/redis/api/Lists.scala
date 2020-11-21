package zio.redis.api

import java.time.Duration

import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.{ Chunk, ZIO }

trait Lists {
  import Lists._

  /** Pops an element from the list stored at source, pushes it to the list stored at destination; or block until one
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

  /** Returns the element at index in the list stored at key
   *
   * @param key   they key identifier
   * @param index the requested index. It is zero-based, so 0 means the first element, 1 the second element and so on.
   *              Negative indices can be used to designate elements starting at the tail of the list
   * @return the requested element, or empty if the index is out of range
   */
  final def lIndex(key: String, index: Long): ZIO[RedisExecutor, RedisError, Option[String]] = LIndex.run((key, index))

  /** Returns the length of the list stored at key
   *
   * @param key the key identifier. If key does not exist, it is interpreted as an empty list and 0 is returned
   * @return the length of the list at key
   */
  final def lLen(key: String): ZIO[RedisExecutor, RedisError, Long] = LLen.run(key)

  /** Removes and returns the first element of the list stored at key
   *
   * @param key the key identifier
   * @return the value of the first element, or empty when key does not exist
   */
  final def lPop(key: String): ZIO[RedisExecutor, RedisError, Option[String]] = LPop.run(key)

  /** Prepends one or multiple elements to the list stored at key. If key does not exist, it is created as empty list
   * before performing the push operations
   *
   * @param key      the key identifier
   * @param element  the first element to prepend
   * @param elements the rest of elements to prepend
   * @return the length of the list after the push operation
   */
  final def lPush(key: String, element: String, elements: String*): ZIO[RedisExecutor, RedisError, Long] =
    LPush.run((key, (element, elements.toList)))

  /** Prepends an element to a list, only if the list exists. In contrary to [[zio.redis.api.Lists#lPush]], no
   * operation will be performed when key does not yet exist
   *
   * @param key      the key identifier
   * @param element  the first element to prepend
   * @param elements the rest of elements to prepends
   * @return the length of the list after the push operation
   */
  final def lPushX(key: String, element: String, elements: String*): ZIO[RedisExecutor, RedisError, Long] =
    LPushX.run((key, (element, elements.toList)))

  /** Gets a range of elements from the list stored at key
   *
   * @param key   the key identifier
   * @param range the range of elements to retrieve. The range should be zero-based, with 0 being the first element of
   *              the list (the head of the list), 1 being the next element and so on
   * @return a chunk of elements in the specified range
   */
  final def lRange(key: String, range: Range): ZIO[RedisExecutor, RedisError, Chunk[String]] = LRange.run((key, range))

  /** Removes the first count occurrences of element from the list stored at key.
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

  /** Sets the list element at index to element
   *
   * @param key     the key identifier
   * @param index   the requested index. The index is zero-based, so 0 means the first element, 1 the second element
   *                and so on
   * @param element the value to be inserted
   * @return the Unit value
   */
  final def lSet(key: String, index: Long, element: String): ZIO[RedisExecutor, RedisError, Unit] =
    LSet.run((key, index, element))

  /** Trims an existing list so that it will contain only the specified range of elements
   *
   * @param key   the key identifier
   * @param range the range of elements to trim. The range should be zero-based, with 0 being the first element of the
   *              list (the head of the list), 1 being the next element and so on
   * @return the Unit value
   */
  final def lTrim(key: String, range: Range): ZIO[RedisExecutor, RedisError, Unit] = LTrim.run((key, range))

  /** Removes and returns the last element in the list stored at key
   *
   * @param key the key identifier
   * @return the value of the last element, or empty when key does not exist
   */
  final def rPop(key: String): ZIO[RedisExecutor, RedisError, Option[String]] = RPop.run(key)

  /** Atomically removes the last element in the list stored at source, prepends it to the list stored at destination
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

  /** Appends one or more elements to the list stored at key. If key does not exist, it is created as empty list before
   * performing the push operation
   *
   * @param key      the key identifier
   * @param element  the first element to append
   * @param elements the rest of elements to append
   * @return the length of the list after the push operation
   */
  final def rPush(key: String, element: String, elements: String*): ZIO[RedisExecutor, RedisError, Long] =
    RPush.run((key, (element, elements.toList)))

  /** Appends on or multiple elements to the list stored at key, only if the list exists.
   * In contrary to [[zio.redis.api.Lists#rPush]], no operation will be performed when key does not yet exist
   *
   * @param key      the key identifier
   * @param element  the first element to append
   * @param elements the rest of elements to append
   * @return the length of the list after the push operation
   */
  final def rPushX(key: String, element: String, elements: String*): ZIO[RedisExecutor, RedisError, Long] =
    RPushX.run((key, (element, elements.toList)))

  /** Removes and gets the first element in a list, or blocks until one is available. An element is popped from the head
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

  /** Removes and gets the last element in a list, or block until one is available. An element is popped from the tail
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

  /** Inserts element in the list stored at key either before or after the reference value pivot
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
}

private[redis] object Lists {
  final val BrPopLPush =
    RedisCommand(
      "BRPOPLPUSH",
      Tuple3(StringInput, StringInput, DurationSecondsInput),
      OptionalOutput(MultiStringOutput)
    )

  final val LIndex = RedisCommand("LINDEX", Tuple2(StringInput, LongInput), OptionalOutput(MultiStringOutput))
  final val LLen   = RedisCommand("LLEN", StringInput, LongOutput)
  final val LPop   = RedisCommand("LPOP", StringInput, OptionalOutput(MultiStringOutput))
  final val LPush  = RedisCommand("LPUSH", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  final val LPushX = RedisCommand("LPUSHX", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  final val LRange = RedisCommand("LRANGE", Tuple2(StringInput, RangeInput), ChunkOutput)
  final val LRem   = RedisCommand("LREM", Tuple3(StringInput, LongInput, StringInput), LongOutput)
  final val LSet   = RedisCommand("LSET", Tuple3(StringInput, LongInput, StringInput), UnitOutput)
  final val LTrim  = RedisCommand("LTRIM", Tuple2(StringInput, RangeInput), UnitOutput)
  final val RPop   = RedisCommand("RPOP", StringInput, OptionalOutput(MultiStringOutput))

  final val RPopLPush =
    RedisCommand("RPOPLPUSH", Tuple2(StringInput, StringInput), OptionalOutput(MultiStringOutput))

  final val RPush  = RedisCommand("RPUSH", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  final val RPushX = RedisCommand("RPUSHX", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  final val BlPop  = RedisCommand("BLPOP", Tuple2(NonEmptyList(StringInput), DurationSecondsInput), KeyElemOutput)
  final val BrPop  = RedisCommand("BRPOP", Tuple2(NonEmptyList(StringInput), DurationSecondsInput), KeyElemOutput)

  final val LInsert =
    RedisCommand("LINSERT", Tuple4(StringInput, PositionInput, StringInput, StringInput), LongOutput)
}
