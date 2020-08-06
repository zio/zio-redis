package zio.redis

import zio.{ Chunk, ZIO }
import zio.redis.RedisError.{ ProtocolError, WrongType }
import zio.test.Assertion._
import zio.test._
import zio.duration._
import zio.test.TestAspect._

trait StringsSpec extends BaseSpec {
  val stringsSuite =
    suite("strings")(
      suite("append")(
        testM("to the end of non-empty string") {
          for {
            key <- uuid
            _   <- set(key, "val", None, None, None)
            len <- append(key, "ue")
          } yield assert(len)(equalTo(5L))
        },
        testM("to the end of empty string") {
          for {
            key <- uuid
            len <- append(key, "value")
          } yield assert(len)(equalTo(5L))
        },
        testM("empty value to the end of non-empty string") {
          for {
            key <- uuid
            _   <- set(key, "value", None, None, None)
            len <- append(key, "")
          } yield assert(len)(equalTo(5L))
        },
        testM("empty value to the end of empty string") {
          for {
            key <- uuid
            len <- append(key, "")
          } yield assert(len)(equalTo(0L))
        },
        testM("error when not string") {
          for {
            key <- uuid
            _   <- sAdd(key)("a")
            len <- append(key, "b").either
          } yield assert(len)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("bitCount")(
        testM("over non-empty string") {
          for {
            key   <- uuid
            _     <- set(key, "value", None, None, None)
            count <- bitCount(key, None)
          } yield assert(count)(equalTo(21L))
        },
        testM("over empty string") {
          for {
            key   <- uuid
            count <- bitCount(key, None)
          } yield assert(count)(equalTo(0L))
        },
        testM("error when not string") {
          for {
            key   <- uuid
            _     <- sAdd(key)("a")
            count <- bitCount(key, None).either
          } yield assert(count)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("over non-empty string with range") {
          for {
            key   <- uuid
            _     <- set(key, "value", None, None, None)
            count <- bitCount(key, Some(1 to 3))
          } yield assert(count)(equalTo(12L))
        },
        testM("over non-empty string with range that is too large") {
          for {
            key   <- uuid
            _     <- set(key, "value", None, None, None)
            count <- bitCount(key, Some(1 to 20))
          } yield assert(count)(equalTo(16L))
        },
        testM("over non-empty string with range that ends with the string") {
          for {
            key   <- uuid
            _     <- set(key, "value", None, None, None)
            count <- bitCount(key, Some(1 to -1))
          } yield assert(count)(equalTo(16L))
        },
        testM("over non-empty string with range whose start is bigger than end") {
          for {
            key   <- uuid
            _     <- set(key, "value", None, None, None)
            count <- bitCount(key, Some(3 to 1))
          } yield assert(count)(equalTo(0L))
        },
        testM("over empty string with range") {
          for {
            key   <- uuid
            count <- bitCount(key, Some(1 to 3))
          } yield assert(count)(equalTo(0L))
        },
        testM("over not string with range") {
          for {
            key   <- uuid
            _     <- sAdd(key)("a")
            count <- bitCount(key, Some(1 to 3)).either
          } yield assert(count)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("bitOp")(
        testM("AND over multiple non-empty strings") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            third  <- uuid
            _      <- set(first, "a", None, None, None)
            _      <- set(second, "ab", None, None, None)
            _      <- set(third, "abc", None, None, None)
            result <- bitOp(BitOperation.AND, dest)(first, second, third)
          } yield assert(result)(equalTo(3L))
        },
        testM("AND over two non-empty strings") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            _      <- set(first, "first", None, None, None)
            _      <- set(second, "second", None, None, None)
            result <- bitOp(BitOperation.AND, dest)(first, second)
          } yield assert(result)(equalTo(6L))
        },
        testM("AND over one empty and one non-empty string") {
          for {
            dest     <- uuid
            empty    <- uuid
            nonEmpty <- uuid
            _        <- set(nonEmpty, "value", None, None, None)
            result   <- bitOp(BitOperation.AND, dest)(empty, nonEmpty)
          } yield assert(result)(equalTo(5L))
        },
        testM("AND over two empty strings") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            result <- bitOp(BitOperation.AND, dest)(first, second)
          } yield assert(result)(equalTo(0L))
        },
        testM("error when AND over one empty and one not string") {
          for {
            dest      <- uuid
            empty     <- uuid
            notString <- uuid
            _         <- sAdd(notString)("a")
            result    <- bitOp(BitOperation.AND, dest)(empty, notString).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("error when AND over non-empty and one not string") {
          for {
            dest      <- uuid
            nonEmpty  <- uuid
            notString <- uuid
            _         <- sAdd(notString)("a")
            result    <- bitOp(BitOperation.AND, dest)(nonEmpty, notString).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("OR over multiple non-empty strings") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            third  <- uuid
            _      <- set(first, "a", None, None, None)
            _      <- set(second, "ab", None, None, None)
            _      <- set(third, "abc", None, None, None)
            result <- bitOp(BitOperation.OR, dest)(first, second, third)
          } yield assert(result)(equalTo(3L))
        },
        testM("OR over two non-empty strings") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            _      <- set(first, "first", None, None, None)
            _      <- set(second, "second", None, None, None)
            result <- bitOp(BitOperation.OR, dest)(first, second)
          } yield assert(result)(equalTo(6L))
        },
        testM("OR over one empty and one non-empty string") {
          for {
            dest     <- uuid
            empty    <- uuid
            nonEmpty <- uuid
            _        <- set(nonEmpty, "value", None, None, None)
            result   <- bitOp(BitOperation.OR, dest)(empty, nonEmpty)
          } yield assert(result)(equalTo(5L))
        },
        testM("OR over two empty strings") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            result <- bitOp(BitOperation.OR, dest)(first, second)
          } yield assert(result)(equalTo(0L))
        },
        testM("error when OR over one empty and one not string") {
          for {
            dest      <- uuid
            empty     <- uuid
            notString <- uuid
            _         <- sAdd(notString)("a")
            result    <- bitOp(BitOperation.OR, dest)(empty, notString).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("error when OR over non-empty and one not string") {
          for {
            dest      <- uuid
            nonEmpty  <- uuid
            notString <- uuid
            _         <- sAdd(notString)("a")
            result    <- bitOp(BitOperation.OR, dest)(nonEmpty, notString).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("XOR over multiple non-empty strings") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            third  <- uuid
            _      <- set(first, "a", None, None, None)
            _      <- set(second, "ab", None, None, None)
            _      <- set(third, "abc", None, None, None)
            result <- bitOp(BitOperation.XOR, dest)(first, second, third)
          } yield assert(result)(equalTo(3L))
        },
        testM("XOR over two non-empty strings") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            _      <- set(first, "first", None, None, None)
            _      <- set(second, "second", None, None, None)
            result <- bitOp(BitOperation.XOR, dest)(first, second)
          } yield assert(result)(equalTo(6L))
        },
        testM("XOR over one empty and one non-empty string") {
          for {
            dest     <- uuid
            empty    <- uuid
            nonEmpty <- uuid
            _        <- set(nonEmpty, "value", None, None, None)
            result   <- bitOp(BitOperation.XOR, dest)(empty, nonEmpty)
          } yield assert(result)(equalTo(5L))
        },
        testM("XOR over two empty strings") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            result <- bitOp(BitOperation.XOR, dest)(first, second)
          } yield assert(result)(equalTo(0L))
        },
        testM("error when XOR over one empty and one not string") {
          for {
            dest      <- uuid
            empty     <- uuid
            notString <- uuid
            _         <- sAdd(notString)("a")
            result    <- bitOp(BitOperation.XOR, dest)(empty, notString).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("error when XOR over non-empty and one not string") {
          for {
            dest      <- uuid
            nonEmpty  <- uuid
            notString <- uuid
            _         <- sAdd(notString)("a")
            result    <- bitOp(BitOperation.XOR, dest)(nonEmpty, notString).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("error when NOT over multiple non-empty strings") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            third  <- uuid
            _      <- set(first, "a", None, None, None)
            _      <- set(second, "ab", None, None, None)
            _      <- set(third, "abc", None, None, None)
            result <- bitOp(BitOperation.NOT, dest)(first, second, third).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when NOT over one non-empty and one empty string") {
          for {
            dest     <- uuid
            nonEmpty <- uuid
            empty    <- uuid
            _        <- set(nonEmpty, "a", None, None, None)
            result   <- bitOp(BitOperation.NOT, dest)(nonEmpty, empty).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("NOT over non-empty string") {
          for {
            dest   <- uuid
            key    <- uuid
            _      <- set(key, "value", None, None, None)
            result <- bitOp(BitOperation.NOT, dest)(key)
          } yield assert(result)(equalTo(5L))
        },
        testM("NOT over empty string") {
          for {
            dest   <- uuid
            key    <- uuid
            result <- bitOp(BitOperation.NOT, dest)(key)
          } yield assert(result)(equalTo(0L))
        },
        testM("error when NOT over not string") {
          for {
            dest   <- uuid
            key    <- uuid
            _      <- sAdd(key)("a")
            result <- bitOp(BitOperation.NOT, dest)(key).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("bitPos")(
        testM("of 1 when non-empty string") {
          for {
            key <- uuid
            _   <- set(key, "value", None, None, None)
            pos <- bitPos(key, true, None)
          } yield assert(pos)(equalTo(1L))
        },
        testM("of 0 when non-empty string") {
          for {
            key <- uuid
            _   <- set(key, "value", None, None, None)
            pos <- bitPos(key, false, None)
          } yield assert(pos)(equalTo(0L))
        },
        testM("of 1 when empty string") {
          for {
            key <- uuid
            pos <- bitPos(key, true, None)
          } yield assert(pos)(equalTo(-1L))
        },
        testM("of 0 when empty string") {
          for {
            key <- uuid
            pos <- bitPos(key, false, None)
          } yield assert(pos)(equalTo(0L))
        },
        testM("of 1 when non-empty string with start") {
          for {
            key <- uuid
            _   <- set(key, "value", None, None, None)
            pos <- bitPos(key, true, Some(BitPosRange(2L, None)))
          } yield assert(pos)(equalTo(17L))
        },
        testM("of 0 when non-empty string with start") {
          for {
            key <- uuid
            _   <- set(key, "value", None, None, None)
            pos <- bitPos(key, false, Some(BitPosRange(2L, None)))
          } yield assert(pos)(equalTo(16L))
        },
        testM("of 1 when start is greater than non-empty string length") {
          for {
            key <- uuid
            _   <- set(key, "value", None, None, None)
            pos <- bitPos(key, true, Some(BitPosRange(10L, None)))
          } yield assert(pos)(equalTo(-1L))
        },
        testM("of 0 when start is greater than non-empty string length") {
          for {
            key <- uuid
            _   <- set(key, "value", None, None, None)
            pos <- bitPos(key, false, Some(BitPosRange(10L, None)))
          } yield assert(pos)(equalTo(-1L))
        },
        testM("of 1 when empty string with start") {
          for {
            key <- uuid
            pos <- bitPos(key, true, Some(BitPosRange(1L, None)))
          } yield assert(pos)(equalTo(-1L))
        },
        testM("of 0 when empty string with start") {
          for {
            key <- uuid
            pos <- bitPos(key, false, Some(BitPosRange(10L, None)))
          } yield assert(pos)(equalTo(0L))
        },
        testM("of 1 when non-empty string with start and end") {
          for {
            key <- uuid
            _   <- set(key, "value", None, None, None)
            pos <- bitPos(key, true, Some(BitPosRange(2L, Some(4L))))
          } yield assert(pos)(equalTo(17L))
        },
        testM("of 0 when non-empty string with start and end") {
          for {
            key <- uuid
            _   <- set(key, "value", None, None, None)
            pos <- bitPos(key, false, Some(BitPosRange(2L, Some(4L))))
          } yield assert(pos)(equalTo(16L))
        },
        testM("of 1 when start is greater than end with non-empty string") {
          for {
            key <- uuid
            _   <- set(key, "value", None, None, None)
            pos <- bitPos(key, true, Some(BitPosRange(4L, Some(2L))))
          } yield assert(pos)(equalTo(-1L))
        },
        testM("of 0 when start is greater than end with non-empty string") {
          for {
            key <- uuid
            _   <- set(key, "value", None, None, None)
            pos <- bitPos(key, false, Some(BitPosRange(4L, Some(2L))))
          } yield assert(pos)(equalTo(-1L))
        },
        testM("of 1 when range is out of non-empty string") {
          for {
            key <- uuid
            _   <- set(key, "value", None, None, None)
            pos <- bitPos(key, true, Some(BitPosRange(10L, Some(15L))))
          } yield assert(pos)(equalTo(-1L))
        },
        testM("of 0 when range is out of non-empty string") {
          for {
            key <- uuid
            _   <- set(key, "value", None, None, None)
            pos <- bitPos(key, false, Some(BitPosRange(10L, Some(15L))))
          } yield assert(pos)(equalTo(-1L))
        },
        testM("of 1 when start is equal to end with non-empty string") {
          for {
            key <- uuid
            _   <- set(key, "value", None, None, None)
            pos <- bitPos(key, true, Some(BitPosRange(1L, Some(1L))))
          } yield assert(pos)(equalTo(9L))
        },
        testM("of 0 when start is equal to end with non-empty string") {
          for {
            key <- uuid
            _   <- set(key, "value", None, None, None)
            pos <- bitPos(key, false, Some(BitPosRange(1L, Some(1L))))
          } yield assert(pos)(equalTo(8L))
        },
        testM("of 1 when empty string with start and end") {
          for {
            key <- uuid
            pos <- bitPos(key, true, Some(BitPosRange(2L, Some(4L))))
          } yield assert(pos)(equalTo(-1L))
        },
        testM("of 0 when empty string with start and end") {
          for {
            key <- uuid
            pos <- bitPos(key, false, Some(BitPosRange(2L, Some(4L))))
          } yield assert(pos)(equalTo(0L))
        },
        testM("of 1 when start is greater than end with empty string") {
          for {
            key <- uuid
            pos <- bitPos(key, true, Some(BitPosRange(4L, Some(2L))))
          } yield assert(pos)(equalTo(-1L))
        },
        testM("of 0 when start is greater than end with empty string") {
          for {
            key <- uuid
            pos <- bitPos(key, false, Some(BitPosRange(4L, Some(2L))))
          } yield assert(pos)(equalTo(0L))
        },
        testM("error when not string") {
          for {
            key <- uuid
            _   <- sAdd(key)("a")
            pos <- bitPos(key, true, None).either
          } yield assert(pos)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("error when not string and start is greater than end") {
          for {
            key <- uuid
            _   <- sAdd(key)("a")
            pos <- bitPos(key, false, Some(BitPosRange(4L, Some(2L)))).either
          } yield assert(pos)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("decr")(
        testM("non-empty integer") {
          for {
            key    <- uuid
            _      <- set(key, "5", None, None, None)
            result <- decr(key)
          } yield assert(result)(equalTo(4L))
        },
        testM("empty integer") {
          for {
            key    <- uuid
            result <- decr(key)
          } yield assert(result)(equalTo(-1L))
        },
        testM("error when out of range integer") {
          for {
            key    <- uuid
            _      <- set(key, "234293482390480948029348230948", None, None, None)
            result <- decr(key).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when not integer") {
          for {
            key    <- uuid
            _      <- set(key, "not-integer", None, None, None)
            result <- decr(key).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        }
      ),
      suite("decrBy")(
        testM("3 when non-empty integer") {
          for {
            key    <- uuid
            _      <- set(key, "10", None, None, None)
            result <- decrBy(key, 3L)
          } yield assert(result)(equalTo(7L))
        },
        testM("3 when empty integer") {
          for {
            key    <- uuid
            result <- decrBy(key, 3L)
          } yield assert(result)(equalTo(-3L))
        },
        testM("-3 when non-empty integer") {
          for {
            key    <- uuid
            _      <- set(key, "10", None, None, None)
            result <- decrBy(key, -3L)
          } yield assert(result)(equalTo(13L))
        },
        testM("error when out of range integer") {
          for {
            key    <- uuid
            _      <- set(key, "234293482390480948029348230948", None, None, None)
            result <- decrBy(key, 3).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when not integer") {
          for {
            key    <- uuid
            _      <- set(key, "not-integer", None, None, None)
            result <- decrBy(key, 3).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        }
      ),
      suite("get")(
        testM("non-emtpy string") {
          for {
            key    <- uuid
            _      <- set(key, "value", None, None, None)
            result <- get(key)
          } yield assert(result)(isSome(equalTo("value")))
        },
        testM("emtpy string") {
          for {
            key    <- uuid
            result <- get(key)
          } yield assert(result)(isNone)
        },
        testM("error when not string") {
          for {
            key    <- uuid
            _      <- sAdd(key)("a")
            result <- get(key).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("getBit")(
        testM("from non-empty string") {
          for {
            key <- uuid
            _   <- set(key, "value", None, None, None)
            bit <- getBit(key, 17L)
          } yield assert(bit)(equalTo(1L))
        },
        testM("with offset larger then string length") {
          for {
            key <- uuid
            _   <- set(key, "value", None, None, None)
            bit <- getBit(key, 100L)
          } yield assert(bit)(equalTo(0L))
        },
        testM("with empty string") {
          for {
            key <- uuid
            bit <- getBit(key, 10L)
          } yield assert(bit)(equalTo(0L))
        },
        testM("error when negative offset") {
          for {
            key <- uuid
            bit <- getBit(key, -1L).either
          } yield assert(bit)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when not string") {
          for {
            key <- uuid
            _   <- sAdd(key)("a")
            bit <- getBit(key, 10L).either
          } yield assert(bit)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("getRange")(
        testM("from non-empty string") {
          for {
            key    <- uuid
            _      <- set(key, "value", None, None, None)
            substr <- getRange(key, 1 to 3)
          } yield assert(substr)(equalTo("alu"))
        },
        testM("with range that exceeds non-empty string length") {
          for {
            key    <- uuid
            _      <- set(key, "value", None, None, None)
            substr <- getRange(key, 1 to 10)
          } yield assert(substr)(equalTo("alue"))
        },
        testM("with range that is outside of non-empty string") {
          for {
            key    <- uuid
            _      <- set(key, "value", None, None, None)
            substr <- getRange(key, 10 to 15)
          } yield assert(substr)(equalTo(""))
        },
        testM("with inverse range of non-empty string") {
          for {
            key    <- uuid
            _      <- set(key, "value", None, None, None)
            substr <- getRange(key, 15 to 3)
          } yield assert(substr)(equalTo(""))
        },
        testM("with negative range end from non-empty string") {
          for {
            key    <- uuid
            _      <- set(key, "value", None, None, None)
            substr <- getRange(key, 1 to -1)
          } yield assert(substr)(equalTo("alue"))
        },
        testM("with start and end equal from non-empty string") {
          for {
            key    <- uuid
            _      <- set(key, "value", None, None, None)
            substr <- getRange(key, 1 to 1)
          } yield assert(substr)(equalTo("a"))
        },
        testM("from empty string") {
          for {
            key    <- uuid
            substr <- getRange(key, 1 to 3)
          } yield assert(substr)(equalTo(""))
        },
        testM("error when not string") {
          for {
            key    <- uuid
            _      <- sAdd(key)("a")
            substr <- getRange(key, 1 to 3).either
          } yield assert(substr)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("getSet")(
        testM("non-empty value to the existing string") {
          for {
            key    <- uuid
            _      <- set(key, "value", None, None, None)
            oldVal <- getSet(key, "abc")
          } yield assert(oldVal)(isSome(equalTo("value")))
        },
        testM("empty value to the existing string") {
          for {
            key    <- uuid
            _      <- set(key, "value", None, None, None)
            oldVal <- getSet(key, "")
          } yield assert(oldVal)(isSome(equalTo("value")))
        },
        testM("non-empty value to the empty string") {
          for {
            key    <- uuid
            oldVal <- getSet(key, "value")
          } yield assert(oldVal)(isNone)
        },
        testM("empty value to the empty string") {
          for {
            key    <- uuid
            oldVal <- getSet(key, "")
          } yield assert(oldVal)(isNone)
        },
        testM("error when not string") {
          for {
            key    <- uuid
            _      <- sAdd(key)("a")
            oldVal <- getSet(key, "value").either
          } yield assert(oldVal)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("incr")(
        testM("non-empty integer") {
          for {
            key    <- uuid
            _      <- set(key, "5", None, None, None)
            result <- incr(key)
          } yield assert(result)(equalTo(6L))
        },
        testM("empty integer") {
          for {
            key    <- uuid
            result <- incr(key)
          } yield assert(result)(equalTo(1L))
        },
        testM("error when out of range integer") {
          for {
            key    <- uuid
            _      <- set(key, "234293482390480948029348230948", None, None, None)
            result <- incr(key).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when not integer") {
          for {
            key    <- uuid
            _      <- set(key, "not-integer", None, None, None)
            result <- incr(key).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        }
      ),
      suite("incrBy")(
        testM("3 when non-empty integer") {
          for {
            key    <- uuid
            _      <- set(key, "10", None, None, None)
            result <- incrBy(key, 3L)
          } yield assert(result)(equalTo(13L))
        },
        testM("3 when empty integer") {
          for {
            key    <- uuid
            result <- incrBy(key, 3L)
          } yield assert(result)(equalTo(3L))
        },
        testM("-3 when non-empty integer") {
          for {
            key    <- uuid
            _      <- set(key, "10", None, None, None)
            result <- incrBy(key, -3L)
          } yield assert(result)(equalTo(7L))
        },
        testM("error when out of range integer") {
          for {
            key    <- uuid
            _      <- set(key, "234293482390480948029348230948", None, None, None)
            result <- incrBy(key, 3).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when not integer") {
          for {
            key    <- uuid
            _      <- set(key, "not-integer", None, None, None)
            result <- incrBy(key, 3).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        }
      ),
      suite("incrByFloat")(
        testM("3.4 when non-empty float") {
          for {
            key    <- uuid
            _      <- set(key, "5.1", None, None, None)
            result <- incrByFloat(key, 3.4d)
          } yield assert(result)(equalTo("8.5"))
        },
        testM("3.4 when empty float") {
          for {
            key    <- uuid
            result <- incrByFloat(key, 3.4d)
          } yield assert(result)(equalTo("3.4"))
        },
        testM("-3.4 when non-empty float") {
          for {
            key    <- uuid
            _      <- set(key, "5", None, None, None)
            result <- incrByFloat(key, -3.4d)
          } yield assert(result)(equalTo("1.6"))
        },
        testM("error when out of range value") {
          for {
            key    <- uuid
            _      <- set(key, s"${Double.MaxValue.toString}1234", None, None, None)
            result <- incrByFloat(key, 3d).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when not integer") {
          for {
            key    <- uuid
            _      <- set(key, "not-integer", None, None, None)
            result <- incrByFloat(key, 3).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        }
      ),
      suite("mGet")(
        testM("from multiple non-empty strings") {
          for {
            first  <- uuid
            second <- uuid
            _      <- set(first, "1", None, None, None)
            _      <- set(second, "2", None, None, None)
            result <- mGet(first, second)
          } yield assert(result)(equalTo(Chunk(Some("1"), Some("2"))))
        },
        testM("from one non-empty and one empty string") {
          for {
            nonEmpty <- uuid
            empty    <- uuid
            _        <- set(nonEmpty, "value", None, None, None)
            result   <- mGet(nonEmpty, empty)
          } yield assert(result)(equalTo(Chunk(Some("value"), None)))
        },
        testM("from two empty strings") {
          for {
            first  <- uuid
            second <- uuid
            result <- mGet(first, second)
          } yield assert(result)(equalTo(Chunk(None, None)))
        },
        testM("from one string and one not string") {
          for {
            str    <- uuid
            notStr <- uuid
            _      <- set(str, "value", None, None, None)
            _      <- sAdd(notStr)("a")
            result <- mGet(str, notStr)
          } yield assert(result)(equalTo(Chunk(Some("value"), None)))
        },
        testM("from one not string") {
          for {
            key    <- uuid
            _      <- sAdd(key)("a")
            result <- mGet(key)
          } yield assert(result)(equalTo(Chunk(None)))
        }
      ),
      suite("mSet")(
        testM("one new value") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- mSet((key, value))
            result <- get(key)
          } yield assert(result)(isSome(equalTo(value)))
        },
        testM("multiple new values") {
          for {
            first     <- uuid
            second    <- uuid
            firstVal  <- uuid
            secondVal <- uuid
            _         <- mSet((first, firstVal), (second, secondVal))
            result    <- mGet(first, second)
          } yield assert(result)(equalTo(Chunk(Some(firstVal), Some(secondVal))))
        },
        testM("replace existing values") {
          for {
            first       <- uuid
            second      <- uuid
            firstVal    <- uuid
            secondVal   <- uuid
            replacement <- uuid
            _           <- mSet((first, firstVal), (second, secondVal))
            _           <- mSet((first, replacement), (second, replacement))
            result      <- mGet(first, second)
          } yield assert(result)(equalTo(Chunk(Some(replacement), Some(replacement))))
        },
        testM("one value multiple times at once") {
          for {
            key       <- uuid
            firstVal  <- uuid
            secondVal <- uuid
            _         <- mSet((key, firstVal), (key, secondVal))
            result    <- get(key)
          } yield assert(result)(isSome(equalTo(secondVal)))
        },
        testM("replace not string value") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- sAdd(key)("a")
            result <- mSet((key, value)).either
          } yield assert(result)(isRight)
        }
      ),
      suite("mSetNx")(
        testM("one new value") {
          for {
            key   <- uuid
            value <- uuid
            set   <- mSetNx((key, value))
          } yield assert(set)(isTrue)
        },
        testM("multiple new values") {
          for {
            first     <- uuid
            second    <- uuid
            firstVal  <- uuid
            secondVal <- uuid
            set       <- mSetNx((first, firstVal), (second, secondVal))
          } yield assert(set)(isTrue)
        },
        testM("replace existing values") {
          for {
            first       <- uuid
            second      <- uuid
            firstVal    <- uuid
            secondVal   <- uuid
            replacement <- uuid
            _           <- mSetNx((first, firstVal), (second, secondVal))
            set         <- mSetNx((first, replacement), (second, replacement))
          } yield assert(set)(isFalse)
        },
        testM("one value multiple times at once") {
          for {
            key       <- uuid
            firstVal  <- uuid
            secondVal <- uuid
            set       <- mSetNx((key, firstVal), (key, secondVal))
          } yield assert(set)(isTrue)
        },
        testM("replace not string value") {
          for {
            key   <- uuid
            value <- uuid
            _     <- sAdd(key)("a")
            set   <- mSetNx((key, value))
          } yield assert(set)(isFalse)
        }
      ),
      suite("pSetEx")(
        testM("new value with 1000 milliseconds") {
          for {
            key          <- uuid
            value        <- uuid
            _            <- pSetEx(key, 1000.millis, value)
            existsBefore <- exists(key)
            _            <- ZIO.sleep(1010.millis)
            existsAfter  <- exists(key)
          } yield assert(existsBefore)(isTrue) && assert(existsAfter)(isFalse)
        } @@ eventually,
        testM("override existing string") {
          for {
            key        <- uuid
            value      <- uuid
            _          <- set(key, "value", None, None, None)
            _          <- pSetEx(key, 1000.millis, value)
            currentVal <- get(key)
          } yield assert(currentVal)(isSome(equalTo(value)))
        },
        testM("override not string") {
          for {
            key        <- uuid
            value      <- uuid
            _          <- sAdd(key)("a")
            _          <- pSetEx(key, 1000.millis, value)
            currentVal <- get(key)
          } yield assert(currentVal)(isSome(equalTo(value)))
        },
        testM("error when 0 milliseconds") {
          for {
            key    <- uuid
            value  <- uuid
            result <- pSetEx(key, 0.millis, value).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when negative milliseconds") {
          for {
            key    <- uuid
            value  <- uuid
            result <- pSetEx(key, (-1).millis, value).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        }
      ),
      suite("set")(
        testM("new value") {
          for {
            key    <- uuid
            value  <- uuid
            result <- set(key, value, None, None, None)
          } yield assert(result)(isSome)
        },
        testM("override existing string") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- set(key, "value", None, None, None)
            result <- set(key, value, None, None, None)
          } yield assert(result)(isSome)
        },
        testM("override not string") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- sAdd(key)("a")
            result <- set(key, value, None, None, None)
          } yield assert(result)(isSome)
        },
        testM("new value with ttl 1 second") {
          for {
            key    <- uuid
            value  <- uuid
            result <- set(key, value, Some(1.second), None, None)
          } yield assert(result)(isSome)
        },
        testM("new value with ttl 100 milliseconds") {
          for {
            key    <- uuid
            value  <- uuid
            result <- set(key, value, Some(100.milliseconds), None, None)
          } yield assert(result)(isSome)
        },
        testM("error when negative ttl") {
          for {
            key    <- uuid
            value  <- uuid
            result <- set(key, value, Some((-1).millisecond), None, None).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("new value with SetNew parameter") {
          for {
            key    <- uuid
            value  <- uuid
            result <- set(key, value, None, Some(Update.SetNew), None)
          } yield assert(result)(isSome)
        },
        testM("existing value with SetNew parameter") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- set(key, "value", None, None, None)
            result <- set(key, value, None, Some(Update.SetNew), None)
          } yield assert(result)(isNone)
        },
        testM("new value with SetExisting parameter") {
          for {
            key    <- uuid
            value  <- uuid
            result <- set(key, value, None, Some(Update.SetExisting), None)
          } yield assert(result)(isNone)
        },
        testM("existing value with SetExisting parameter") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- set(key, "value", None, None, None)
            result <- set(key, value, None, Some(Update.SetExisting), None)
          } yield assert(result)(isSome)
        },
        testM("existing not string value with SetExisting parameter") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- sAdd(key)("a")
            result <- set(key, value, None, Some(Update.SetExisting), None)
          } yield assert(result)(isSome)
        },
        // next three tests include KEEPTTL parameter that is valid for Redis version >= 6
        testM("new value with KeepTtl parameter") {
          for {
            key    <- uuid
            value  <- uuid
            result <- set(key, value, None, None, Some(KeepTtl))
          } yield assert(result)(isSome)
        } @@ ignore,
        testM("existing value with KeepTtl parameter") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- set(key, "value", Some(1.second), None, None)
            result <- set(key, value, None, None, Some(KeepTtl))
          } yield assert(result)(isSome)
        } @@ ignore,
        testM("existing value with both ttl and KeepTtl parameters") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- set(key, "value", Some(1.second), None, None)
            result <- set(key, value, Some(1.second), None, Some(KeepTtl))
          } yield assert(result)(isSome)
        } @@ ignore
      ),
      suite("setBit")(
        testM("for existing key") {
          for {
            key    <- uuid
            _      <- set(key, "value", None, None, None)
            oldBit <- setBit(key, 16L, true)
          } yield assert(oldBit)(isFalse)
        },
        testM("for non-existent key") {
          for {
            key    <- uuid
            oldBit <- setBit(key, 16L, true)
          } yield assert(oldBit)(isFalse)
        },
        testM("for offset that is out of string range") {
          for {
            key    <- uuid
            _      <- set(key, "value", None, None, None)
            oldBit <- setBit(key, 100L, true)
          } yield assert(oldBit)(isFalse)
        },
        testM("error when negative offset") {
          for {
            key    <- uuid
            _      <- set(key, "value", None, None, None)
            oldBit <- setBit(key, -1L, false).either
          } yield assert(oldBit)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when not string") {
          for {
            key    <- uuid
            _      <- sAdd(key)("a")
            oldBit <- setBit(key, 10L, true).either
          } yield assert(oldBit)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("setEx")(
        testM("new value with 1 second ttl") {
          for {
            key          <- uuid
            value        <- uuid
            _            <- setEx(key, 1.second, value)
            existsBefore <- exists(key)
            _            <- ZIO.sleep(1010.millis)
            existsAfter  <- exists(key)
          } yield assert(existsBefore)(isTrue) && assert(existsAfter)(isFalse)
        } @@ eventually,
        testM("existing value with 1 second ttl") {
          for {
            key          <- uuid
            value        <- uuid
            _            <- set(key, "value", None, None, None)
            _            <- setEx(key, 1.second, value)
            existsBefore <- exists(key)
            _            <- ZIO.sleep(1010.millis)
            existsAfter  <- exists(key)
          } yield assert(existsBefore)(isTrue) && assert(existsAfter)(isFalse)
        } @@ eventually,
        testM("override when not string") {
          for {
            key          <- uuid
            value        <- uuid
            _            <- sAdd(key)("a")
            _            <- setEx(key, 1.second, value)
            existsBefore <- exists(key)
            _            <- ZIO.sleep(1010.millis)
            existsAfter  <- exists(key)
          } yield assert(existsBefore)(isTrue) && assert(existsAfter)(isFalse)
        },
        testM("error when 0 seconds ttl") {
          for {
            key    <- uuid
            value  <- uuid
            result <- setEx(key, 0.seconds, value).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when negative ttl") {
          for {
            key    <- uuid
            value  <- uuid
            result <- setEx(key, (-1).second, value).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        }
      ),
      suite("setNx")(
        testM("new value") {
          for {
            key    <- uuid
            value  <- uuid
            result <- setNx(key, value)
          } yield assert(result)(isTrue)
        },
        testM("existing value") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- set(key, "value", None, None, None)
            result <- setNx(key, value)
          } yield assert(result)(isFalse)
        },
        testM("not string") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- sAdd(key)("a")
            result <- setNx(key, value)
          } yield assert(result)(isFalse)
        }
      )
    )
}
