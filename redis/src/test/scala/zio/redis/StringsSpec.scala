package zio.redis

import zio._
import zio.redis.RedisError.{ProtocolError, WrongType}
import zio.test.Assertion.{exists => _, _}
import zio.test.TestAspect.{eventually, ignore}
import zio.test._

trait StringsSpec extends BaseSpec {
  def stringsSuite: Spec[Redis with TestEnvironment, RedisError] =
    suite("strings")(
      suite("append")(
        test("to the end of non-empty string") {
          for {
            key <- uuid
            _   <- set(key, "val")
            len <- append(key, "ue")
          } yield assert(len)(equalTo(5L))
        },
        test("to the end of empty string") {
          for {
            key <- uuid
            len <- append(key, "value")
          } yield assert(len)(equalTo(5L))
        },
        test("empty value to the end of non-empty string") {
          for {
            key <- uuid
            _   <- set(key, "value")
            len <- append(key, "")
          } yield assert(len)(equalTo(5L))
        },
        test("empty value to the end of empty string") {
          for {
            key <- uuid
            len <- append(key, "")
          } yield assert(len)(equalTo(0L))
        },
        test("error when not string") {
          for {
            key <- uuid
            _   <- sAdd(key, "a")
            len <- append(key, "b").either
          } yield assert(len)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("bitCount")(
        test("over non-empty string") {
          for {
            key   <- uuid
            _     <- set(key, "value")
            count <- bitCount(key)
          } yield assert(count)(equalTo(21L))
        },
        test("over empty string") {
          for {
            key   <- uuid
            count <- bitCount(key)
          } yield assert(count)(equalTo(0L))
        },
        test("error when not string") {
          for {
            key   <- uuid
            _     <- sAdd(key, "a")
            count <- bitCount(key).either
          } yield assert(count)(isLeft(isSubtype[WrongType](anything)))
        },
        test("over non-empty string with range") {
          for {
            key   <- uuid
            _     <- set(key, "value")
            count <- bitCount(key, Some(1 to 3))
          } yield assert(count)(equalTo(12L))
        },
        test("over non-empty string with range that is too large") {
          for {
            key   <- uuid
            _     <- set(key, "value")
            count <- bitCount(key, Some(1 to 20))
          } yield assert(count)(equalTo(16L))
        },
        test("over non-empty string with range that ends with the string") {
          for {
            key   <- uuid
            _     <- set(key, "value")
            count <- bitCount(key, Some(1 to -1))
          } yield assert(count)(equalTo(16L))
        },
        test("over non-empty string with range whose start is bigger than end") {
          for {
            key   <- uuid
            _     <- set(key, "value")
            count <- bitCount(key, Some(3 to 1))
          } yield assert(count)(equalTo(0L))
        },
        test("over empty string with range") {
          for {
            key   <- uuid
            count <- bitCount(key, Some(1 to 3))
          } yield assert(count)(equalTo(0L))
        },
        test("over not string with range") {
          for {
            key   <- uuid
            _     <- sAdd(key, "a")
            count <- bitCount(key, Some(1 to 3)).either
          } yield assert(count)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("bitField")(
        test("get second byte with signed 8 type from existing value") {
          for {
            key    <- uuid
            _      <- set(key, "value")
            result <- bitField(key, BitFieldCommand.BitFieldGet(BitFieldType.SignedInt(8), 8))
          } yield assert(result)(equalTo(Chunk(Some(97L))))
        },
        test("get second byte with unsigned 8 type from existing value") {
          for {
            key    <- uuid
            _      <- set(key, "value")
            result <- bitField(key, BitFieldCommand.BitFieldGet(BitFieldType.UnsignedInt(8), 8))
          } yield assert(result)(equalTo(Chunk(Some(97L))))
        },
        test("get bit with offset out of range from existing value") {
          for {
            key    <- uuid
            _      <- set(key, "value")
            result <- bitField(key, BitFieldCommand.BitFieldGet(BitFieldType.UnsignedInt(1), 100))
          } yield assert(result)(equalTo(Chunk(Some(0L))))
        },
        test("get bit when empty string") {
          for {
            key    <- uuid
            result <- bitField(key, BitFieldCommand.BitFieldGet(BitFieldType.UnsignedInt(1), 10))
          } yield assert(result)(equalTo(Chunk(Some(0L))))
        },
        test("get error when negative offset") {
          for {
            key    <- uuid
            _      <- set(key, "value")
            result <- bitField(key, BitFieldCommand.BitFieldGet(BitFieldType.UnsignedInt(1), -10)).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("get error when not string") {
          for {
            key    <- uuid
            _      <- sAdd(key, "a")
            result <- bitField(key, BitFieldCommand.BitFieldGet(BitFieldType.UnsignedInt(1), 10)).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        },
        test("set second byte when non-empty string") {
          for {
            key    <- uuid
            _      <- set(key, "vblue")
            result <- bitField(key, BitFieldCommand.BitFieldSet(BitFieldType.UnsignedInt(8), 8, 97))
          } yield assert(result)(equalTo(Chunk(Some(98L))))
        },
        test("set bit when offset out of range") {
          for {
            key    <- uuid
            _      <- set(key, "value")
            result <- bitField(key, BitFieldCommand.BitFieldSet(BitFieldType.UnsignedInt(8), 100, 97))
          } yield assert(result)(equalTo(Chunk(Some(0L))))
        },
        test("set negative value when unsigned type and existing string") {
          for {
            key    <- uuid
            _      <- set(key, "value")
            result <- bitField(key, BitFieldCommand.BitFieldSet(BitFieldType.UnsignedInt(4), 10, -10))
          } yield assert(result)(equalTo(Chunk(Some(8L))))
        },
        test("set too big value when unsigned type and existing string") {
          for {
            key    <- uuid
            _      <- set(key, "value")
            result <- bitField(key, BitFieldCommand.BitFieldSet(BitFieldType.UnsignedInt(2), 10, 100))
          } yield assert(result)(equalTo(Chunk(Some(2L))))
        },
        test("set error when negative offset") {
          for {
            key    <- uuid
            _      <- set(key, "value")
            result <- bitField(key, BitFieldCommand.BitFieldSet(BitFieldType.UnsignedInt(1), -10, 10)).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("set error when not string") {
          for {
            key    <- uuid
            _      <- sAdd(key, "a")
            result <- bitField(key, BitFieldCommand.BitFieldSet(BitFieldType.UnsignedInt(8), 8, 97)).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        },
        test("increment byte when non-empty string") {
          for {
            key    <- uuid
            _      <- set(key, "value")
            result <- bitField(key, BitFieldCommand.BitFieldIncr(BitFieldType.UnsignedInt(8), 8, 3))
          } yield assert(result)(equalTo(Chunk(Some(100L))))
        },
        test("increment byte by negative value when non-empty string") {
          for {
            key    <- uuid
            _      <- set(key, "value")
            result <- bitField(key, BitFieldCommand.BitFieldIncr(BitFieldType.UnsignedInt(8), 8, -2))
          } yield assert(result)(equalTo(Chunk(Some(95L))))
        },
        test("increment bit when offset out of range") {
          for {
            key    <- uuid
            _      <- set(key, "value")
            result <- bitField(key, BitFieldCommand.BitFieldIncr(BitFieldType.UnsignedInt(1), 100, 1))
          } yield assert(result)(equalTo(Chunk(Some(1L))))
        },
        test("increment bit when value will overflow") {
          for {
            key    <- uuid
            _      <- set(key, "value")
            result <- bitField(key, BitFieldCommand.BitFieldIncr(BitFieldType.UnsignedInt(8), 8, 200))
          } yield assert(result)(equalTo(Chunk(Some(41L))))
        },
        test("increment error when negative offset") {
          for {
            key    <- uuid
            _      <- set(key, "value")
            result <- bitField(key, BitFieldCommand.BitFieldIncr(BitFieldType.UnsignedInt(1), -10, 1)).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("increment error when not string") {
          for {
            key    <- uuid
            _      <- sAdd(key, "a")
            result <- bitField(key, BitFieldCommand.BitFieldIncr(BitFieldType.UnsignedInt(1), 10, 1)).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        },
        test("increment with overflow saturation") {
          for {
            key <- uuid
            _   <- set(key, "value")
            result <- bitField(
                        key,
                        BitFieldCommand.BitFieldOverflow.Sat,
                        BitFieldCommand.BitFieldIncr(BitFieldType.UnsignedInt(8), 8, 200)
                      )
          } yield assert(result)(equalTo(Chunk(Some(255L))))
        },
        test("increment with overflow fail") {
          for {
            key <- uuid
            _   <- set(key, "value")
            result <- bitField(
                        key,
                        BitFieldCommand.BitFieldOverflow.Fail,
                        BitFieldCommand.BitFieldIncr(BitFieldType.UnsignedInt(8), 8, 200)
                      )
          } yield assert(result)(equalTo(Chunk(None)))
        },
        test("increment first with overflow wrap and then overflow fail") {
          for {
            key <- uuid
            _   <- set(key, "value")
            result <- bitField(
                        key,
                        BitFieldCommand.BitFieldOverflow.Wrap,
                        BitFieldCommand.BitFieldIncr(BitFieldType.UnsignedInt(8), 8, 200),
                        BitFieldCommand.BitFieldOverflow.Fail,
                        BitFieldCommand.BitFieldIncr(BitFieldType.UnsignedInt(8), 8, 250)
                      )
          } yield assert(result)(equalTo(Chunk(Some(41L), None)))
        },
        test("first set, then increment and then get same bits") {
          for {
            key <- uuid
            _   <- set(key, "value", None, None, None)
            result <- bitField(
                        key,
                        BitFieldCommand.BitFieldSet(BitFieldType.UnsignedInt(8), 8, 98L),
                        BitFieldCommand.BitFieldIncr(BitFieldType.UnsignedInt(8), 8, 2L),
                        BitFieldCommand.BitFieldGet(BitFieldType.UnsignedInt(8), 8)
                      )
          } yield assert(result)(equalTo(Chunk(Some(97L), Some(100L), Some(100L))))
        }
      ) @@ testExecutorUnsupported,
      suite("Stralgo")(
        test("get LCS from 2 strings") {
          val str1 = "foo"
          val str2 = "fao"
          assertZIO(stralgoLcs(StralgoLCS.Strings, str1, str2))(
            equalTo(LcsOutput.Lcs("fo"))
          )
        },
        test("get LCS from 2 keys") {
          val str1 = "foo"
          val str2 = "fao"

          for {
            key1   <- uuid
            _      <- set(key1, str1, None, None, None)
            key2   <- uuid
            _      <- set(key2, str2, None, None, None)
            result <- stralgoLcs(StralgoLCS.Keys, key1, key2)
          } yield assert(result)(equalTo(LcsOutput.Lcs("fo")))
        },
        test("get LCS from unknown keys") {
          val str1 = "foo"
          val str2 = "fao"

          for {
            key1   <- uuid
            _      <- set(key1, str1, None, None, None)
            key2   <- uuid
            _      <- set(key2, str2, None, None, None)
            result <- stralgoLcs(StralgoLCS.Keys, "unknown", "unknown")
          } yield assert(result)(equalTo(LcsOutput.Lcs("")))
        },
        test("Get length of LCS for strings") {
          val str1 = "foo"
          val str2 = "fao"
          assertZIO(
            stralgoLcs(StralgoLCS.Strings, str1, str2, Some(StrAlgoLcsQueryType.Len))
          )(
            equalTo(LcsOutput.Length(2))
          )
        },
        test("get length of LCS for keys") {
          val str1 = "foo"
          val str2 = "fao"

          for {
            key1 <- uuid
            _    <- set(key1, str1, None, None, None)
            key2 <- uuid
            _    <- set(key2, str2, None, None, None)
            result <-
              stralgoLcs(StralgoLCS.Keys, key1, key2, Some(StrAlgoLcsQueryType.Len))
          } yield assert(result)(equalTo(LcsOutput.Length(2)))
        },
        test("get length of LCS for unknown keys") {
          val str1 = "foo"
          val str2 = "fao"

          for {
            key1 <- uuid
            _    <- set(key1, str1, None, None, None)
            key2 <- uuid
            _    <- set(key2, str2, None, None, None)
            result <-
              stralgoLcs(
                StralgoLCS.Keys,
                "unknown",
                "unknown",
                Some(StrAlgoLcsQueryType.Len)
              )
          } yield assert(result)(equalTo(LcsOutput.Length(0)))
        },
        test("get index of LCS for strings") {
          val str1 = "ohmytext"
          val str2 = "mynewtext"
          assertZIO(
            stralgoLcs(StralgoLCS.Strings, str1, str2, Some(StrAlgoLcsQueryType.Idx()))
          )(
            equalTo(
              LcsOutput.Matches(
                List(
                  Match(matchIdxA = MatchIdx(4, 7), matchIdxB = MatchIdx(5, 8)),
                  Match(matchIdxA = MatchIdx(2, 3), matchIdxB = MatchIdx(0, 1))
                ),
                6
              )
            )
          )
        },
        test("get index of LCS for keys") {
          val str1 = "!ohmytext"
          val str2 = "!mynewtext"

          for {
            key1 <- uuid
            _    <- set(key1, str1)
            key2 <- uuid
            _    <- set(key2, str2)
            result <-
              stralgoLcs(StralgoLCS.Keys, key1, key2, Some(StrAlgoLcsQueryType.Idx()))
          } yield {
            assert(result)(
              equalTo(
                LcsOutput.Matches(
                  List(
                    Match(matchIdxA = MatchIdx(5, 8), matchIdxB = MatchIdx(6, 9)),
                    Match(matchIdxA = MatchIdx(3, 4), matchIdxB = MatchIdx(1, 2)),
                    Match(matchIdxA = MatchIdx(0, 0), matchIdxB = MatchIdx(0, 0))
                  ),
                  7
                )
              )
            )
          }
        },
        test("get index of LCS for keys with MINMATCHLEN") {
          val str1 = "!ohmytext"
          val str2 = "!mynewtext"

          for {
            key1 <- uuid
            _    <- set(key1, str1)
            key2 <- uuid
            _    <- set(key2, str2)
            result <-
              stralgoLcs(
                StralgoLCS.Keys,
                key1,
                key2,
                Some(StrAlgoLcsQueryType.Idx(minMatchLength = 2))
              )
          } yield {
            assert(result)(
              equalTo(
                LcsOutput.Matches(
                  List(
                    Match(matchIdxA = MatchIdx(5, 8), matchIdxB = MatchIdx(6, 9)),
                    Match(matchIdxA = MatchIdx(3, 4), matchIdxB = MatchIdx(1, 2))
                  ),
                  7
                )
              )
            )
          }
        },
        test("get index of LCS for keys with WITHMATCHLEN") {
          val str1 = "!ohmytext"
          val str2 = "!mynewtext"

          for {
            key1 <- uuid
            _    <- set(key1, str1)
            key2 <- uuid
            _    <- set(key2, str2)
            result <-
              stralgoLcs(
                StralgoLCS.Keys,
                key1,
                key2,
                Some(StrAlgoLcsQueryType.Idx(withMatchLength = true))
              )
          } yield {
            assert(result)(
              equalTo(
                LcsOutput.Matches(
                  List(
                    Match(matchIdxA = MatchIdx(5, 8), matchIdxB = MatchIdx(6, 9), matchLength = Some(4)),
                    Match(matchIdxA = MatchIdx(3, 4), matchIdxB = MatchIdx(1, 2), matchLength = Some(2)),
                    Match(matchIdxA = MatchIdx(0, 0), matchIdxB = MatchIdx(0, 0), matchLength = Some(1))
                  ),
                  7
                )
              )
            )
          }
        },
        test("get index of LCS for keys with MINMATCHLEN and WITHMATCHLEN") {
          val str1 = "!ohmytext"
          val str2 = "!mynewtext"

          for {
            key1 <- uuid
            _    <- set(key1, str1)
            key2 <- uuid
            _    <- set(key2, str2)
            result <-
              stralgoLcs(
                StralgoLCS.Keys,
                key1,
                key2,
                Some(StrAlgoLcsQueryType.Idx(minMatchLength = 2, withMatchLength = true))
              )
          } yield {
            assert(result)(
              equalTo(
                LcsOutput.Matches(
                  List(
                    Match(matchIdxA = MatchIdx(5, 8), matchIdxB = MatchIdx(6, 9), matchLength = Some(4)),
                    Match(matchIdxA = MatchIdx(3, 4), matchIdxB = MatchIdx(1, 2), matchLength = Some(2))
                  ),
                  7
                )
              )
            )
          }
        }
      ) @@ clusterExecutorUnsupported,
      suite("bitOp")(
        test("AND over multiple non-empty strings") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            third  <- uuid
            _      <- set(first, "a")
            _      <- set(second, "ab")
            _      <- set(third, "abc")
            result <- bitOp(BitOperation.AND, dest, first, second, third)
          } yield assert(result)(equalTo(3L))
        },
        test("AND over two non-empty strings") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            _      <- set(first, "first")
            _      <- set(second, "second")
            result <- bitOp(BitOperation.AND, dest, first, second)
          } yield assert(result)(equalTo(6L))
        },
        test("AND over one empty and one non-empty string") {
          for {
            dest     <- uuid
            empty    <- uuid
            nonEmpty <- uuid
            _        <- set(nonEmpty, "value")
            result   <- bitOp(BitOperation.AND, dest, empty, nonEmpty)
          } yield assert(result)(equalTo(5L))
        },
        test("AND over two empty strings") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            result <- bitOp(BitOperation.AND, dest, first, second)
          } yield assert(result)(equalTo(0L))
        },
        test("error when AND over one empty and one not string") {
          for {
            dest      <- uuid
            empty     <- uuid
            notString <- uuid
            _         <- sAdd(notString, "a")
            result    <- bitOp(BitOperation.AND, dest, empty, notString).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        },
        test("error when AND over non-empty and one not string") {
          for {
            dest      <- uuid
            nonEmpty  <- uuid
            notString <- uuid
            _         <- sAdd(notString, "a")
            result    <- bitOp(BitOperation.AND, dest, nonEmpty, notString).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        },
        test("OR over multiple non-empty strings") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            third  <- uuid
            _      <- set(first, "a")
            _      <- set(second, "ab")
            _      <- set(third, "abc")
            result <- bitOp(BitOperation.OR, dest, first, second, third)
          } yield assert(result)(equalTo(3L))
        },
        test("OR over two non-empty strings") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            _      <- set(first, "first")
            _      <- set(second, "second")
            result <- bitOp(BitOperation.OR, dest, first, second)
          } yield assert(result)(equalTo(6L))
        },
        test("OR over one empty and one non-empty string") {
          for {
            dest     <- uuid
            empty    <- uuid
            nonEmpty <- uuid
            _        <- set(nonEmpty, "value")
            result   <- bitOp(BitOperation.OR, dest, empty, nonEmpty)
          } yield assert(result)(equalTo(5L))
        },
        test("OR over two empty strings") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            result <- bitOp(BitOperation.OR, dest, first, second)
          } yield assert(result)(equalTo(0L))
        },
        test("error when OR over one empty and one not string") {
          for {
            dest      <- uuid
            empty     <- uuid
            notString <- uuid
            _         <- sAdd(notString, "a")
            result    <- bitOp(BitOperation.OR, dest, empty, notString).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        },
        test("error when OR over non-empty and one not string") {
          for {
            dest      <- uuid
            nonEmpty  <- uuid
            notString <- uuid
            _         <- sAdd(notString, "a")
            result    <- bitOp(BitOperation.OR, dest, nonEmpty, notString).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        },
        test("XOR over multiple non-empty strings") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            third  <- uuid
            _      <- set(first, "a")
            _      <- set(second, "ab")
            _      <- set(third, "abc")
            result <- bitOp(BitOperation.XOR, dest, first, second, third)
          } yield assert(result)(equalTo(3L))
        },
        test("XOR over two non-empty strings") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            _      <- set(first, "first")
            _      <- set(second, "second")
            result <- bitOp(BitOperation.XOR, dest, first, second)
          } yield assert(result)(equalTo(6L))
        },
        test("XOR over one empty and one non-empty string") {
          for {
            dest     <- uuid
            empty    <- uuid
            nonEmpty <- uuid
            _        <- set(nonEmpty, "value")
            result   <- bitOp(BitOperation.XOR, dest, empty, nonEmpty)
          } yield assert(result)(equalTo(5L))
        },
        test("XOR over two empty strings") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            result <- bitOp(BitOperation.XOR, dest, first, second)
          } yield assert(result)(equalTo(0L))
        },
        test("error when XOR over one empty and one not string") {
          for {
            dest      <- uuid
            empty     <- uuid
            notString <- uuid
            _         <- sAdd(notString, "a")
            result    <- bitOp(BitOperation.XOR, dest, empty, notString).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        },
        test("error when XOR over non-empty and one not string") {
          for {
            dest      <- uuid
            nonEmpty  <- uuid
            notString <- uuid
            _         <- sAdd(notString, "a")
            result    <- bitOp(BitOperation.XOR, dest, nonEmpty, notString).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        },
        test("error when NOT over multiple non-empty strings") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            third  <- uuid
            _      <- set(first, "a")
            _      <- set(second, "ab")
            _      <- set(third, "abc")
            result <- bitOp(BitOperation.NOT, dest, first, second, third).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when NOT over one non-empty and one empty string") {
          for {
            dest     <- uuid
            nonEmpty <- uuid
            empty    <- uuid
            _        <- set(nonEmpty, "a")
            result   <- bitOp(BitOperation.NOT, dest, nonEmpty, empty).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("NOT over non-empty string") {
          for {
            dest   <- uuid
            key    <- uuid
            _      <- set(key, "value")
            result <- bitOp(BitOperation.NOT, dest, key)
          } yield assert(result)(equalTo(5L))
        },
        test("NOT over empty string") {
          for {
            dest   <- uuid
            key    <- uuid
            result <- bitOp(BitOperation.NOT, dest, key)
          } yield assert(result)(equalTo(0L))
        },
        test("error when NOT over not string") {
          for {
            dest   <- uuid
            key    <- uuid
            _      <- sAdd(key, "a")
            result <- bitOp(BitOperation.NOT, dest, key).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ) @@ clusterExecutorUnsupported,
      suite("bitPos")(
        test("of 1 when non-empty string") {
          for {
            key <- uuid
            _   <- set(key, "value")
            pos <- bitPos(key, true)
          } yield assert(pos)(equalTo(1L))
        },
        test("of 0 when non-empty string") {
          for {
            key <- uuid
            _   <- set(key, "value")
            pos <- bitPos(key, false)
          } yield assert(pos)(equalTo(0L))
        },
        test("of 1 when empty string") {
          for {
            key <- uuid
            pos <- bitPos(key, true)
          } yield assert(pos)(equalTo(-1L))
        },
        test("of 0 when empty string") {
          for {
            key <- uuid
            pos <- bitPos(key, false)
          } yield assert(pos)(equalTo(0L))
        },
        test("of 1 when non-empty string with start") {
          for {
            key <- uuid
            _   <- set(key, "value")
            pos <- bitPos(key, true, Some(BitPosRange(2L, None)))
          } yield assert(pos)(equalTo(17L))
        },
        test("of 0 when non-empty string with start") {
          for {
            key <- uuid
            _   <- set(key, "value")
            pos <- bitPos(key, false, Some(BitPosRange(2L, None)))
          } yield assert(pos)(equalTo(16L))
        },
        test("of 1 when start is greater than non-empty string length") {
          for {
            key <- uuid
            _   <- set(key, "value")
            pos <- bitPos(key, true, Some(BitPosRange(10L, None)))
          } yield assert(pos)(equalTo(-1L))
        },
        test("of 0 when start is greater than non-empty string length") {
          for {
            key <- uuid
            _   <- set(key, "value")
            pos <- bitPos(key, false, Some(BitPosRange(10L, None)))
          } yield assert(pos)(equalTo(-1L))
        },
        test("of 1 when empty string with start") {
          for {
            key <- uuid
            pos <- bitPos(key, true, Some(BitPosRange(1L, None)))
          } yield assert(pos)(equalTo(-1L))
        },
        test("of 0 when empty string with start") {
          for {
            key <- uuid
            pos <- bitPos(key, false, Some(BitPosRange(10L, None)))
          } yield assert(pos)(equalTo(0L))
        },
        test("of 1 when non-empty string with start and end") {
          for {
            key <- uuid
            _   <- set(key, "value")
            pos <- bitPos(key, true, Some(BitPosRange(2L, Some(4L))))
          } yield assert(pos)(equalTo(17L))
        },
        test("of 0 when non-empty string with start and end") {
          for {
            key <- uuid
            _   <- set(key, "value")
            pos <- bitPos(key, false, Some(BitPosRange(2L, Some(4L))))
          } yield assert(pos)(equalTo(16L))
        },
        test("of 1 when start is greater than end with non-empty string") {
          for {
            key <- uuid
            _   <- set(key, "value")
            pos <- bitPos(key, true, Some(BitPosRange(4L, Some(2L))))
          } yield assert(pos)(equalTo(-1L))
        },
        test("of 0 when start is greater than end with non-empty string") {
          for {
            key <- uuid
            _   <- set(key, "value")
            pos <- bitPos(key, false, Some(BitPosRange(4L, Some(2L))))
          } yield assert(pos)(equalTo(-1L))
        },
        test("of 1 when range is out of non-empty string") {
          for {
            key <- uuid
            _   <- set(key, "value")
            pos <- bitPos(key, true, Some(BitPosRange(10L, Some(15L))))
          } yield assert(pos)(equalTo(-1L))
        },
        test("of 0 when range is out of non-empty string") {
          for {
            key <- uuid
            _   <- set(key, "value")
            pos <- bitPos(key, false, Some(BitPosRange(10L, Some(15L))))
          } yield assert(pos)(equalTo(-1L))
        },
        test("of 1 when start is equal to end with non-empty string") {
          for {
            key <- uuid
            _   <- set(key, "value")
            pos <- bitPos(key, true, Some(BitPosRange(1L, Some(1L))))
          } yield assert(pos)(equalTo(9L))
        },
        test("of 0 when start is equal to end with non-empty string") {
          for {
            key <- uuid
            _   <- set(key, "value")
            pos <- bitPos(key, false, Some(BitPosRange(1L, Some(1L))))
          } yield assert(pos)(equalTo(8L))
        },
        test("of 1 when empty string with start and end") {
          for {
            key <- uuid
            pos <- bitPos(key, true, Some(BitPosRange(2L, Some(4L))))
          } yield assert(pos)(equalTo(-1L))
        },
        test("of 0 when empty string with start and end") {
          for {
            key <- uuid
            pos <- bitPos(key, false, Some(BitPosRange(2L, Some(4L))))
          } yield assert(pos)(equalTo(0L))
        },
        test("of 1 when start is greater than end with empty string") {
          for {
            key <- uuid
            pos <- bitPos(key, true, Some(BitPosRange(4L, Some(2L))))
          } yield assert(pos)(equalTo(-1L))
        },
        test("of 0 when start is greater than end with empty string") {
          for {
            key <- uuid
            pos <- bitPos(key, false, Some(BitPosRange(4L, Some(2L))))
          } yield assert(pos)(equalTo(0L))
        },
        test("error when not string") {
          for {
            key <- uuid
            _   <- sAdd(key, "a")
            pos <- bitPos(key, true).either
          } yield assert(pos)(isLeft(isSubtype[WrongType](anything)))
        },
        test("error when not string and start is greater than end") {
          for {
            key <- uuid
            _   <- sAdd(key, "a")
            pos <- bitPos(key, false, Some(BitPosRange(4L, Some(2L)))).either
          } yield assert(pos)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("decr")(
        test("non-empty integer") {
          for {
            key    <- uuid
            _      <- set(key, "5")
            result <- decr(key)
          } yield assert(result)(equalTo(4L))
        },
        test("empty integer") {
          for {
            key    <- uuid
            result <- decr(key)
          } yield assert(result)(equalTo(-1L))
        },
        test("error when out of range integer") {
          for {
            key    <- uuid
            _      <- set(key, "234293482390480948029348230948")
            result <- decr(key).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when not integer") {
          for {
            key    <- uuid
            _      <- set(key, "not-integer")
            result <- decr(key).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        }
      ),
      suite("decrBy")(
        test("3 when non-empty integer") {
          for {
            key    <- uuid
            _      <- set(key, "10")
            result <- decrBy(key, 3L)
          } yield assert(result)(equalTo(7L))
        },
        test("3 when empty integer") {
          for {
            key    <- uuid
            result <- decrBy(key, 3L)
          } yield assert(result)(equalTo(-3L))
        },
        test("-3 when non-empty integer") {
          for {
            key    <- uuid
            _      <- set(key, "10")
            result <- decrBy(key, -3L)
          } yield assert(result)(equalTo(13L))
        },
        test("error when out of range integer") {
          for {
            key    <- uuid
            _      <- set(key, "234293482390480948029348230948")
            result <- decrBy(key, 3).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when not integer") {
          for {
            key    <- uuid
            _      <- set(key, "not-integer")
            result <- decrBy(key, 3).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        }
      ),
      suite("get")(
        test("non-emtpy string") {
          for {
            key    <- uuid
            _      <- set(key, "value")
            result <- get(key).returning[String]
          } yield assert(result)(isSome(equalTo("value")))
        },
        test("emtpy string") {
          for {
            key    <- uuid
            result <- get(key).returning[String]
          } yield assert(result)(isNone)
        },
        test("error when not string") {
          for {
            key    <- uuid
            _      <- sAdd(key, "a")
            result <- get(key).returning[String].either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("getBit")(
        test("from non-empty string") {
          for {
            key <- uuid
            _   <- set(key, "value")
            bit <- getBit(key, 17L)
          } yield assert(bit)(equalTo(1L))
        },
        test("with offset larger then string length") {
          for {
            key <- uuid
            _   <- set(key, "value")
            bit <- getBit(key, 100L)
          } yield assert(bit)(equalTo(0L))
        },
        test("with empty string") {
          for {
            key <- uuid
            bit <- getBit(key, 10L)
          } yield assert(bit)(equalTo(0L))
        },
        test("error when negative offset") {
          for {
            key <- uuid
            bit <- getBit(key, -1L).either
          } yield assert(bit)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when not string") {
          for {
            key <- uuid
            _   <- sAdd(key, "a")
            bit <- getBit(key, 10L).either
          } yield assert(bit)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("getRange")(
        test("from non-empty string") {
          for {
            key    <- uuid
            _      <- set(key, "value")
            substr <- getRange(key, 1 to 3).returning[String]
          } yield assert(substr)(isSome(equalTo("alu")))
        },
        test("with range that exceeds non-empty string length") {
          for {
            key    <- uuid
            _      <- set(key, "value")
            substr <- getRange(key, 1 to 10).returning[String]
          } yield assert(substr)(isSome(equalTo("alue")))
        },
        test("with range that is outside of non-empty string") {
          for {
            key    <- uuid
            _      <- set(key, "value")
            substr <- getRange(key, 10 to 15).returning[String]
          } yield assert(substr)(isNone)
        },
        test("with inverse range of non-empty string") {
          for {
            key    <- uuid
            _      <- set(key, "value")
            substr <- getRange(key, 15 to 3).returning[String]
          } yield assert(substr)(isNone)
        },
        test("with negative range end from non-empty string") {
          for {
            key    <- uuid
            _      <- set(key, "value")
            substr <- getRange(key, 1 to -1).returning[String]
          } yield assert(substr)(isSome(equalTo("alue")))
        },
        test("with start and end equal from non-empty string") {
          for {
            key    <- uuid
            _      <- set(key, "value")
            substr <- getRange(key, 1 to 1).returning[String]
          } yield assert(substr)(isSome(equalTo("a")))
        },
        test("from empty string") {
          for {
            key    <- uuid
            substr <- getRange(key, 1 to 3).returning[String]
          } yield assert(substr)(isNone)
        },
        test("error when not string") {
          for {
            key    <- uuid
            _      <- sAdd(key, "a")
            substr <- getRange(key, 1 to 3).returning[String].either
          } yield assert(substr)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("getSet")(
        test("non-empty value to the existing string") {
          for {
            key    <- uuid
            _      <- set(key, "value")
            oldVal <- getSet(key, "abc").returning[String]
          } yield assert(oldVal)(isSome(equalTo("value")))
        },
        test("empty value to the existing string") {
          for {
            key    <- uuid
            _      <- set(key, "value")
            oldVal <- getSet(key, "").returning[String]
          } yield assert(oldVal)(isSome(equalTo("value")))
        },
        test("non-empty value to the empty string") {
          for {
            key    <- uuid
            oldVal <- getSet(key, "value").returning[String]
          } yield assert(oldVal)(isNone)
        },
        test("empty value to the empty string") {
          for {
            key    <- uuid
            oldVal <- getSet(key, "").returning[String]
          } yield assert(oldVal)(isNone)
        },
        test("error when not string") {
          for {
            key    <- uuid
            _      <- sAdd(key, "a")
            oldVal <- getSet(key, "value").returning[String].either
          } yield assert(oldVal)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("incr")(
        test("non-empty integer") {
          for {
            key    <- uuid
            _      <- set(key, "5")
            result <- incr(key)
          } yield assert(result)(equalTo(6L))
        },
        test("empty integer") {
          for {
            key    <- uuid
            result <- incr(key)
          } yield assert(result)(equalTo(1L))
        },
        test("error when out of range integer") {
          for {
            key    <- uuid
            _      <- set(key, "234293482390480948029348230948")
            result <- incr(key).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when not integer") {
          for {
            key    <- uuid
            _      <- set(key, "not-integer")
            result <- incr(key).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        }
      ),
      suite("incrBy")(
        test("3 when non-empty integer") {
          for {
            key    <- uuid
            _      <- set(key, "10")
            result <- incrBy(key, 3L)
          } yield assert(result)(equalTo(13L))
        },
        test("3 when empty integer") {
          for {
            key    <- uuid
            result <- incrBy(key, 3L)
          } yield assert(result)(equalTo(3L))
        },
        test("-3 when non-empty integer") {
          for {
            key    <- uuid
            _      <- set(key, "10")
            result <- incrBy(key, -3L)
          } yield assert(result)(equalTo(7L))
        },
        test("error when out of range integer") {
          for {
            key    <- uuid
            _      <- set(key, "234293482390480948029348230948")
            result <- incrBy(key, 3).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when not integer") {
          for {
            key    <- uuid
            _      <- set(key, "not-integer")
            result <- incrBy(key, 3).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        }
      ),
      suite("incrByFloat")(
        test("3.4 when non-empty float") {
          for {
            key    <- uuid
            _      <- set(key, "5.1")
            result <- incrByFloat(key, 3.4d)
          } yield assert(result)(equalTo(8.5))
        },
        test("3.4 when empty float") {
          for {
            key    <- uuid
            result <- incrByFloat(key, 3.4d)
          } yield assert(result)(equalTo(3.4))
        },
        test("-3.4 when non-empty float") {
          for {
            key    <- uuid
            _      <- set(key, "5")
            result <- incrByFloat(key, -3.4d)
          } yield assert(result)(equalTo(1.6))
        },
        test("error when out of range value") {
          for {
            key    <- uuid
            _      <- set(key, s"${Double.MaxValue.toString}1234")
            result <- incrByFloat(key, 3d).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when not integer") {
          for {
            key    <- uuid
            _      <- set(key, "not-integer")
            result <- incrByFloat(key, 3).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        }
      ),
      suite("mGet")(
        test("from multiple non-empty strings") {
          for {
            first  <- uuid
            second <- uuid
            _      <- set(first, "1")
            _      <- set(second, "2")
            result <- mGet(first, second).returning[String]
          } yield assert(result)(equalTo(Chunk(Some("1"), Some("2"))))
        },
        test("from one non-empty and one empty string") {
          for {
            nonEmpty <- uuid
            empty    <- uuid
            _        <- set(nonEmpty, "value")
            result   <- mGet(nonEmpty, empty).returning[String]
          } yield assert(result)(equalTo(Chunk(Some("value"), None)))
        },
        test("from two empty strings") {
          for {
            first  <- uuid
            second <- uuid
            result <- mGet(first, second).returning[String]
          } yield assert(result)(equalTo(Chunk(None, None)))
        },
        test("from one string and one not string") {
          for {
            str    <- uuid
            notStr <- uuid
            _      <- set(str, "value")
            _      <- sAdd(notStr, "a")
            result <- mGet(str, notStr).returning[String]
          } yield assert(result)(equalTo(Chunk(Some("value"), None)))
        },
        test("from one not string") {
          for {
            key    <- uuid
            _      <- sAdd(key, "a")
            result <- mGet(key).returning[String]
          } yield assert(result)(equalTo(Chunk(None)))
        }
      ) @@ clusterExecutorUnsupported,
      suite("mSet")(
        test("one new value") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- mSet((key, value))
            result <- get(key).returning[String]
          } yield assert(result)(isSome(equalTo(value)))
        },
        test("multiple new values") {
          for {
            first     <- uuid
            second    <- uuid
            firstVal  <- uuid
            secondVal <- uuid
            _         <- mSet((first, firstVal), (second, secondVal))
            result    <- mGet(first, second).returning[String]
          } yield assert(result)(equalTo(Chunk(Some(firstVal), Some(secondVal))))
        },
        test("replace existing values") {
          for {
            first       <- uuid
            second      <- uuid
            firstVal    <- uuid
            secondVal   <- uuid
            replacement <- uuid
            _           <- mSet((first, firstVal), (second, secondVal))
            _           <- mSet((first, replacement), (second, replacement))
            result      <- mGet(first, second).returning[String]
          } yield assert(result)(equalTo(Chunk(Some(replacement), Some(replacement))))
        },
        test("one value multiple times at once") {
          for {
            key       <- uuid
            firstVal  <- uuid
            secondVal <- uuid
            _         <- mSet((key, firstVal), (key, secondVal))
            result    <- get(key).returning[String]
          } yield assert(result)(isSome(equalTo(secondVal)))
        },
        test("replace not string value") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- sAdd(key, "a")
            result <- mSet((key, value)).either
          } yield assert(result)(isRight)
        }
      ) @@ clusterExecutorUnsupported,
      suite("mSetNx")(
        test("one new value") {
          for {
            key   <- uuid
            value <- uuid
            set   <- mSetNx((key, value))
          } yield assert(set)(isTrue)
        },
        test("multiple new values") {
          for {
            first     <- uuid
            second    <- uuid
            firstVal  <- uuid
            secondVal <- uuid
            set       <- mSetNx((first, firstVal), (second, secondVal))
          } yield assert(set)(isTrue)
        },
        test("replace existing values") {
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
        test("one value multiple times at once") {
          for {
            key       <- uuid
            firstVal  <- uuid
            secondVal <- uuid
            set       <- mSetNx((key, firstVal), (key, secondVal))
          } yield assert(set)(isTrue)
        },
        test("replace not string value") {
          for {
            key   <- uuid
            value <- uuid
            _     <- sAdd(key, "a")
            set   <- mSetNx((key, value))
          } yield assert(set)(isFalse)
        }
      ) @@ clusterExecutorUnsupported,
      suite("pSetEx")(
        test("new value with 1000 milliseconds") {
          for {
            key          <- uuid
            value        <- uuid
            _            <- pSetEx(key, 1000.millis, value)
            existsBefore <- exists(key)
            fiber        <- ZIO.sleep(1010.millis).fork <* TestClock.adjust(1010.millis)
            _            <- fiber.join
            existsAfter  <- exists(key)
          } yield assert(existsBefore)(equalTo(1L)) && assert(existsAfter)(equalTo(0L))
        } @@ eventually,
        test("override existing string") {
          for {
            key        <- uuid
            value      <- uuid
            _          <- set(key, "value")
            _          <- pSetEx(key, 1000.millis, value)
            currentVal <- get(key).returning[String]
          } yield assert(currentVal)(isSome(equalTo(value)))
        },
        test("override not string") {
          for {
            key        <- uuid
            value      <- uuid
            _          <- sAdd(key, "a")
            _          <- pSetEx(key, 1000.millis, value)
            currentVal <- get(key).returning[String]
          } yield assert(currentVal)(isSome(equalTo(value)))
        },
        test("error when 0 milliseconds") {
          for {
            key    <- uuid
            value  <- uuid
            result <- pSetEx(key, 0.millis, value).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when negative milliseconds") {
          for {
            key    <- uuid
            value  <- uuid
            result <- pSetEx(key, (-1).millis, value).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        }
      ),
      suite("set")(
        test("new value") {
          for {
            key    <- uuid
            value  <- uuid
            result <- set(key, value)
          } yield assert(result)(isTrue)
        },
        test("override existing string") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- set(key, "value")
            result <- set(key, value)
          } yield assert(result)(isTrue)
        },
        test("override not string") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- sAdd(key, "a")
            result <- set(key, value)
          } yield assert(result)(isTrue)
        },
        test("new value with ttl 1 second") {
          for {
            key    <- uuid
            value  <- uuid
            result <- set(key, value, Some(1.second))
          } yield assert(result)(isTrue)
        },
        test("new value with ttl 100 milliseconds") {
          for {
            key    <- uuid
            value  <- uuid
            result <- set(key, value, Some(100.milliseconds))
          } yield assert(result)(isTrue)
        },
        test("error when negative ttl") {
          for {
            key    <- uuid
            value  <- uuid
            result <- set(key, value, Some((-1).millisecond)).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("new value with SetNew parameter") {
          for {
            key    <- uuid
            value  <- uuid
            result <- set(key, value, update = Some(Update.SetNew))
          } yield assert(result)(isTrue)
        },
        test("existing value with SetNew parameter") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- set(key, "value")
            result <- set(key, value, update = Some(Update.SetNew))
          } yield assert(result)(isFalse)
        },
        test("new value with SetExisting parameter") {
          for {
            key    <- uuid
            value  <- uuid
            result <- set(key, value, update = Some(Update.SetExisting))
          } yield assert(result)(isFalse)
        },
        test("existing value with SetExisting parameter") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- set(key, "value")
            result <- set(key, value, update = Some(Update.SetExisting))
          } yield assert(result)(isTrue)
        },
        test("existing not string value with SetExisting parameter") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- sAdd(key, "a")
            result <- set(key, value, update = Some(Update.SetExisting))
          } yield assert(result)(isTrue)
        },
        // next three tests include KEEPTTL parameter that is valid for Redis version >= 6
        test("new value with KeepTtl parameter") {
          for {
            key    <- uuid
            value  <- uuid
            result <- set(key, value, keepTtl = Some(KeepTtl))
          } yield assert(result)(isTrue)
        } @@ ignore,
        test("existing value with KeepTtl parameter") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- set(key, "value", Some(1.second))
            result <- set(key, value, keepTtl = Some(KeepTtl))
          } yield assert(result)(isTrue)
        } @@ ignore,
        test("existing value with both ttl and KeepTtl parameters") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- set(key, "value", Some(1.second))
            result <- set(key, value, Some(1.second), keepTtl = Some(KeepTtl))
          } yield assert(result)(isTrue)
        } @@ ignore
      ),
      suite("setBit")(
        test("for existing key") {
          for {
            key    <- uuid
            _      <- set(key, "value")
            oldBit <- setBit(key, 16L, true)
          } yield assert(oldBit)(isFalse)
        },
        test("for non-existent key") {
          for {
            key    <- uuid
            oldBit <- setBit(key, 16L, true)
          } yield assert(oldBit)(isFalse)
        },
        test("for offset that is out of string range") {
          for {
            key    <- uuid
            _      <- set(key, "value")
            oldBit <- setBit(key, 100L, true)
          } yield assert(oldBit)(isFalse)
        },
        test("error when negative offset") {
          for {
            key    <- uuid
            _      <- set(key, "value")
            oldBit <- setBit(key, -1L, false).either
          } yield assert(oldBit)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when not string") {
          for {
            key    <- uuid
            _      <- sAdd(key, "a")
            oldBit <- setBit(key, 10L, true).either
          } yield assert(oldBit)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("setEx")(
        test("new value with 1 second ttl") {
          for {
            key          <- uuid
            value        <- uuid
            _            <- setEx(key, 1.second, value)
            existsBefore <- exists(key)
            fiber        <- ZIO.sleep(1010.millis).fork <* TestClock.adjust(1010.millis)
            _            <- fiber.join
            existsAfter  <- exists(key)
          } yield assert(existsBefore)(equalTo(1L)) && assert(existsAfter)(equalTo(0L))
        } @@ eventually,
        test("existing value with 1 second ttl") {
          for {
            key          <- uuid
            value        <- uuid
            _            <- set(key, "value")
            _            <- setEx(key, 1.second, value)
            existsBefore <- exists(key)
            fiber        <- ZIO.sleep(1010.millis).fork <* TestClock.adjust(1010.millis)
            _            <- fiber.join
            existsAfter  <- exists(key)
          } yield assert(existsBefore)(equalTo(1L)) && assert(existsAfter)(equalTo(0L))
        } @@ eventually,
        test("override when not string") {
          for {
            key          <- uuid
            value        <- uuid
            _            <- sAdd(key, "a")
            _            <- setEx(key, 1.second, value)
            existsBefore <- exists(key)
            fiber        <- ZIO.sleep(1010.millis).fork <* TestClock.adjust(1010.millis)
            _            <- fiber.join
            existsAfter  <- exists(key)
          } yield assert(existsBefore)(equalTo(1L)) && assert(existsAfter)(equalTo(0L))
        },
        test("error when 0 seconds ttl") {
          for {
            key    <- uuid
            value  <- uuid
            result <- setEx(key, 0.seconds, value).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when negative ttl") {
          for {
            key    <- uuid
            value  <- uuid
            result <- setEx(key, (-1).second, value).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        }
      ),
      suite("setNx")(
        test("new value") {
          for {
            key    <- uuid
            value  <- uuid
            result <- setNx(key, value)
          } yield assert(result)(isTrue)
        },
        test("existing value") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- set(key, "value")
            result <- setNx(key, value)
          } yield assert(result)(isFalse)
        },
        test("not string") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- sAdd(key, "a")
            result <- setNx(key, value)
          } yield assert(result)(isFalse)
        }
      ),
      suite("setRange")(
        test("in existing string") {
          for {
            key <- uuid
            _   <- set(key, "val")
            len <- setRange(key, 3L, "ue")
          } yield assert(len)(equalTo(5L))
        },
        test("in non-existent string") {
          for {
            key <- uuid
            len <- setRange(key, 2L, "value")
          } yield assert(len)(equalTo(7L))
        },
        test("when offset is larger then string length") {
          for {
            key <- uuid
            _   <- set(key, "value")
            len <- setRange(key, 7L, "value")
          } yield assert(len)(equalTo(12L))
        },
        test("error when negative offset") {
          for {
            key <- uuid
            len <- setRange(key, -1L, "value").either
          } yield assert(len)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when not string") {
          for {
            key <- uuid
            _   <- sAdd(key, "a")
            len <- setRange(key, 1L, "value").either
          } yield assert(len)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("strLen")(
        test("for non-empty string") {
          for {
            key <- uuid
            _   <- set(key, "value")
            len <- strLen(key)
          } yield assert(len)(equalTo(5L))
        },
        test("for empty string") {
          for {
            key <- uuid
            len <- strLen(key)
          } yield assert(len)(equalTo(0L))
        },
        test("error when not string") {
          for {
            key <- uuid
            _   <- sAdd(key, "a")
            len <- strLen(key).either
          } yield assert(len)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("getEx")(
        test("value exists after removing ttl") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- pSetEx(key, 10.millis, value)
            exists <- getEx(key, true).returning[String]
            fiber  <- ZIO.sleep(20.millis).fork <* TestClock.adjust(20.millis)
            _      <- fiber.join
            res    <- get(key).returning[String]
          } yield assert(res.isDefined)(equalTo(true)) && assert(exists)(equalTo(Some(value)))
        } @@ eventually,
        test("not found value when set seconds ttl") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- set(key, value)
            exists <- getEx(key, Expire.SetExpireSeconds, 1.second).returning[String]
            fiber  <- ZIO.sleep(1020.millis).fork <* TestClock.adjust(1020.millis)
            _      <- fiber.join
            res    <- get(key).returning[String]
          } yield assert(res.isDefined)(equalTo(false)) && assert(exists)(equalTo(Some(value)))
        } @@ eventually,
        test("not found value when set milliseconds ttl") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- set(key, value)
            exists <- getEx(key, Expire.SetExpireMilliseconds, 10.millis).returning[String]
            fiber  <- ZIO.sleep(20.millis).fork <* TestClock.adjust(20.millis)
            _      <- fiber.join
            res    <- get(key).returning[String]
          } yield assert(res.isDefined)(equalTo(false)) && assert(exists)(equalTo(Some(value)))
        } @@ eventually,
        test("not found value when set seconds timestamp") {
          for {
            key       <- uuid
            value     <- uuid
            _         <- set(key, value)
            expiresAt <- Clock.instant.map(_.plusMillis(10.millis.toMillis))
            exists    <- getEx(key, ExpiredAt.SetExpireAtSeconds, expiresAt).returning[String]
            fiber     <- ZIO.sleep(20.millis).fork <* TestClock.adjust(20.millis)
            _         <- fiber.join
            res       <- get(key).returning[String]
          } yield assert(res.isDefined)(equalTo(false)) && assert(exists)(equalTo(Some(value)))
        } @@ eventually,
        test("not found value when set milliseconds timestamp") {
          for {
            key       <- uuid
            value     <- uuid
            _         <- set(key, value)
            expiresAt <- Clock.instant.map(_.plusMillis(10.millis.toMillis))
            exists    <- getEx(key, ExpiredAt.SetExpireAtMilliseconds, expiresAt).returning[String]
            fiber     <- ZIO.sleep(20.millis).fork <* TestClock.adjust(20.millis)
            _         <- fiber.join
            res       <- get(key).returning[String]
          } yield assert(res.isDefined)(equalTo(false)) && assert(exists)(equalTo(Some(value)))
        } @@ eventually,
        test("key not found") {
          for {
            key       <- uuid
            value     <- uuid
            _         <- set(key, value)
            expiresAt <- Clock.instant.map(_.plusMillis(10.millis.toMillis))
            res       <- getEx(value, ExpiredAt.SetExpireAtMilliseconds, expiresAt).returning[String]
            res2      <- getEx(value, Expire.SetExpireMilliseconds, 10.millis).returning[String]
            res3      <- getEx(value, true).returning[String]
          } yield assert(res)(equalTo(None)) && assert(res2)(equalTo(None)) && assert(res3)(equalTo(None))
        } @@ eventually
      ),
      suite("getDel")(
        test("error when not string") {
          for {
            key <- uuid
            _   <- sAdd(key, "a")
            res <- getDel(key).returning[String].either
          } yield assert(res)(isLeft(isSubtype[WrongType](anything)))
        },
        test("key not exists") {
          for {
            key <- uuid
            res <- getDel(key).returning[String]
          } yield assert(res)(equalTo(None))
        },
        test("get and remove key") {
          for {
            key      <- uuid
            value    <- uuid
            _        <- set(key, value)
            res      <- getDel(key).returning[String]
            notFound <- getDel(key).returning[String]
          } yield assert(res)(equalTo(Some(value))) && assert(notFound)(equalTo(None))
        }
      )
    )
}
