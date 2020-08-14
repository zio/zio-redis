package zio.redis

import java.time.Instant

import BitFieldCommand._
import BitFieldType._
import BitOperation._
import Order._
import RadiusUnit._
import zio.duration._
import zio.redis.Input._
import zio.test.Assertion._
import zio.test._
import zio.{ Chunk, Task }

object InputSpec extends BaseSpec {
  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("Input encoders")(
      suite("AbsTtl")(
        testM("valid value") {
          for {
            result <- Task(AbsTtlInput.encode(AbsTtl))
          } yield assert(result)(equalTo(Chunk.single("$6\r\nABSTTL\r\n")))
        }
      ),
      suite("Aggregate")(
        testM("max") {
          for {
            result <- Task(AggregateInput.encode(Aggregate.Max))
          } yield assert(result)(equalTo(Chunk.single("$3\r\nMAX\r\n")))
        },
        testM("min") {
          for {
            result <- Task(AggregateInput.encode(Aggregate.Min))
          } yield assert(result)(equalTo(Chunk.single("$3\r\nMIN\r\n")))
        },
        testM("sum") {
          for {
            result <- Task(AggregateInput.encode(Aggregate.Sum))
          } yield assert(result)(equalTo(Chunk.single("$3\r\nSUM\r\n")))
        }
      ),
      suite("Auth")(
        testM("with empty password") {
          for {
            result <- Task(AuthInput.encode(Auth("")))
          } yield assert(result)(equalTo(Chunk("$4\r\nAUTH\r\n", "$0\r\n\r\n")))
        },
        testM("with non-empty password") {
          for {
            result <- Task(AuthInput.encode(Auth("pass")))
          } yield assert(result)(equalTo(Chunk("$4\r\nAUTH\r\n", "$4\r\npass\r\n")))
        }
      ),
      suite("Bool")(
        testM("true") {
          for {
            result <- Task(BoolInput.encode(true))
          } yield assert(result)(equalTo(Chunk.single("$1\r\n1\r\n")))
        },
        testM("false") {
          for {
            result <- Task(BoolInput.encode(false))
          } yield assert(result)(equalTo(Chunk.single("$1\r\n0\r\n")))
        }
      ),
      suite("BitFieldCommand")(
        testM("get with unsigned type and positive offset") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldGet(UnsignedInt(3), 2)))
          } yield assert(result)(equalTo(Chunk("$3\r\nGET\r\n", "$2\r\nu3\r\n", "$1\r\n2\r\n")))
        },
        testM("get with signed type and negative offset") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldGet(SignedInt(3), -2)))
          } yield assert(result)(equalTo(Chunk("$3\r\nGET\r\n", "$2\r\ni3\r\n", "$2\r\n-2\r\n")))
        },
        testM("get with unsigned type and zero offset") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldGet(UnsignedInt(3), 0)))
          } yield assert(result)(equalTo(Chunk("$3\r\nGET\r\n", "$2\r\nu3\r\n", "$1\r\n0\r\n")))
        },
        testM("set with unsigned type, positive offset and positive value") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldSet(UnsignedInt(3), 2, 100L)))
          } yield assert(result)(equalTo(Chunk("$3\r\nSET\r\n", "$2\r\nu3\r\n", "$1\r\n2\r\n", "$3\r\n100\r\n")))
        },
        testM("set with signed type, negative offset and negative value") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldSet(SignedInt(3), -2, -100L)))
          } yield assert(result)(equalTo(Chunk("$3\r\nSET\r\n", "$2\r\ni3\r\n", "$2\r\n-2\r\n", "$4\r\n-100\r\n")))
        },
        testM("set with unsigned type, zero offset and zero value") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldSet(UnsignedInt(3), 0, 0L)))
          } yield assert(result)(equalTo(Chunk("$3\r\nSET\r\n", "$2\r\nu3\r\n", "$1\r\n0\r\n", "$1\r\n0\r\n")))
        },
        testM("incr with unsigned type, positive offset and positive value") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldIncr(UnsignedInt(3), 2, 100L)))
          } yield assert(result)(equalTo(Chunk("$6\r\nINCRBY\r\n", "$2\r\nu3\r\n", "$1\r\n2\r\n", "$3\r\n100\r\n")))
        },
        testM("incr with signed type, negative offset and negative value") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldIncr(SignedInt(3), -2, -100L)))
          } yield assert(result)(equalTo(Chunk("$6\r\nINCRBY\r\n", "$2\r\ni3\r\n", "$2\r\n-2\r\n", "$4\r\n-100\r\n")))
        },
        testM("incr with unsigned type, zero offset and zero value") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldIncr(UnsignedInt(3), 0, 0L)))
          } yield assert(result)(equalTo(Chunk("$6\r\nINCRBY\r\n", "$2\r\nu3\r\n", "$1\r\n0\r\n", "$1\r\n0\r\n")))
        },
        testM("overflow sat") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldOverflow.Sat))
          } yield assert(result)(equalTo(Chunk("$8\r\nOVERFLOW\r\n", "$3\r\nSAT\r\n")))
        },
        testM("overflow fail") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldOverflow.Fail))
          } yield assert(result)(equalTo(Chunk("$8\r\nOVERFLOW\r\n", "$4\r\nFAIL\r\n")))
        },
        testM("overflow warp") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldOverflow.Wrap))
          } yield assert(result)(equalTo(Chunk("$8\r\nOVERFLOW\r\n", "$4\r\nWRAP\r\n")))
        }
      ),
      suite("BitOperation")(
        testM("and") {
          for {
            result <- Task(BitOperationInput.encode(AND))
          } yield assert(result)(equalTo(Chunk.single("$3\r\nAND\r\n")))
        },
        testM("or") {
          for {
            result <- Task(BitOperationInput.encode(OR))
          } yield assert(result)(equalTo(Chunk.single("$2\r\nOR\r\n")))
        },
        testM("xor") {
          for {
            result <- Task(BitOperationInput.encode(XOR))
          } yield assert(result)(equalTo(Chunk.single("$3\r\nXOR\r\n")))
        },
        testM("not") {
          for {
            result <- Task(BitOperationInput.encode(NOT))
          } yield assert(result)(equalTo(Chunk.single("$3\r\nNOT\r\n")))
        }
      ),
      suite("BitPosRange")(
        testM("with only start") {
          for {
            result <- Task(BitPosRangeInput.encode(BitPosRange(1.second.toMillis, None)))
          } yield assert(result)(equalTo(Chunk.single("$4\r\n1000\r\n")))
        },
        testM("with start and the end") {
          for {
            result <- Task(BitPosRangeInput.encode(BitPosRange(0.second.toMillis, Some(1.second.toMillis))))
          } yield assert(result)(equalTo(Chunk("$1\r\n0\r\n", "$4\r\n1000\r\n")))
        }
      ),
      suite("Changed")(
        testM("valid value") {
          for {
            result <- Task(ChangedInput.encode(Changed))
          } yield assert(result)(equalTo(Chunk("$2\r\nCH\r\n")))
        }
      ),
      suite("Copy")(
        testM("valid value") {
          for {
            result <- Task(CopyInput.encode(Copy))
          } yield assert(result)(equalTo(Chunk("$4\r\nCOPY\r\n")))
        }
      ),
      suite("Count")(
        testM("positive value") {
          for {
            result <- Task(CountInput.encode(Count(3L)))
          } yield assert(result)(equalTo(Chunk("$5\r\nCOUNT\r\n", "$1\r\n3\r\n")))
        },
        testM("negative value") {
          for {
            result <- Task(CountInput.encode(Count(-3L)))
          } yield assert(result)(equalTo(Chunk("$5\r\nCOUNT\r\n", "$2\r\n-3\r\n")))
        },
        testM("zero value") {
          for {
            result <- Task(CountInput.encode(Count(0L)))
          } yield assert(result)(equalTo(Chunk("$5\r\nCOUNT\r\n", "$1\r\n0\r\n")))
        }
      ),
      suite("Position")(
        testM("before") {
          for {
            result <- Task(PositionInput.encode(Position.Before))
          } yield assert(result)(equalTo(Chunk.single("$6\r\nBEFORE\r\n")))
        },
        testM("after") {
          for {
            result <- Task(PositionInput.encode(Position.After))
          } yield assert(result)(equalTo(Chunk.single("$5\r\nAFTER\r\n")))
        }
      ),
      suite("Double")(
        testM("positive value") {
          for {
            result <- Task(DoubleInput.encode(4.2d))
          } yield assert(result)(equalTo(Chunk.single("$3\r\n4.2\r\n")))
        },
        testM("negative value") {
          for {
            result <- Task(DoubleInput.encode(-4.2d))
          } yield assert(result)(equalTo(Chunk.single("$4\r\n-4.2\r\n")))
        },
        testM("zero value") {
          for {
            result <- Task(DoubleInput.encode(0d))
          } yield assert(result)(equalTo(Chunk.single("$3\r\n0.0\r\n")))
        }
      ),
      suite("DurationMilliseconds")(
        testM("1 second") {
          for {
            result <- Task(DurationMillisecondsInput.encode(1.second))
          } yield assert(result)(equalTo(Chunk("$4\r\n1000\r\n")))
        },
        testM("100 milliseconds") {
          for {
            result <- Task(DurationMillisecondsInput.encode(100.millis))
          } yield assert(result)(equalTo(Chunk("$3\r\n100\r\n")))
        }
      ),
      suite("DurationSeconds")(
        testM("1 minute") {
          for {
            result <- Task(DurationSecondsInput.encode(1.minute))
          } yield assert(result)(equalTo(Chunk.single("$2\r\n60\r\n")))
        },
        testM("1 second") {
          for {
            result <- Task(DurationSecondsInput.encode(1.second))
          } yield assert(result)(equalTo(Chunk.single("$1\r\n1\r\n")))
        },
        testM("100 milliseconds") {
          for {
            result <- Task(DurationSecondsInput.encode(100.millis))
          } yield assert(result)(equalTo(Chunk.single("$1\r\n0\r\n")))
        }
      ),
      suite("DurationTtl")(
        testM("1 second") {
          for {
            result <- Task(DurationTtlInput.encode(1.second))
          } yield assert(result)(equalTo(Chunk("$2\r\nPX\r\n", "$4\r\n1000\r\n")))
        },
        testM("100 milliseconds") {
          for {
            result <- Task(DurationTtlInput.encode(100.millis))
          } yield assert(result)(equalTo(Chunk("$2\r\nPX\r\n", "$3\r\n100\r\n")))
        }
      ),
      suite("Freq")(
        testM("empty string") {
          for {
            result <- Task(FreqInput.encode(Freq("")))
          } yield assert(result)(equalTo(Chunk("$4\r\nFREQ\r\n", "$0\r\n\r\n")))
        },
        testM("non-empty string") {
          for {
            result <- Task(FreqInput.encode(Freq("frequency")))
          } yield assert(result)(equalTo(Chunk("$4\r\nFREQ\r\n", "$9\r\nfrequency\r\n")))
        }
      ),
      suite("IdleTime")(
        testM("0 seconds") {
          for {
            result <- Task(IdleTimeInput.encode(IdleTime(0)))
          } yield assert(result)(equalTo(Chunk("$8\r\nIDLETIME\r\n", "$1\r\n0\r\n")))
        },
        testM("5 seconds") {
          for {
            result <- Task(IdleTimeInput.encode(IdleTime(5)))
          } yield assert(result)(equalTo(Chunk("$8\r\nIDLETIME\r\n", "$1\r\n5\r\n")))
        }
      ),
      suite("Increment")(
        testM("valid value") {
          for {
            result <- Task(IncrementInput.encode(Increment))
          } yield assert(result)(equalTo(Chunk.single("$4\r\nINCR\r\n")))
        }
      ),
      suite("KeepTtl")(
        testM("valid value") {
          for {
            result <- Task(KeepTtlInput.encode(KeepTtl))
          } yield assert(result)(equalTo(Chunk.single("$7\r\nKEEPTTL\r\n")))
        }
      ),
      suite("LexRange")(
        testM("with unbound min and unbound max") {
          for {
            result <- Task(LexRangeInput.encode(LexRange(LexMinimum.Unbounded, LexMaximum.Unbounded)))
          } yield assert(result)(equalTo(Chunk("$1\r\n-\r\n", "$1\r\n+\r\n")))
        },
        testM("with open min and unbound max") {
          for {
            result <- Task(LexRangeInput.encode(LexRange(LexMinimum.Open("a"), LexMaximum.Unbounded)))
          } yield assert(result)(equalTo(Chunk("$2\r\n(a\r\n", "$1\r\n+\r\n")))
        },
        testM("with closed min and unbound max") {
          for {
            result <- Task(LexRangeInput.encode(LexRange(LexMinimum.Closed("a"), LexMaximum.Unbounded)))
          } yield assert(result)(equalTo(Chunk("$2\r\n[a\r\n", "$1\r\n+\r\n")))
        },
        testM("with unbound min and open max") {
          for {
            result <- Task(LexRangeInput.encode(LexRange(LexMinimum.Unbounded, LexMaximum.Open("z"))))
          } yield assert(result)(equalTo(Chunk("$1\r\n-\r\n", "$2\r\n(z\r\n")))
        },
        testM("with open min and open max") {
          for {
            result <- Task(LexRangeInput.encode(LexRange(LexMinimum.Open("a"), LexMaximum.Open("z"))))
          } yield assert(result)(equalTo(Chunk("$2\r\n(a\r\n", "$2\r\n(z\r\n")))
        },
        testM("with closed min and open max") {
          for {
            result <- Task(LexRangeInput.encode(LexRange(LexMinimum.Closed("a"), LexMaximum.Open("z"))))
          } yield assert(result)(equalTo(Chunk("$2\r\n[a\r\n", "$2\r\n(z\r\n")))
        },
        testM("with unbound min and closed max") {
          for {
            result <- Task(LexRangeInput.encode(LexRange(LexMinimum.Unbounded, LexMaximum.Closed("z"))))
          } yield assert(result)(equalTo(Chunk("$1\r\n-\r\n", "$2\r\n[z\r\n")))
        },
        testM("with open min and closed max") {
          for {
            result <- Task(LexRangeInput.encode(LexRange(LexMinimum.Open("a"), LexMaximum.Closed("z"))))
          } yield assert(result)(equalTo(Chunk("$2\r\n(a\r\n", "$2\r\n[z\r\n")))
        },
        testM("with closed min and closed max") {
          for {
            result <- Task(LexRangeInput.encode(LexRange(LexMinimum.Closed("a"), LexMaximum.Closed("z"))))
          } yield assert(result)(equalTo(Chunk("$2\r\n[a\r\n", "$2\r\n[z\r\n")))
        }
      ),
      suite("Limit")(
        testM("with positive offset and positive count") {
          for {
            result <- Task(LimitInput.encode(Limit(4L, 5L)))
          } yield assert(result)(equalTo(Chunk("$5\r\nLIMIT\r\n", "$1\r\n4\r\n", "$1\r\n5\r\n")))
        },
        testM("with negative offset and negative count") {
          for {
            result <- Task(LimitInput.encode(Limit(-4L, -5L)))
          } yield assert(result)(equalTo(Chunk("$5\r\nLIMIT\r\n", "$2\r\n-4\r\n", "$2\r\n-5\r\n")))
        },
        testM("with zero offset and zero count") {
          for {
            result <- Task(LimitInput.encode(Limit(0L, 0L)))
          } yield assert(result)(equalTo(Chunk("$5\r\nLIMIT\r\n", "$1\r\n0\r\n", "$1\r\n0\r\n")))
        }
      ),
      suite("Long")(
        testM("positive value") {
          for {
            result <- Task(LongInput.encode(4L))
          } yield assert(result)(equalTo(Chunk.single("$1\r\n4\r\n")))
        },
        testM("negative value") {
          for {
            result <- Task(LongInput.encode(-4L))
          } yield assert(result)(equalTo(Chunk.single("$2\r\n-4\r\n")))
        },
        testM("zero value") {
          for {
            result <- Task(LongInput.encode(0L))
          } yield assert(result)(equalTo(Chunk.single("$1\r\n0\r\n")))
        }
      ),
      suite("LongLat")(
        testM("positive longitude and latitude") {
          for {
            result <- Task(LongLatInput.encode(LongLat(4.2d, 5.2d)))
          } yield assert(result)(equalTo(Chunk("$3\r\n4.2\r\n", "$3\r\n5.2\r\n")))
        },
        testM("negative longitude and latitude") {
          for {
            result <- Task(LongLatInput.encode(LongLat(-4.2d, -5.2d)))
          } yield assert(result)(equalTo(Chunk("$4\r\n-4.2\r\n", "$4\r\n-5.2\r\n")))
        },
        testM("zero longitude and latitude") {
          for {
            result <- Task(LongLatInput.encode(LongLat(0d, 0d)))
          } yield assert(result)(equalTo(Chunk("$3\r\n0.0\r\n", "$3\r\n0.0\r\n")))
        }
      ),
      suite("MemberScore")(
        testM("with positive score and empty member") {
          for {
            result <- Task(MemberScoreInput.encode(MemberScore(4.2d, "")))
          } yield assert(result)(equalTo(Chunk("$3\r\n4.2\r\n", "$0\r\n\r\n")))
        },
        testM("with negative score and empty member") {
          for {
            result <- Task(MemberScoreInput.encode(MemberScore(-4.2d, "")))
          } yield assert(result)(equalTo(Chunk("$4\r\n-4.2\r\n", "$0\r\n\r\n")))
        },
        testM("with zero score and empty member") {
          for {
            result <- Task(MemberScoreInput.encode(MemberScore(0d, "")))
          } yield assert(result)(equalTo(Chunk("$3\r\n0.0\r\n", "$0\r\n\r\n")))
        },
        testM("with positive score and non-empty member") {
          for {
            result <- Task(MemberScoreInput.encode(MemberScore(4.2d, "member")))
          } yield assert(result)(equalTo(Chunk("$3\r\n4.2\r\n", "$6\r\nmember\r\n")))
        },
        testM("with negative score and non-empty member") {
          for {
            result <- Task(MemberScoreInput.encode(MemberScore(-4.2d, "member")))
          } yield assert(result)(equalTo(Chunk("$4\r\n-4.2\r\n", "$6\r\nmember\r\n")))
        },
        testM("with zero score and non-empty member") {
          for {
            result <- Task(MemberScoreInput.encode(MemberScore(0d, "member")))
          } yield assert(result)(equalTo(Chunk("$3\r\n0.0\r\n", "$6\r\nmember\r\n")))
        }
      ),
      suite("NoInput")(
        testM("valid value") {
          for {
            result <- Task(NoInput.encode(()))
          } yield assert(result)(isEmpty)
        }
      ),
      suite("NonEmptyList")(
        testM("with multiple elements") {
          for {
            result <- Task(NonEmptyList(StringInput).encode(("a", List("b", "c"))))
          } yield assert(result)(equalTo(Chunk("$1\r\na\r\n", "$1\r\nb\r\n", "$1\r\nc\r\n")))
        },
        testM("with one element") {
          for {
            result <- Task(NonEmptyList(StringInput).encode(("a", List.empty)))
          } yield assert(result)(equalTo(Chunk.single("$1\r\na\r\n")))
        }
      ),
      suite("Order")(
        testM("ascending") {
          for {
            result <- Task(OrderInput.encode(Ascending))
          } yield assert(result)(equalTo(Chunk.single("$3\r\nASC\r\n")))
        },
        testM("descending") {
          for {
            result <- Task(OrderInput.encode(Descending))
          } yield assert(result)(equalTo(Chunk.single("$4\r\nDESC\r\n")))
        }
      ),
      suite("RadiusUnit")(
        testM("meters") {
          for {
            result <- Task(RadiusUnitInput.encode(Meters))
          } yield assert(result)(equalTo(Chunk.single("$1\r\nm\r\n")))
        },
        testM("kilometers") {
          for {
            result <- Task(RadiusUnitInput.encode(Kilometers))
          } yield assert(result)(equalTo(Chunk.single("$2\r\nkm\r\n")))
        },
        testM("feet") {
          for {
            result <- Task(RadiusUnitInput.encode(Feet))
          } yield assert(result)(equalTo(Chunk.single("$2\r\nft\r\n")))
        },
        testM("miles") {
          for {
            result <- Task(RadiusUnitInput.encode(Miles))
          } yield assert(result)(equalTo(Chunk.single("$2\r\nmi\r\n")))
        }
      ),
      suite("Range")(
        testM("with positive start and positive end") {
          for {
            result <- Task(RangeInput.encode(Range(1, 5)))
          } yield assert(result)(equalTo(Chunk("$1\r\n1\r\n", "$1\r\n5\r\n")))
        },
        testM("with negative start and positive end") {
          for {
            result <- Task(RangeInput.encode(Range(-1, 5)))
          } yield assert(result)(equalTo(Chunk("$2\r\n-1\r\n", "$1\r\n5\r\n")))
        },
        testM("with positive start and negative end") {
          for {
            result <- Task(RangeInput.encode(Range(1, -5)))
          } yield assert(result)(equalTo(Chunk("$1\r\n1\r\n", "$2\r\n-5\r\n")))
        },
        testM("with negative start and negative end") {
          for {
            result <- Task(RangeInput.encode(Range(-1, -5)))
          } yield assert(result)(equalTo(Chunk("$2\r\n-1\r\n", "$2\r\n-5\r\n")))
        }
      ),
      suite("Regex")(
        testM("with valid pattern") {
          for {
            result <- Task(RegexInput.encode("\\d{3}".r))
          } yield assert(result)(equalTo(Chunk("$5\r\nMATCH\r\n", "$5\r\n\\d{3}\r\n")))
        },
        testM("with empty pattern") {
          for {
            result <- Task(RegexInput.encode("".r))
          } yield assert(result)(equalTo(Chunk("$5\r\nMATCH\r\n", "$0\r\n\r\n")))
        }
      ),
      suite("Replace")(
        testM("valid value") {
          for {
            result <- Task(ReplaceInput.encode(Replace))
          } yield assert(result)(equalTo(Chunk.single("$7\r\nREPLACE\r\n")))
        }
      ),
      suite("StoreDist")(
        testM("with non-empty string") {
          for {
            result <- Task(StoreDistInput.encode(StoreDist("key")))
          } yield assert(result)(equalTo(Chunk("$9\r\nSTOREDIST\r\n", "$3\r\nkey\r\n")))
        },
        testM("with empty string") {
          for {
            result <- Task(StoreDistInput.encode(StoreDist("")))
          } yield assert(result)(equalTo(Chunk("$9\r\nSTOREDIST\r\n", "$0\r\n\r\n")))
        }
      ),
      suite("Store")(
        testM("with non-empty string") {
          for {
            result <- Task(StoreInput.encode(Store("key")))
          } yield assert(result)(equalTo(Chunk("$5\r\nSTORE\r\n", "$3\r\nkey\r\n")))
        },
        testM("with empty string") {
          for {
            result <- Task(StoreInput.encode(Store("")))
          } yield assert(result)(equalTo(Chunk("$5\r\nSTORE\r\n", "$0\r\n\r\n")))
        }
      ),
      suite("ScoreRange")(
        testM("with infinite min and infinite max") {
          for {
            result <- Task(ScoreRangeInput.encode(ScoreRange(ScoreMinimum.Infinity, ScoreMaximum.Infinity)))
          } yield assert(result)(equalTo(Chunk("$4\r\n-inf\r\n", "$4\r\n+inf\r\n")))
        },
        testM("with open min and infinite max") {
          for {
            result <- Task(ScoreRangeInput.encode(ScoreRange(ScoreMinimum.Open(4.2d), ScoreMaximum.Infinity)))
          } yield assert(result)(equalTo(Chunk("$4\r\n(4.2\r\n", "$4\r\n+inf\r\n")))
        },
        testM("with closed min and infinite max") {
          for {
            result <- Task(ScoreRangeInput.encode(ScoreRange(ScoreMinimum.Closed(4.2d), ScoreMaximum.Infinity)))
          } yield assert(result)(equalTo(Chunk("$4\r\n[4.2\r\n", "$4\r\n+inf\r\n")))
        },
        testM("with infinite min and open max") {
          for {
            result <- Task(ScoreRangeInput.encode(ScoreRange(ScoreMinimum.Infinity, ScoreMaximum.Open(5.2d))))
          } yield assert(result)(equalTo(Chunk("$4\r\n-inf\r\n", "$4\r\n(5.2\r\n")))
        },
        testM("with open min and open max") {
          for {
            result <- Task(ScoreRangeInput.encode(ScoreRange(ScoreMinimum.Open(4.2d), ScoreMaximum.Open(5.2d))))
          } yield assert(result)(equalTo(Chunk("$4\r\n(4.2\r\n", "$4\r\n(5.2\r\n")))
        },
        testM("with closed min and open max") {
          for {
            result <- Task(ScoreRangeInput.encode(ScoreRange(ScoreMinimum.Closed(4.2d), ScoreMaximum.Open(5.2d))))
          } yield assert(result)(equalTo(Chunk("$4\r\n[4.2\r\n", "$4\r\n(5.2\r\n")))
        },
        testM("with infinite min and closed max") {
          for {
            result <- Task(ScoreRangeInput.encode(ScoreRange(ScoreMinimum.Infinity, ScoreMaximum.Closed(5.2d))))
          } yield assert(result)(equalTo(Chunk("$4\r\n-inf\r\n", "$4\r\n[5.2\r\n")))
        },
        testM("with open min and closed max") {
          for {
            result <- Task(ScoreRangeInput.encode(ScoreRange(ScoreMinimum.Open(4.2d), ScoreMaximum.Closed(5.2d))))
          } yield assert(result)(equalTo(Chunk("$4\r\n(4.2\r\n", "$4\r\n[5.2\r\n")))
        },
        testM("with closed min and closed max") {
          for {
            result <- Task(ScoreRangeInput.encode(ScoreRange(ScoreMinimum.Closed(4.2d), ScoreMaximum.Closed(5.2d))))
          } yield assert(result)(equalTo(Chunk("$4\r\n[4.2\r\n", "$4\r\n[5.2\r\n")))
        }
      ),
      suite("String")(
        testM("non-empty value") {
          for {
            result <- Task(StringInput.encode("non-empty"))
          } yield assert(result)(equalTo(Chunk.single("$9\r\nnon-empty\r\n")))
        },
        testM("empty value") {
          for {
            result <- Task(StringInput.encode(""))
          } yield assert(result)(equalTo(Chunk.single("$0\r\n\r\n")))
        }
      ),
      suite("Optional")(
        testM("none") {
          for {
            result <- Task(OptionalInput(LongInput).encode(None))
          } yield assert(result)(isEmpty)
        },
        testM("some") {
          for {
            result <- Task(OptionalInput(LongInput).encode(Some(2L)))
          } yield assert(result)(equalTo(Chunk.single("$1\r\n2\r\n")))
        }
      ),
      suite("TimeSeconds")(
        testM("positiv value") {
          for {
            result <- Task(TimeSecondsInput.encode(Instant.ofEpochSecond(3L)))
          } yield assert(result)(equalTo(Chunk("$1\r\n3\r\n")))
        },
        testM("zero value") {
          for {
            result <- Task(TimeSecondsInput.encode(Instant.ofEpochSecond(0L)))
          } yield assert(result)(equalTo(Chunk("$1\r\n0\r\n")))
        },
        testM("negative value") {
          for {
            result <- Task(TimeSecondsInput.encode(Instant.ofEpochSecond(-3L)))
          } yield assert(result)(equalTo(Chunk("$2\r\n-3\r\n")))
        }
      ),
      suite("TimeMilliseconds")(
        testM("positiv value") {
          for {
            result <- Task(TimeMillisecondsInput.encode(Instant.ofEpochSecond(3L)))
          } yield assert(result)(equalTo(Chunk("$4\r\n3000\r\n")))
        },
        testM("zero value") {
          for {
            result <- Task(TimeMillisecondsInput.encode(Instant.ofEpochSecond(0L)))
          } yield assert(result)(equalTo(Chunk("$1\r\n0\r\n")))
        },
        testM("negative value") {
          for {
            result <- Task(TimeMillisecondsInput.encode(Instant.ofEpochSecond(-3L)))
          } yield assert(result)(equalTo(Chunk("$5\r\n-3000\r\n")))
        }
      ),
      suite("Tuple2")(
        testM("valid value") {
          for {
            result <- Task(Tuple2(StringInput, LongInput).encode(("one", 2L)))
          } yield assert(result)(equalTo(Chunk("$3\r\none\r\n", "$1\r\n2\r\n")))
        }
      ),
      suite("Tuple3")(
        testM("valid value") {
          for {
            result <- Task(Tuple3(StringInput, LongInput, StringInput).encode(("one", 2, "three")))
          } yield assert(result)(equalTo(Chunk("$3\r\none\r\n", "$1\r\n2\r\n", "$5\r\nthree\r\n")))
        }
      ),
      suite("Tuple4")(
        testM("valid value") {
          for {
            result <- Task(Tuple4(StringInput, LongInput, StringInput, LongInput).encode(("one", 2, "three", 4)))
          } yield assert(result)(equalTo(Chunk("$3\r\none\r\n", "$1\r\n2\r\n", "$5\r\nthree\r\n", "$1\r\n4\r\n")))
        }
      ),
      suite("Tuple5")(
        testM("valid value") {
          for {
            result <- Task(
                        Tuple5(StringInput, LongInput, StringInput, LongInput, StringInput)
                          .encode(("one", 2, "three", 4, "five"))
                      )
          } yield assert(result)(
            equalTo(Chunk("$3\r\none\r\n", "$1\r\n2\r\n", "$5\r\nthree\r\n", "$1\r\n4\r\n", "$4\r\nfive\r\n"))
          )
        }
      ),
      suite("Tuple7")(
        testM("valid value") {
          for {
            result <- Task(
                        Tuple7(StringInput, LongInput, StringInput, LongInput, StringInput, LongInput, StringInput)
                          .encode(("one", 2, "three", 4, "five", 6, "seven"))
                      )
          } yield assert(result)(
            equalTo(
              Chunk(
                "$3\r\none\r\n",
                "$1\r\n2\r\n",
                "$5\r\nthree\r\n",
                "$1\r\n4\r\n",
                "$4\r\nfive\r\n",
                "$1\r\n6\r\n",
                "$5\r\nseven\r\n"
              )
            )
          )
        }
      ),
      suite("Tuple9")(
        testM("valid value") {
          for {
            result <- Task(
                        Tuple9(
                          StringInput,
                          LongInput,
                          StringInput,
                          LongInput,
                          StringInput,
                          LongInput,
                          StringInput,
                          LongInput,
                          StringInput
                        ).encode(("one", 2, "three", 4, "five", 6, "seven", 8, "nine"))
                      )
          } yield assert(result)(
            equalTo(
              Chunk(
                "$3\r\none\r\n",
                "$1\r\n2\r\n",
                "$5\r\nthree\r\n",
                "$1\r\n4\r\n",
                "$4\r\nfive\r\n",
                "$1\r\n6\r\n",
                "$5\r\nseven\r\n",
                "$1\r\n8\r\n",
                "$4\r\nnine\r\n"
              )
            )
          )
        }
      )
    )
}
