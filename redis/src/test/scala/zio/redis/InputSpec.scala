package zio.redis

import zio.{ Chunk, Task }
import zio.redis.Input._
import zio.test._
import zio.test.Assertion._
import BitFieldCommand._
import BitFieldType._

object InputSpec extends BaseSpec {
  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("Input encoders")(
      suite("abs ttl")(
        testM("valid value") {
          for {
            result <- Task(AbsTtlInput.encode(AbsTtl))
          } yield assert(result)(equalTo(Chunk.single("$6\r\nABSTTL\r\n")))
        }
      ),
      suite("aggregate")(
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
      suite("auth")(
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
      suite("bool")(
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
      suite("bit field command")(
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
      )
    )
}
