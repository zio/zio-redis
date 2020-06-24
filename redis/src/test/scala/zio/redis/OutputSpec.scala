package zio.redis

import zio.{ Chunk, Task }
import zio.duration._
import zio.redis.Output._
import zio.redis.RedisError._
import zio.test._
import zio.test.Assertion._

object OutputSpec extends BaseSpec {
  def spec =
    suite("Output decoders")(
      suite("errors")(
        testM("protocol errors") {
          for {
            res <- Task(BoolOutput.decode(s"-ERR\r\n$Noise\r\n")).either
          } yield assert(res)(isLeft(equalTo(ProtocolError(Noise))))
        },
        testM("wrong type") {
          for {
            res <- Task(BoolOutput.decode(s"-WRONGTYPE\r\n$Noise\r\n")).either
          } yield assert(res)(isLeft(equalTo(WrongType(Noise))))
        }
      ),
      suite("boolean")(
        testM("extract true") {
          for {
            res <- Task(BoolOutput.decode(":1\r\n"))
          } yield assert(res)(isTrue)
        },
        testM("extract false") {
          for {
            res <- Task(BoolOutput.decode(":0\r\n"))
          } yield assert(res)(isFalse)
        },
        testM("report invalid input as protocol error") {
          for {
            res <- Task(BoolOutput.decode(Noise)).either
          } yield assert(res)(isLeft(equalTo(ProtocolError(s"$Noise isn't a boolean."))))
        }
      ),
      suite("chunk")(
        testM("extract empty arrays") {
          for {
            res <- Task(ChunkOutput.decode("*0\r\n"))
          } yield assert(res)(isEmpty)
        },
        testM("extract non-empty arrays") {
          for {
            res <- Task(ChunkOutput.decode("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"))
          } yield assert(res)(hasSameElements(Chunk("foo", "bar")))
        },
        testM("report invalid input as protocol error") {
          for {
            res <- Task(ChunkOutput.decode(Noise)).either
          } yield assert(res)(isLeft(equalTo(ProtocolError(s"$Noise isn't an array."))))
        }
      ),
      suite("double")(
        testM("extract numbers") {
          val num = 42.3
          for {
            res <- Task(DoubleOutput.decode(s"$$4\r\n$num\r\n"))
          } yield assert(res)(equalTo(num))
        },
        testM("report invalid input as protocol error") {
          for {
            res <- Task(DoubleOutput.decode(Noise)).either
          } yield assert(res)(isLeft(equalTo(ProtocolError(s"$Noise isn't a double."))))
        },
        testM("report number format exceptions as protocol errors") {
          val bad = "$2\r\nok\r\n"
          for {
            res <- Task(DoubleOutput.decode(bad)).either
          } yield assert(res)(isLeft(equalTo(ProtocolError(s"$bad isn't a double."))))
        }
      ),
      suite("durations")(
        suite("milliseconds")(
          testM("extract milliseconds") {
            val num = 42
            for {
              res <- Task(DurationMillisecondsOutput.decode(s":$num\r\n"))
            } yield assert(res)(equalTo(num.millis))
          },
          testM("report error for non-existing key") {
            val bad = ":-2\r\n"
            for {
              res <- Task(DurationMillisecondsOutput.decode(bad)).either
            } yield assert(res)(isLeft(equalTo(ProtocolError("Key not found."))))
          },
          testM("report error for key without TTL") {
            val bad = ":-1\r\n"
            for {
              res <- Task(DurationMillisecondsOutput.decode(bad)).either
            } yield assert(res)(isLeft(equalTo(ProtocolError("Key has no expire."))))
          },
          testM("report invalid input as protocol error") {
            for {
              res <- Task(DurationMillisecondsOutput.decode(Noise)).either
            } yield assert(res)(isLeft(equalTo(ProtocolError(s"$Noise isn't a duration."))))
          }
        ),
        suite("seconds")(
          testM("extract seconds") {
            val num = 42
            for {
              res <- Task(DurationSecondsOutput.decode(s":$num\r\n"))
            } yield assert(res)(equalTo(num.seconds))
          },
          testM("report error for non-existing key") {
            val bad = ":-2\r\n"
            for {
              res <- Task(DurationSecondsOutput.decode(bad)).either
            } yield assert(res)(isLeft(equalTo(ProtocolError("Key not found."))))
          },
          testM("report error for key without TTL") {
            val bad = ":-1\r\n"
            for {
              res <- Task(DurationSecondsOutput.decode(bad)).either
            } yield assert(res)(isLeft(equalTo(ProtocolError("Key has no expire."))))
          },
          testM("report invalid input as protocol error") {
            for {
              res <- Task(DurationSecondsOutput.decode(Noise)).either
            } yield assert(res)(isLeft(equalTo(ProtocolError(s"$Noise isn't a duration."))))
          }
        )
      ),
      suite("long")(
        testM("extract positive numbers") {
          val num = 42L
          for {
            res <- Task(LongOutput.decode(s":$num\r\n"))
          } yield assert(res)(equalTo(num))
        },
        testM("extract negative numbers") {
          val num = -42L
          for {
            res <- Task(LongOutput.decode(s":$num\r\n"))
          } yield assert(res)(equalTo(num))
        },
        testM("report invalid input as protocol error") {
          for {
            res <- Task(LongOutput.decode(Noise)).either
          } yield assert(res)(isLeft(equalTo(ProtocolError(s"$Noise isn't a number."))))
        }
      ),
      suite("optional")(
        testM("extract None") {
          for {
            res <- Task(OptionalOutput(UnitOutput).decode("$-1"))
          } yield assert(res)(isNone)
        },
        testM("extract some") {
          for {
            res <- Task(OptionalOutput(UnitOutput).decode("+OK\r\n"))
          } yield assert(res)(isSome(isUnit))
        },
        testM("report invalid input as protocol error") {
          for {
            res <- Task(OptionalOutput(UnitOutput).decode(Noise)).either
          } yield assert(res)(isLeft(equalTo(ProtocolError(s"$Noise isn't unit."))))
        }
      ),
      suite("scan")(
        testM("extract cursor and elements") {
          val input = "*2\r\n$1\r\n5\r\n*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
          for {
            res <- Task(ScanOutput.decode(input))
          } yield assert(res)(equalTo("5" -> Chunk("foo", "bar")))
        },
        testM("report invalid input as protocol error") {
          // TODO: expand failure cases
          for {
            res <- Task(ScanOutput.decode(Noise)).either
          } yield assert(res)(isLeft(equalTo(ProtocolError(s"$Noise isn't scan output."))))
        }
      ),
      suite("string")(
        testM("extract strings") {
          for{
            res  <- Task(StringOutput.decode(s"$$${Noise.length}\r\n$Noise\r\n"))
          } yield assert(res)(equalTo(Noise))
        },
        testM("report invalid input as protocol error") {
          for {
            res <- Task(StringOutput.decode(Noise)).either
          } yield assert(res)(isLeft(equalTo(ProtocolError(s"$Noise isn't a string."))))
        }
      ),
      suite("unit")(
        testM("extract unit") {
          for{
            res <- Task(UnitOutput.decode(s"+OK\r\n"))
          } yield assert(res)(isUnit)
        },
        testM("report invalid input as protocol error") {
          for{
            res <- Task(UnitOutput.decode(Noise)).either
          } yield assert(res)(isLeft(equalTo(ProtocolError(s"$Noise isn't unit."))))
        }
      )
    )

  private val Noise = "bad input"
}
