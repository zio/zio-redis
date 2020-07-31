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
            res <- Task(BoolOutput.unsafeDecode(s"-ERR\r\n$Noise\r\n")).either
          } yield assert(res)(isLeft(equalTo(ProtocolError(Noise))))
        },
        testM("wrong type") {
          for {
            res <- Task(BoolOutput.unsafeDecode(s"-WRONGTYPE\r\n$Noise\r\n")).either
          } yield assert(res)(isLeft(equalTo(WrongType(Noise))))
        }
      ),
      suite("boolean")(
        testM("extract true") {
          for {
            res <- Task(BoolOutput.unsafeDecode(":1\r\n"))
          } yield assert(res)(isTrue)
        },
        testM("extract false") {
          for {
            res <- Task(BoolOutput.unsafeDecode(":0\r\n"))
          } yield assert(res)(isFalse)
        },
        testM("report invalid input as protocol error") {
          for {
            res <- Task(BoolOutput.unsafeDecode(Noise)).either
          } yield assert(res)(isLeft(equalTo(ProtocolError(s"$Noise isn't a boolean."))))
        }
      ),
      suite("chunk")(
        testM("extract empty arrays") {
          for {
            res <- Task(ChunkOutput.unsafeDecode("*0\r\n"))
          } yield assert(res)(isEmpty)
        },
        testM("extract non-empty arrays") {
          for {
            res <- Task(ChunkOutput.unsafeDecode("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"))
          } yield assert(res)(hasSameElements(Chunk("foo", "bar")))
        },
        testM("report invalid input as protocol error") {
          for {
            res <- Task(ChunkOutput.unsafeDecode(Noise)).either
          } yield assert(res)(isLeft(equalTo(ProtocolError(s"$Noise isn't an array."))))
        }
      ),
      suite("double")(
        testM("extract numbers") {
          val num = 42.3
          for {
            res <- Task(DoubleOutput.unsafeDecode(s"$$4\r\n$num\r\n"))
          } yield assert(res)(equalTo(num))
        },
        testM("report invalid input as protocol error") {
          for {
            res <- Task(DoubleOutput.unsafeDecode(Noise)).either
          } yield assert(res)(isLeft(equalTo(ProtocolError(s"$Noise isn't a double."))))
        },
        testM("report number format exceptions as protocol errors") {
          val bad = "$2\r\nok\r\n"
          for {
            res <- Task(DoubleOutput.unsafeDecode(bad)).either
          } yield assert(res)(isLeft(equalTo(ProtocolError(s"$bad isn't a double."))))
        }
      ),
      suite("durations")(
        suite("milliseconds")(
          testM("extract milliseconds") {
            val num = 42
            for {
              res <- Task(DurationMillisecondsOutput.unsafeDecode(s":$num\r\n"))
            } yield assert(res)(equalTo(num.millis))
          },
          testM("report error for non-existing key") {
            val bad = ":-2\r\n"
            for {
              res <- Task(DurationMillisecondsOutput.unsafeDecode(bad)).either
            } yield assert(res)(isLeft(equalTo(ProtocolError("Key not found."))))
          },
          testM("report error for key without TTL") {
            val bad = ":-1\r\n"
            for {
              res <- Task(DurationMillisecondsOutput.unsafeDecode(bad)).either
            } yield assert(res)(isLeft(equalTo(ProtocolError("Key has no expire."))))
          },
          testM("report invalid input as protocol error") {
            for {
              res <- Task(DurationMillisecondsOutput.unsafeDecode(Noise)).either
            } yield assert(res)(isLeft(equalTo(ProtocolError(s"$Noise isn't a duration."))))
          }
        ),
        suite("seconds")(
          testM("extract seconds") {
            val num = 42
            for {
              res <- Task(DurationSecondsOutput.unsafeDecode(s":$num\r\n"))
            } yield assert(res)(equalTo(num.seconds))
          },
          testM("report error for non-existing key") {
            val bad = ":-2\r\n"
            for {
              res <- Task(DurationSecondsOutput.unsafeDecode(bad)).either
            } yield assert(res)(isLeft(equalTo(ProtocolError("Key not found."))))
          },
          testM("report error for key without TTL") {
            val bad = ":-1\r\n"
            for {
              res <- Task(DurationSecondsOutput.unsafeDecode(bad)).either
            } yield assert(res)(isLeft(equalTo(ProtocolError("Key has no expire."))))
          },
          testM("report invalid input as protocol error") {
            for {
              res <- Task(DurationSecondsOutput.unsafeDecode(Noise)).either
            } yield assert(res)(isLeft(equalTo(ProtocolError(s"$Noise isn't a duration."))))
          }
        )
      ),
      suite("long")(
        testM("extract positive numbers") {
          val num = 42L
          for {
            res <- Task(LongOutput.unsafeDecode(s":$num\r\n"))
          } yield assert(res)(equalTo(num))
        },
        testM("extract negative numbers") {
          val num = -42L
          for {
            res <- Task(LongOutput.unsafeDecode(s":$num\r\n"))
          } yield assert(res)(equalTo(num))
        },
        testM("report invalid input as protocol error") {
          for {
            res <- Task(LongOutput.unsafeDecode(Noise)).either
          } yield assert(res)(isLeft(equalTo(ProtocolError(s"$Noise isn't a number."))))
        }
      ),
      suite("optional")(
        testM("extract None") {
          for {
            res <- Task(OptionalOutput(UnitOutput).unsafeDecode("$-1"))
          } yield assert(res)(isNone)
        },
        testM("extract some") {
          for {
            res <- Task(OptionalOutput(UnitOutput).unsafeDecode("+OK\r\n"))
          } yield assert(res)(isSome(isUnit))
        },
        testM("report invalid input as protocol error") {
          for {
            res <- Task(OptionalOutput(UnitOutput).unsafeDecode(Noise)).either
          } yield assert(res)(isLeft(equalTo(ProtocolError(s"$Noise isn't unit."))))
        }
      ),
      suite("scan")(
        testM("extract cursor and elements") {
          val input = "*2\r\n$1\r\n5\r\n*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
          for {
            res <- Task(ScanOutput.unsafeDecode(input))
          } yield assert(res)(equalTo("5" -> Chunk("foo", "bar")))
        },
        testM("report invalid input as protocol error") {
          // TODO: expand failure cases
          for {
            res <- Task(ScanOutput.unsafeDecode(Noise)).either
          } yield assert(res)(isLeft(equalTo(ProtocolError(s"$Noise isn't scan output."))))
        }
      ),
      suite("string")(
        testM("extract strings") {
          for {
            res <- Task(MultiStringOutput.unsafeDecode(s"$$${Noise.length}\r\n$Noise\r\n"))
          } yield assert(res)(equalTo(Noise))
        },
        testM("report invalid input as protocol error") {
          for {
            res <- Task(MultiStringOutput.unsafeDecode(Noise)).either
          } yield assert(res)(isLeft(equalTo(ProtocolError(s"$Noise isn't a string."))))
        }
      ),
      suite("unit")(
        testM("extract unit") {
          for {
            res <- Task(UnitOutput.unsafeDecode(s"+OK\r\n"))
          } yield assert(res)(isUnit)
        },
        testM("report invalid input as protocol error") {
          for {
            res <- Task(UnitOutput.unsafeDecode(Noise)).either
          } yield assert(res)(isLeft(equalTo(ProtocolError(s"$Noise isn't unit."))))
        }
      ),
      suite("keyElem")(
        testM("extract none") {
          for {
            res <- Task(KeyElemOutput.unsafeDecode(s"*-1\r\n"))
          } yield assert(res)(isNone)
        },
        testM("extract key and element") {
          for {
            res <- Task(KeyElemOutput.unsafeDecode("*2\r\n$3\r\nkey\r\n$1\r\na\r\n"))
          } yield assert(res)(isSome(equalTo(("key", "a"))))
        },
        testM("report invalid input as protocol error") {
          for {
            res <- Task(KeyElemOutput.unsafeDecode("*3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n")).either
          } yield assert(res)(isLeft(isSubtype[ProtocolError](anything)))
        }
      )
    )

  private val Noise = "bad input"
}
