package zio.redis

import zio.Chunk
import zio.redis.Output._
import zio.redis.RedisError._
import zio.test._
import zio.test.Assertion._

object OutputSpec extends BaseSpec {
  def spec =
    suite("Output decoders")(
      suite("errors")(
        test("protocol errors") {
          val error = "Some error"
          val res   = BoolOutput.decode(s"-ERR\r\n$error\r\n")
          assert(res)(isLeft(equalTo(ProtocolError(error))))
        },
        test("wrong type") {
          val error = "Some error"
          val res   = BoolOutput.decode(s"-WRONGTYPE\r\n$error\r\n")
          assert(res)(isLeft(equalTo(WrongType(error))))
        }
      ),
      suite("boolean")(
        test("extract true") {
          val res = BoolOutput.decode(":1\r\n")
          assert(res)(isRight(isTrue))
        },
        test("extract false") {
          val res = BoolOutput.decode(":0\r\n")
          assert(res)(isRight(isFalse))
        },
        test("report invalid input as protocol error") {
          val bad = "random input"
          val res = BoolOutput.decode(bad)
          assert(res)(isLeft(equalTo(ProtocolError(s"$bad isn't a boolean."))))
        }
      ),
      suite("chunk")(
        test("extract empty arrays") {
          val res = ChunkOutput.decode("*0\r\n")
          assert(res)(isRight(isEmpty))
        },
        test("extract non-empty arrays") {
          val res = ChunkOutput.decode("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
          assert(res)(isRight(hasSameElements(Chunk("foo", "bar"))))
        },
        test("report invalid input as protocol error") {
          val bad = "random input"
          val res = ChunkOutput.decode(bad)
          assert(res)(isLeft(equalTo(ProtocolError(s"$bad isn't an array."))))
        }
      ),
      suite("double")(
        test("stub") {
          assert(1)(equalTo(1))
        }
      ),
      suite("duration")(
        test("stub") {
          assert(1)(equalTo(1))
        }
      ),
      suite("long")(
        test("extract numbers") {
          val num = 42L
          val res = LongOutput.decode(s":$num\r\n")
          assert(res)(isRight(equalTo(num)))
        },
        test("report invalid input as protocol error") {
          val bad = "random input"
          val res = LongOutput.decode(bad)
          assert(res)(isLeft(equalTo(ProtocolError(s"$bad isn't a number."))))
        }
      ),
      suite("optional")(
        test("extract None") {
          val res = OptionalOutput(UnitOutput).decode("$-1")
          assert(res)(isRight(isNone))
        },
        test("extract some") {
          val res = OptionalOutput(UnitOutput).decode("+OK\r\n")
          assert(res)(isRight(isSome(isUnit)))
        },
        test("report invalid input as protocol error") {
          val bad = "random input"
          val res = OptionalOutput(UnitOutput).decode(bad)
          assert(res)(isLeft(equalTo(ProtocolError(s"$bad isn't unit."))))
        }
      ),
      suite("scan")(
        test("stub") {
          assert(1)(equalTo(1))
        }
      ),
      suite("string")(
        test("extract strings") {
          val text = "text"
          val res  = StringOutput.decode(s"$$${text.length}\r\n$text\r\n")
          assert(res)(isRight(equalTo(text)))
        },
        test("report invalid input as protocol error") {
          val bad = "random input"
          val res = StringOutput.decode(bad)
          assert(res)(isLeft(equalTo(ProtocolError(s"$bad isn't a string."))))
        }
      ),
      suite("unit")(
        test("extract unit") {
          val res = UnitOutput.decode(s"+OK\r\n")
          assert(res)(isRight(isUnit))
        },
        test("report invalid input as protocol error") {
          val bad = "random input"
          val res = UnitOutput.decode(bad)
          assert(res)(isLeft(equalTo(ProtocolError(s"$bad isn't unit."))))
        }
      )
    )
}
