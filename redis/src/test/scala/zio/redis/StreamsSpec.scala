package zio.redis

import zio.redis.RedisError.{ ProtocolError, WrongType }
import zio.test.Assertion._
import zio.test._

trait StreamsSpec extends BaseSpec {
  val streamsSuite =
    suite("streams")(
      suite("xAck")(
        testM("one message") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- xGroupCreate(stream, group, "$", mkStream = true)
            id       <- xAdd(stream, "*", "name" -> "Sara")
            _        <- xReadGroup(group, consumer)(stream -> ">")
            result   <- xAck(stream, group, id)
          } yield assert(result)(equalTo(1L))
        },
        testM("multiple messages") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- xGroupCreate(stream, group, "$", mkStream = true)
            first    <- xAdd(stream, "*", "name" -> "Sara")
            second   <- xAdd(stream, "*", "name" -> "Sara")
            _        <- xReadGroup(group, consumer)(stream -> ">")
            result   <- xAck(stream, group, first, second)
          } yield assert(result)(equalTo(2L))
        },
        testM("when stream doesn't exist") {
          for {
            stream <- uuid
            group  <- uuid
            id     <- uuid
            result <- xAck(stream, group, id)
          } yield assert(result)(equalTo(0L))
        },
        testM("when group doesn't exist") {
          for {
            stream <- uuid
            group  <- uuid
            id     <- xAdd(stream, "*", "name" -> "Sara")
            result <- xAck(stream, group, id)
          } yield assert(result)(equalTo(0L))
        },
        testM("when message with the given ID doesn't exist") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- xGroupCreate(stream, group, "$", mkStream = true)
            _        <- xAdd(stream, "*", "name" -> "Sara")
            _        <- xReadGroup(group, consumer)(stream -> ">")
            result   <- xAck(stream, group, "0-0")
          } yield assert(result)(equalTo(0L))
        },
        testM("error when ID has an invalid format") {
          for {
            stream <- uuid
            group  <- uuid
            id     <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            result <- xAck(stream, group, id).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when not stream") {
          for {
            nonStream <- uuid
            group     <- uuid
            id        <- uuid
            _         <- set(nonStream, "value")
            result    <- xAck(nonStream, group, id).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("xAdd")(
        testM("object with one field") {
          for {
            stream <- uuid
            id      = "1-0"
            result <- xAdd(stream, id, "name" -> "Sara")
          } yield assert(result)(equalTo(id))
        },
        testM("object with multiple fields") {
          for {
            stream <- uuid
            id      = "1-0"
            result <- xAdd(stream, id, "name" -> "Sara", "surname" -> "OConnor")
          } yield assert(result)(equalTo(id))
        },
        testM("error when ID should be greater") {
          for {
            stream <- uuid
            id      = "0-0"
            result <- xAdd(stream, id, "name" -> "Sara").either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when invalid ID format") {
          for {
            stream <- uuid
            id     <- uuid
            result <- xAdd(stream, id, "name" -> "Sara").either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when not stream") {
          for {
            nonStream <- uuid
            id         = "1-0"
            _         <- set(nonStream, "value")
            result    <- xAdd(nonStream, id, "name" -> "Sara").either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("xAddWithMaxLen")(
        testM("with positive count and without approximate") {
          for {
            stream <- uuid
            id      = "1-0"
            result <- xAddWithMaxLen(stream, id, 10)("name" -> "Sara")
          } yield assert(result)(equalTo(id))
        },
        testM("with positive count and with approximate") {
          for {
            stream <- uuid
            id      = "1-0"
            result <- xAddWithMaxLen(stream, id, 10, approximate = true)("name" -> "Sara")
          } yield assert(result)(equalTo(id))
        },
        testM("error with negative count and without approximate") {
          for {
            stream <- uuid
            id      = "1-0"
            result <- xAddWithMaxLen(stream, id, -10)("name" -> "Sara").either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error with negative count and with approximate") {
          for {
            stream <- uuid
            id      = "1-0"
            result <- xAddWithMaxLen(stream, id, -10, approximate = true)("name" -> "Sara").either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        }
      )
    )
}
