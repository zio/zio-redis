package zio.redis

import zio.Chunk
import zio.redis.RedisError.{ NoGroup, ProtocolError, WrongType }
import zio.test.Assertion._
import zio.test._
import zio.duration._

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
            id       <- xAdd(stream, "*", "a" -> "b")
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
            first    <- xAdd(stream, "*", "a" -> "b")
            second   <- xAdd(stream, "*", "a" -> "b")
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
            id     <- xAdd(stream, "*", "a" -> "b")
            result <- xAck(stream, group, id)
          } yield assert(result)(equalTo(0L))
        },
        testM("when message with the given ID doesn't exist") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- xGroupCreate(stream, group, "$", mkStream = true)
            _        <- xAdd(stream, "*", "a" -> "b")
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
            result <- xAdd(stream, id, "a" -> "b")
          } yield assert(result)(equalTo(id))
        },
        testM("object with multiple fields") {
          for {
            stream <- uuid
            id      = "1-0"
            result <- xAdd(stream, id, "a" -> "b", "c" -> "d")
          } yield assert(result)(equalTo(id))
        },
        testM("error when ID should be greater") {
          for {
            stream <- uuid
            id      = "0-0"
            result <- xAdd(stream, id, "a" -> "b").either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when invalid ID format") {
          for {
            stream <- uuid
            id     <- uuid
            result <- xAdd(stream, id, "a" -> "b").either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when not stream") {
          for {
            nonStream <- uuid
            id         = "1-0"
            _         <- set(nonStream, "value")
            result    <- xAdd(nonStream, id, "a" -> "b").either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("xAddWithMaxLen")(
        testM("with positive count and without approximate") {
          for {
            stream <- uuid
            id      = "1-0"
            result <- xAddWithMaxLen(stream, id, 10)("a" -> "b")
          } yield assert(result)(equalTo(id))
        },
        testM("with positive count and with approximate") {
          for {
            stream <- uuid
            id      = "1-0"
            result <- xAddWithMaxLen(stream, id, 10, approximate = true)("a" -> "b")
          } yield assert(result)(equalTo(id))
        },
        testM("error with negative count and without approximate") {
          for {
            stream <- uuid
            id      = "1-0"
            result <- xAddWithMaxLen(stream, id, -10)("a" -> "b").either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error with negative count and with approximate") {
          for {
            stream <- uuid
            id      = "1-0"
            result <- xAddWithMaxLen(stream, id, -10, approximate = true)("a" -> "b").either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        }
      ),
      suite("xClaim")(
        testM("one pending message") {
          for {
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b")
            _      <- xReadGroup(group, first)(stream -> ">")
            result <- xClaim(stream, group, second, 0.millis)(id)
          } yield assert(result)(equalTo(Map(id -> Map("a" -> "b"))))
        },
        testM("multiple pending messages") {
          for {
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b")
            id1    <- xAdd(stream, "*", "c" -> "d", "e" -> "f")
            _      <- xReadGroup(group, first)(stream -> ">")
            result <- xClaim(stream, group, second, 0.millis)(id, id1)
          } yield assert(result)(equalTo(Map(id -> Map("a" -> "b"), id1 -> Map("c" -> "d", "e" -> "f"))))
        },
        testM("non-existent message") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- xGroupCreate(stream, group, "$", mkStream = true)
            result   <- xClaim(stream, group, consumer, 0.millis)("1-0")
          } yield assert(result)(isEmpty)
        },
        testM("existing message that is not in pending state") {
          for {
            stream <- uuid
            group  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b")
            result <- xClaim(stream, group, second, 0.millis)(id)
          } yield assert(result)(isEmpty)
        },
        testM("with non-existent group") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            result   <- xClaim(stream, group, consumer, 0.millis)("1-0").either
          } yield assert(result)(isLeft(isSubtype[NoGroup](anything)))
        },
        testM("with positive min idle time") {
          for {
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b")
            _      <- xReadGroup(group, first)(stream -> ">")
            result <- xClaim(stream, group, second, 360000.millis)(id)
          } yield assert(result)(isEmpty)
        },
        testM("with negative min idle time") {
          for {
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b")
            _      <- xReadGroup(group, first)(stream -> ">")
            result <- xClaim(stream, group, second, (-360000).millis)(id)
          } yield assert(result)(equalTo(Map(id -> Map("a" -> "b"))))
        },
        testM("with positive idle time") {
          for {
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b")
            _      <- xReadGroup(group, first)(stream -> ">")
            result <- xClaim(stream, group, second, 0.millis, Some(360000.millis))(id)
          } yield assert(result)(equalTo(Map(id -> Map("a" -> "b"))))
        },
        testM("with negative idle time") {
          for {
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b")
            _      <- xReadGroup(group, first)(stream -> ">")
            result <- xClaim(stream, group, second, 0.millis, Some((-360000).millis))(id)
          } yield assert(result)(equalTo(Map(id -> Map("a" -> "b"))))
        },
        testM("with positive time") {
          for {
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b")
            _      <- xReadGroup(group, first)(stream -> ">")
            result <- xClaim(stream, group, second, 0.millis, time = Some(360000.millis))(id)
          } yield assert(result)(equalTo(Map(id -> Map("a" -> "b"))))
        },
        testM("with negative time") {
          for {
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b")
            _      <- xReadGroup(group, first)(stream -> ">")
            result <- xClaim(stream, group, second, 0.millis, time = Some((-360000).millis))(id)
          } yield assert(result)(equalTo(Map(id -> Map("a" -> "b"))))
        },
        testM("with positive retry count") {
          for {
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b")
            _      <- xReadGroup(group, first)(stream -> ">")
            result <- xClaim(stream, group, second, 0.millis, retryCount = Some(3))(id)
          } yield assert(result)(equalTo(Map(id -> Map("a" -> "b"))))
        },
        testM("with negative retry count") {
          for {
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b")
            _      <- xReadGroup(group, first)(stream -> ">")
            result <- xClaim(stream, group, second, 0.millis, retryCount = Some(-3))(id)
          } yield assert(result)(equalTo(Map(id -> Map("a" -> "b"))))
        },
        testM("with force when message is not in the pending state") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- xGroupCreate(stream, group, "$", mkStream = true)
            id       <- xAdd(stream, "*", "a" -> "b")
            result   <- xClaim(stream, group, consumer, 0.millis, force = true)(id)
          } yield assert(result)(equalTo(Map(id -> Map("a" -> "b"))))
        },
        testM("when not stream") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- set(stream, "value")
            result   <- xClaim(stream, group, consumer, 0.millis, force = true)("1-0").either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("xClaimWithJustId")(
        testM("one pending message") {
          for {
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b")
            _      <- xReadGroup(group, first)(stream -> ">")
            result <- xClaimWithJustId(stream, group, second, 0.millis)(id)
          } yield assert(result)(hasSameElements(Chunk.single(id)))
        },
        testM("multiple pending messages") {
          for {
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b")
            id1    <- xAdd(stream, "*", "c" -> "d", "e" -> "f")
            _      <- xReadGroup(group, first)(stream -> ">")
            result <- xClaimWithJustId(stream, group, second, 0.millis)(id, id1)
          } yield assert(result)(hasSameElements(Chunk(id, id1)))
        },
        testM("non-existent message") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- xGroupCreate(stream, group, "$", mkStream = true)
            result   <- xClaimWithJustId(stream, group, consumer, 0.millis)("1-0")
          } yield assert(result)(isEmpty)
        },
        testM("existing message that is not in pending state") {
          for {
            stream <- uuid
            group  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b")
            result <- xClaimWithJustId(stream, group, second, 0.millis)(id)
          } yield assert(result)(isEmpty)
        },
        testM("with non-existent group") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            result   <- xClaimWithJustId(stream, group, consumer, 0.millis)("1-0").either
          } yield assert(result)(isLeft(isSubtype[NoGroup](anything)))
        },
        testM("with positive min idle time") {
          for {
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b")
            _      <- xReadGroup(group, first)(stream -> ">")
            result <- xClaimWithJustId(stream, group, second, 360000.millis)(id)
          } yield assert(result)(isEmpty)
        },
        testM("with negative min idle time") {
          for {
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b")
            _      <- xReadGroup(group, first)(stream -> ">")
            result <- xClaimWithJustId(stream, group, second, (-360000).millis)(id)
          } yield assert(result)(hasSameElements(Chunk.single(id)))
        },
        testM("with positive idle time") {
          for {
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b")
            _      <- xReadGroup(group, first)(stream -> ">")
            result <- xClaimWithJustId(stream, group, second, 0.millis, Some(360000.millis))(id)
          } yield assert(result)(hasSameElements(Chunk.single(id)))
        },
        testM("with negative idle time") {
          for {
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b")
            _      <- xReadGroup(group, first)(stream -> ">")
            result <- xClaimWithJustId(stream, group, second, 0.millis, Some((-360000).millis))(id)
          } yield assert(result)(hasSameElements(Chunk.single(id)))
        },
        testM("with positive time") {
          for {
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b")
            _      <- xReadGroup(group, first)(stream -> ">")
            result <- xClaimWithJustId(stream, group, second, 0.millis, time = Some(360000.millis))(id)
          } yield assert(result)(hasSameElements(Chunk.single(id)))
        },
        testM("with negative time") {
          for {
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b")
            _      <- xReadGroup(group, first)(stream -> ">")
            result <- xClaimWithJustId(stream, group, second, 0.millis, time = Some((-360000).millis))(id)
          } yield assert(result)(hasSameElements(Chunk.single(id)))
        },
        testM("with positive retry count") {
          for {
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b")
            _      <- xReadGroup(group, first)(stream -> ">")
            result <- xClaimWithJustId(stream, group, second, 0.millis, retryCount = Some(3))(id)
          } yield assert(result)(hasSameElements(Chunk.single(id)))
        },
        testM("with negative retry count") {
          for {
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b")
            _      <- xReadGroup(group, first)(stream -> ">")
            result <- xClaimWithJustId(stream, group, second, 0.millis, retryCount = Some(-3))(id)
          } yield assert(result)(hasSameElements(Chunk.single(id)))
        },
        testM("with force when message is not in the pending state") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- xGroupCreate(stream, group, "$", mkStream = true)
            id       <- xAdd(stream, "*", "a" -> "b")
            result   <- xClaimWithJustId(stream, group, consumer, 0.millis, force = true)(id)
          } yield assert(result)(hasSameElements(Chunk.single(id)))
        },
        testM("when not stream") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- set(stream, "value")
            result   <- xClaimWithJustId(stream, group, consumer, 0.millis, force = true)("1-0").either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      )
    )
}
