package zio.redis

import zio.duration._
import zio.redis.RedisError._
import zio.test.Assertion._
import zio.test.TestAspect.ignore
import zio.test._
import zio.{Chunk, Has}

trait StreamsSpec extends BaseSpec {
  val streamsSuite
    : Spec[Has[RedisExecutor.Service] with Has[Annotations.Service], TestFailure[RedisError], TestSuccess] =
    suite("streams")(
      suite("xAck")(
        testM("one message") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- xGroupCreate(stream, group, "$", mkStream = true)
            id       <- xAdd(stream, "*", "a" -> "b").returning[String]
            _        <- xReadGroup(group, consumer)(stream -> ">").returning[String, String]
            result   <- xAck(stream, group, id)
          } yield assert(result)(equalTo(1L))
        },
        testM("multiple messages") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- xGroupCreate(stream, group, "$", mkStream = true)
            first    <- xAdd(stream, "*", "a" -> "b").returning[String]
            second   <- xAdd(stream, "*", "a" -> "b").returning[String]
            _        <- xReadGroup(group, consumer)(stream -> ">").returning[String, String]
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
            id     <- xAdd(stream, "*", "a" -> "b").returning[String]
            result <- xAck(stream, group, id)
          } yield assert(result)(equalTo(0L))
        },
        testM("when message with the given ID doesn't exist") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- xGroupCreate(stream, group, "$", mkStream = true)
            _        <- xAdd(stream, "*", "a" -> "b").returning[String]
            _        <- xReadGroup(group, consumer)(stream -> ">").returning[String, String]
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
            result <- xAdd(stream, id, "a" -> "b").returning[String]
          } yield assert(result)(equalTo(id))
        },
        testM("object with multiple fields") {
          for {
            stream <- uuid
            id      = "1-0"
            result <- xAdd(stream, id, "a" -> "b", "c" -> "d").returning[String]
          } yield assert(result)(equalTo(id))
        },
        testM("error when ID should be greater") {
          for {
            stream <- uuid
            id      = "0-0"
            result <- xAdd(stream, id, "a" -> "b").returning[String].either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when invalid ID format") {
          for {
            stream <- uuid
            id     <- uuid
            result <- xAdd(stream, id, "a" -> "b").returning[String].either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when not stream") {
          for {
            nonStream <- uuid
            id         = "1-0"
            _         <- set(nonStream, "value")
            result    <- xAdd(nonStream, id, "a" -> "b").returning[String].either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("xAddWithMaxLen")(
        testM("with positive count and without approximate") {
          for {
            stream <- uuid
            id      = "1-0"
            result <- xAddWithMaxLen(stream, id, 10)("a" -> "b").returning[String]
          } yield assert(result)(equalTo(id))
        },
        testM("with positive count and with approximate") {
          for {
            stream <- uuid
            id      = "1-0"
            result <- xAddWithMaxLen(stream, id, 10, approximate = true)("a" -> "b").returning[String]
          } yield assert(result)(equalTo(id))
        },
        testM("error with negative count and without approximate") {
          for {
            stream <- uuid
            id      = "1-0"
            result <- xAddWithMaxLen(stream, id, -10)("a" -> "b").returning[String].either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error with negative count and with approximate") {
          for {
            stream <- uuid
            id      = "1-0"
            result <- xAddWithMaxLen(stream, id, -10, approximate = true)("a" -> "b").returning[String].either
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
            id     <- xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <- xClaim(stream, group, second, 0.millis)(id).returning[String, String]
          } yield assert(result)(equalTo(Chunk(StreamEntry(id, Map("a" -> "b")))))
        },
        testM("multiple pending messages") {
          for {
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b").returning[String]
            id1    <- xAdd(stream, "*", "c" -> "d", "e" -> "f").returning[String]
            _      <- xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <- xClaim(stream, group, second, 0.millis)(id, id1).returning[String, String]
          } yield assert(result)(
            equalTo(Chunk(StreamEntry(id, Map("a" -> "b")), StreamEntry(id1, Map("c" -> "d", "e" -> "f"))))
          )
        },
        testM("non-existent message") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- xGroupCreate(stream, group, "$", mkStream = true)
            result   <- xClaim(stream, group, consumer, 0.millis)("1-0").returning[String, String]
          } yield assert(result)(isEmpty)
        },
        testM("existing message that is not in pending state") {
          for {
            stream <- uuid
            group  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b").returning[String]
            result <- xClaim(stream, group, second, 0.millis)(id).returning[String, String]
          } yield assert(result)(isEmpty)
        },
        testM("with non-existent group") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            result   <- xClaim(stream, group, consumer, 0.millis)("1-0").returning[String, String].either
          } yield assert(result)(isLeft(isSubtype[NoGroup](anything)))
        },
        testM("with positive min idle time") {
          for {
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <- xClaim(stream, group, second, 360000.millis)(id).returning[String, String]
          } yield assert(result)(isEmpty)
        },
        testM("with negative min idle time") {
          for {
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <-
              xClaim(stream, group, second, (-360000).millis)(id).returning[String, String]
          } yield assert(result)(equalTo(Chunk(StreamEntry(id, Map("a" -> "b")))))
        },
        testM("with positive idle time") {
          for {
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <- xClaim(stream, group, second, 0.millis, Some(360000.millis))(id).returning[String, String]
          } yield assert(result)(equalTo(Chunk(StreamEntry(id, Map("a" -> "b")))))
        },
        testM("with negative idle time") {
          for {
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <- xClaim(stream, group, second, 0.millis, Some((-360000).millis))(id).returning[String, String]
          } yield assert(result)(equalTo(Chunk(StreamEntry(id, Map("a" -> "b")))))
        },
        testM("with positive time") {
          for {
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <- xClaim(stream, group, second, 0.millis, time = Some(360000.millis))(id).returning[String, String]
          } yield assert(result)(equalTo(Chunk(StreamEntry(id, Map("a" -> "b")))))
        },
        testM("with negative time") {
          for {
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <-
              xClaim(stream, group, second, 0.millis, time = Some((-360000).millis))(id).returning[String, String]
          } yield assert(result)(equalTo(Chunk(StreamEntry(id, Map("a" -> "b")))))
        },
        testM("with positive retry count") {
          for {
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <- xClaim(stream, group, second, 0.millis, retryCount = Some(3))(id).returning[String, String]
          } yield assert(result)(equalTo(Chunk(StreamEntry(id, Map("a" -> "b")))))
        },
        testM("with negative retry count") {
          for {
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <- xClaim(stream, group, second, 0.millis, retryCount = Some(-3))(id).returning[String, String]
          } yield assert(result)(equalTo(Chunk(StreamEntry(id, Map("a" -> "b")))))
        },
        testM("with force when message is not in the pending state") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- xGroupCreate(stream, group, "$", mkStream = true)
            id       <- xAdd(stream, "*", "a" -> "b").returning[String]
            result   <- xClaim(stream, group, consumer, 0.millis, force = true)(id).returning[String, String]
          } yield assert(result)(equalTo(Chunk(StreamEntry(id, Map("a" -> "b")))))
        },
        testM("when not stream") {
          for {
            nonStream <- uuid
            group     <- uuid
            consumer  <- uuid
            _         <- set(nonStream, "value")
            result    <- xClaim(nonStream, group, consumer, 0.millis, force = true)("1-0").returning[String, String].either
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
            id     <- xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <- xClaimWithJustId(stream, group, second, 0.millis)(id).returning[String]
          } yield assert(result)(hasSameElements(Chunk.single(id)))
        },
        testM("multiple pending messages") {
          for {
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b").returning[String]
            id1    <- xAdd(stream, "*", "c" -> "d", "e" -> "f").returning[String]
            _      <- xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <- xClaimWithJustId(stream, group, second, 0.millis)(id, id1).returning[String]
          } yield assert(result)(hasSameElements(Chunk(id, id1)))
        },
        testM("non-existent message") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- xGroupCreate(stream, group, "$", mkStream = true)
            result   <- xClaimWithJustId(stream, group, consumer, 0.millis)("1-0").returning[String]
          } yield assert(result)(isEmpty)
        },
        testM("existing message that is not in pending state") {
          for {
            stream <- uuid
            group  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b").returning[String]
            result <- xClaimWithJustId(stream, group, second, 0.millis)(id).returning[String]
          } yield assert(result)(isEmpty)
        },
        testM("with non-existent group") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            result   <- xClaimWithJustId(stream, group, consumer, 0.millis)("1-0").returning[String].either
          } yield assert(result)(isLeft(isSubtype[NoGroup](anything)))
        },
        testM("with positive min idle time") {
          for {
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <- xClaimWithJustId(stream, group, second, 360000.millis)(id).returning[String]
          } yield assert(result)(isEmpty)
        },
        testM("with negative min idle time") {
          for {
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <- xClaimWithJustId(stream, group, second, (-360000).millis)(id).returning[String]
          } yield assert(result)(hasSameElements(Chunk.single(id)))
        },
        testM("with positive idle time") {
          for {
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <- xClaimWithJustId(stream, group, second, 0.millis, Some(360000.millis))(id).returning[String]
          } yield assert(result)(hasSameElements(Chunk.single(id)))
        },
        testM("with negative idle time") {
          for {
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <- xClaimWithJustId(stream, group, second, 0.millis, Some((-360000).millis))(id).returning[String]
          } yield assert(result)(hasSameElements(Chunk.single(id)))
        },
        testM("with positive time") {
          for {
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <- xClaimWithJustId(stream, group, second, 0.millis, time = Some(360000.millis))(id)
                        .returning[String]
          } yield assert(result)(hasSameElements(Chunk.single(id)))
        },
        testM("with negative time") {
          for {
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <- xClaimWithJustId(stream, group, second, 0.millis, time = Some((-360000).millis))(id)
                        .returning[String]
          } yield assert(result)(hasSameElements(Chunk.single(id)))
        },
        testM("with positive retry count") {
          for {
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <- xClaimWithJustId(stream, group, second, 0.millis, retryCount = Some(3))(id).returning[String]
          } yield assert(result)(hasSameElements(Chunk.single(id)))
        },
        testM("with negative retry count") {
          for {
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <- xClaimWithJustId(stream, group, second, 0.millis, retryCount = Some(-3))(id).returning[String]
          } yield assert(result)(hasSameElements(Chunk.single(id)))
        },
        testM("with force when message is not in the pending state") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- xGroupCreate(stream, group, "$", mkStream = true)
            id       <- xAdd(stream, "*", "a" -> "b").returning[String]
            result   <- xClaimWithJustId(stream, group, consumer, 0.millis, force = true)(id).returning[String]
          } yield assert(result)(hasSameElements(Chunk.single(id)))
        },
        testM("when not stream") {
          for {
            nonStream <- uuid
            group     <- uuid
            consumer  <- uuid
            _         <- set(nonStream, "value")
            result <- xClaimWithJustId(nonStream, group, consumer, 0.millis, force = true)("1-0")
                        .returning[String]
                        .either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("xDel")(
        testM("an existing message") {
          for {
            stream <- uuid
            id     <- xAdd(stream, "*", "a" -> "b").returning[String]
            result <- xDel(stream, id)
          } yield assert(result)(equalTo(1L))
        },
        testM("non-existent message") {
          for {
            stream <- uuid
            result <- xDel(stream, "1-0")
          } yield assert(result)(equalTo(0L))
        },
        testM("with an invalid message ID") {
          for {
            stream <- uuid
            id     <- uuid
            result <- xDel(stream, id)
          } yield assert(result)(equalTo(0L))
        },
        testM("error when not stream") {
          for {
            nonStream <- uuid
            _         <- set(nonStream, "value")
            result    <- xDel(nonStream, "1-0").either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("xGroupCreate")(
        testM("new group") {
          for {
            stream <- uuid
            group  <- uuid
            result <- xGroupCreate(stream, group, "$", mkStream = true).either
          } yield assert(result)(isRight)
        },
        testM("error when stream doesn't exist") {
          for {
            stream <- uuid
            group  <- uuid
            result <- xGroupCreate(stream, group, "$").either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when invalid ID") {
          for {
            stream <- uuid
            group  <- uuid
            id     <- uuid
            result <- xGroupCreate(stream, group, id, mkStream = true).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when group already exists") {
          for {
            stream <- uuid
            group  <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            result <- xGroupCreate(stream, group, "$").either
          } yield assert(result)(isLeft(isSubtype[BusyGroup](anything)))
        },
        testM("error when not stream") {
          for {
            nonStream <- uuid
            group     <- uuid
            _         <- set(nonStream, "value")
            result    <- xGroupCreate(nonStream, group, "$").either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("xGroupSetId")(
        testM("for an existing group and message") {
          for {
            stream <- uuid
            group  <- uuid
            id     <- xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- xGroupCreate(stream, group, "$")
            result <- xGroupSetId(stream, group, id).either
          } yield assert(result)(isRight)
        },
        testM("error when non-existent group and an existing message") {
          for {
            stream <- uuid
            group  <- uuid
            id     <- xAdd(stream, "*", "a" -> "b").returning[String]
            result <- xGroupSetId(stream, group, id).either
          } yield assert(result)(isLeft(isSubtype[NoGroup](anything)))
        },
        testM("error when an invalid ID") {
          for {
            stream <- uuid
            group  <- uuid
            id     <- uuid
            result <- xGroupSetId(stream, group, id).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when not stream") {
          for {
            nonStream <- uuid
            group     <- uuid
            result    <- xGroupSetId(nonStream, group, "1-0").either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        }
      ),
      suite("xGroupDestroy")(
        testM("an existing consumer group") {
          for {
            stream <- uuid
            group  <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            result <- xGroupDestroy(stream, group)
          } yield assert(result)(isTrue)
        },
        testM("non-existent consumer group") {
          for {
            stream <- uuid
            group  <- uuid
            _      <- xAdd(stream, "*", "a" -> "b").returning[String]
            result <- xGroupDestroy(stream, group)
          } yield assert(result)(isFalse)
        },
        testM("error when stream doesn't exist") {
          for {
            stream <- uuid
            group  <- uuid
            result <- xGroupDestroy(stream, group).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when not stream") {
          for {
            nonStream <- uuid
            group     <- uuid
            _         <- set(nonStream, "value")
            result    <- xGroupDestroy(nonStream, group).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("xGroupCreateConsumer")(
        testM("new consumer") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- xGroupCreate(stream, group, "$", mkStream = true)
            result   <- xGroupCreateConsumer(stream, group, consumer).either
          } yield assert(result)(isRight)
        },
        testM("error when group doesn't exist") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            result   <- xGroupCreateConsumer(stream, group, consumer).either
          } yield assert(result)(isLeft)
        },
        testM("error when not stream") {
          for {
            nonStream <- uuid
            group     <- uuid
            consumer  <- uuid
            _         <- set(nonStream, "value")
            result    <- xGroupCreateConsumer(nonStream, group, consumer).either
          } yield assert(result)(isLeft)
        }
      ),
      suite("xGroupDelConsumer")(
        testM("non-existing consumer") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- xGroupCreate(stream, group, "$", mkStream = true)
            result   <- xGroupDelConsumer(stream, group, consumer)
          } yield assert(result)(equalTo(0L))
        },
        testM("existing consumer with one pending message") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- xGroupCreate(stream, group, "$", mkStream = true)
            _        <- xAdd(stream, "*", "a" -> "b").returning[String]
            _        <- xReadGroup(group, consumer)(stream -> ">").returning[String, String]
            result   <- xGroupDelConsumer(stream, group, consumer)
          } yield assert(result)(equalTo(1L))
        },
        testM("existing consumer with multiple pending message") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- xGroupCreate(stream, group, "$", mkStream = true)
            _        <- xAdd(stream, "*", "a" -> "b").returning[String]
            _        <- xAdd(stream, "*", "a" -> "b").returning[String]
            _        <- xReadGroup(group, consumer)(stream -> ">").returning[String, String]
            result   <- xGroupDelConsumer(stream, group, consumer)
          } yield assert(result)(equalTo(2L))
        },
        testM("error when stream doesn't exist") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            result   <- xGroupDelConsumer(stream, group, consumer).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when group doesn't exist") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- xAdd(stream, "*", "a" -> "b").returning[String]
            result   <- xGroupDelConsumer(stream, group, consumer).either
          } yield assert(result)(isLeft(isSubtype[NoGroup](anything)))
        },
        testM("error when not stream") {
          for {
            nonStream <- uuid
            group     <- uuid
            consumer  <- uuid
            _         <- set(nonStream, "value")
            result    <- xGroupDelConsumer(nonStream, group, consumer).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("xLen")(
        testM("empty stream") {
          for {
            stream <- uuid
            group  <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            result <- xLen(stream)
          } yield assert(result)(equalTo(0L))
        },
        testM("non-empty stream") {
          for {
            stream <- uuid
            _      <- xAdd(stream, "*", "a" -> "b").returning[String]
            result <- xLen(stream)
          } yield assert(result)(equalTo(1L))
        },
        testM("error when not stream") {
          for {
            nonStream <- uuid
            _         <- set(nonStream, "value")
            result    <- xLen(nonStream).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("xPending")(
        testM("with an empty stream") {
          for {
            stream <- uuid
            group  <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            result <- xPending(stream, group)
          } yield assert(result)(equalTo(PendingInfo(0L, None, None, Map.empty[String, Long])))
        },
        testM("with one consumer") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- xGroupCreate(stream, group, "$", mkStream = true)
            id       <- xAdd(stream, "*", "a" -> "b").returning[String]
            _        <- xReadGroup(group, consumer)(stream -> ">").returning[String, String]
            result   <- xPending(stream, group)
          } yield assert(result)(equalTo(PendingInfo(1L, Some(id), Some(id), Map(consumer -> 1L))))
        },
        testM("with multiple consumers and multiple messages") {
          for {
            stream   <- uuid
            group    <- uuid
            first    <- uuid
            second   <- uuid
            _        <- xGroupCreate(stream, group, "$", mkStream = true)
            firstMsg <- xAdd(stream, "*", "a" -> "b").returning[String]
            _        <- xReadGroup(group, first)(stream -> ">").returning[String, String]
            lastMsg  <- xAdd(stream, "*", "a" -> "b").returning[String]
            _        <- xReadGroup(group, second)(stream -> ">").returning[String, String]
            result   <- xPending(stream, group)
          } yield assert(result)(
            equalTo(PendingInfo(2L, Some(firstMsg), Some(lastMsg), Map(first -> 1L, second -> 1L)))
          )
        },
        testM("with 0ms idle time") {
          for {
            stream              <- uuid
            group               <- uuid
            consumer            <- uuid
            _                   <- xGroupCreate(stream, group, "$", mkStream = true)
            id                  <- xAdd(stream, "*", "a" -> "b").returning[String]
            _                   <- xReadGroup(group, consumer)(stream -> ">").returning[String, String]
            cons: Option[String] = None
            result              <- xPending(stream, group, "-", "+", 10L, cons, Some(0.millis))
            msg                  = result.head
          } yield assert(msg.id)(equalTo(id)) && assert(msg.owner)(equalTo(consumer)) && assert(msg.counter)(
            equalTo(1L)
          )
        },
        testM("with 60s idle time") {
          for {
            stream              <- uuid
            group               <- uuid
            consumer            <- uuid
            _                   <- xGroupCreate(stream, group, "$", mkStream = true)
            _                   <- xAdd(stream, "*", "a" -> "b").returning[String]
            _                   <- xReadGroup(group, consumer)(stream -> ">").returning[String, String]
            cons: Option[String] = None
            result              <- xPending(stream, group, "-", "+", 10L, cons, Some(1.minute))
          } yield assert(result)(isEmpty)
        },
        testM("error when group doesn't exist") {
          for {
            stream <- uuid
            group  <- uuid
            _      <- xAdd(stream, "*", "a" -> "b").returning[String]
            result <- xPending(stream, group).either
          } yield assert(result)(isLeft(isSubtype[NoGroup](anything)))
        },
        testM("error when not stream") {
          for {
            nonStream <- uuid
            group     <- uuid
            _         <- set(nonStream, "value")
            result    <- xPending(nonStream, group).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("with one message unlimited start, unlimited end and count with value 10") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- xGroupCreate(stream, group, "$", mkStream = true)
            id       <- xAdd(stream, "*", "a" -> "b").returning[String]
            _        <- xReadGroup(group, consumer)(stream -> ">").returning[String, String]
            messages <- xPending[String, String, String, String](stream, group, "-", "+", 10L)
            result    = messages.head
          } yield assert(messages)(hasSize(equalTo(1))) &&
            assert(result.id)(equalTo(id)) &&
            assert(result.owner)(equalTo(consumer)) &&
            assert(result.lastDelivered)(isGreaterThan(0.millis)) &&
            assert(result.counter)(equalTo(1L))
        },
        testM("with multiple message, unlimited start, unlimited end and count with value 10") {
          for {
            stream                     <- uuid
            group                      <- uuid
            first                      <- uuid
            second                     <- uuid
            _                          <- xGroupCreate(stream, group, "$", mkStream = true)
            firstMsg                   <- xAdd(stream, "*", "a" -> "b").returning[String]
            _                          <- xReadGroup(group, first)(stream -> ">").returning[String, String]
            secondMsg                  <- xAdd(stream, "*", "a" -> "b").returning[String]
            _                          <- xReadGroup(group, second)(stream -> ">").returning[String, String]
            messages                   <- xPending[String, String, String, String](stream, group, "-", "+", 10L)
            (firstResult, secondResult) = (messages(0), messages(1))
          } yield assert(messages)(hasSize(equalTo(2))) &&
            assert(firstResult.id)(equalTo(firstMsg)) &&
            assert(firstResult.owner)(equalTo(first)) &&
            assert(firstResult.lastDelivered)(isGreaterThan(0.millis)) &&
            assert(firstResult.counter)(equalTo(1L)) &&
            assert(secondResult.id)(equalTo(secondMsg)) &&
            assert(secondResult.owner)(equalTo(second)) &&
            assert(secondResult.lastDelivered)(isGreaterThan(0.millis)) &&
            assert(secondResult.counter)(equalTo(1L))
        },
        testM("with unlimited start, unlimited end, count with value 10, and the specified consumer") {
          for {
            stream   <- uuid
            group    <- uuid
            first    <- uuid
            second   <- uuid
            _        <- xGroupCreate(stream, group, "$", mkStream = true)
            id       <- xAdd(stream, "*", "a" -> "b").returning[String]
            _        <- xReadGroup(group, first)(stream -> ">").returning[String, String]
            _        <- xAdd(stream, "*", "a" -> "b").returning[String]
            _        <- xReadGroup(group, second)(stream -> ">").returning[String, String]
            messages <- xPending[String, String, String, String](stream, group, "-", "+", 10L, Some(first))
            result    = messages.head
          } yield assert(messages)(hasSize(equalTo(1))) &&
            assert(result.id)(equalTo(id)) &&
            assert(result.owner)(equalTo(first)) &&
            assert(result.lastDelivered)(isGreaterThan(0.millis)) &&
            assert(result.counter)(equalTo(1L))
        },
        testM("error when invalid ID") {
          for {
            stream <- uuid
            group  <- uuid
            result <- xPending[String, String, String, String](stream, group, "-", "invalid", 10L).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        }
      ),
      suite("xRange")(
        testM("with an unlimited start and an unlimited end") {
          for {
            stream <- uuid
            id     <- xAdd(stream, "*", "a" -> "b").returning[String]
            result <- xRange(stream, "-", "+").returning[String, String]
          } yield assert(result)(equalTo(Chunk(StreamEntry(id, Map("a" -> "b")))))
        },
        testM("with the positive count") {
          for {
            stream <- uuid
            first  <- xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- xAdd(stream, "*", "a" -> "b").returning[String]
            result <- xRange(stream, "-", "+", 1L).returning[String, String]
          } yield assert(result)(equalTo(Chunk(StreamEntry(first, Map("a" -> "b")))))
        },
        testM("with the negative count") {
          for {
            stream <- uuid
            _      <- xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- xAdd(stream, "*", "a" -> "b").returning[String]
            result <- xRange(stream, "-", "+", -1L).returning[String, String]
          } yield assert(result)(isEmpty)
        },
        testM("with the zero count") {
          for {
            stream <- uuid
            _      <- xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- xAdd(stream, "*", "a" -> "b").returning[String]
            result <- xRange(stream, "-", "+", 0L).returning[String, String]
          } yield assert(result)(isEmpty)
        },
        testM("when stream doesn't exist") {
          for {
            stream <- uuid
            result <- xRange(stream, "-", "+").returning[String, String]
          } yield assert(result)(isEmpty)
        },
        testM("when start is greater than an end") {
          for {
            stream <- uuid
            result <- xRange(stream, "+", "-").returning[String, String]
          } yield assert(result)(isEmpty)
        },
        testM("error when invalid ID") {
          for {
            stream <- uuid
            result <- xRange(stream, "invalid", "+").returning[String, String].either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when not stream") {
          for {
            nonStream <- uuid
            _         <- set(nonStream, "value")
            result    <- xRange(nonStream, "-", "+").returning[String, String].either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("xRead")(
        testM("from the non-empty stream") {
          for {
            stream <- uuid
            id     <- xAdd(stream, "*", "a" -> "b").returning[String]
            result <- xRead()(stream -> "0-0").returning[String, String]
          } yield assert(result)(equalTo(Chunk(StreamChunk(stream, Chunk(StreamEntry(id, Map("a" -> "b")))))))
        },
        testM("from the stream that doesn't exist") {
          for {
            stream <- uuid
            result <- xRead()(stream -> "0-0").returning[String, String]
          } yield assert(result)(isEmpty)
        },
        testM("from the multiple streams") {
          for {
            first     <- uuid
            second    <- uuid
            firstMsg  <- xAdd(first, "*", "a" -> "b").returning[String]
            secondMsg <- xAdd(second, "*", "a" -> "b").returning[String]
            result    <- xRead()(first -> "0-0", second -> "0-0").returning[String, String]
          } yield assert(result)(
            equalTo(
              Chunk(
                StreamChunk(first, Chunk(StreamEntry(firstMsg, Map("a" -> "b")))),
                StreamChunk(second, Chunk(StreamEntry(secondMsg, Map("a" -> "b"))))
              )
            )
          )
        },
        testM("with the positive count") {
          for {
            stream <- uuid
            id     <- xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- xAdd(stream, "*", "a" -> "b").returning[String]
            result <- xRead(Some(1L))(stream -> "0-0").returning[String, String]
          } yield assert(result)(equalTo(Chunk(StreamChunk(stream, Chunk(StreamEntry(id, Map("a" -> "b")))))))
        },
        testM("with the zero count") {
          for {
            stream    <- uuid
            firstMsg  <- xAdd(stream, "*", "a" -> "b").returning[String]
            secondMsg <- xAdd(stream, "*", "a" -> "b").returning[String]
            result    <- xRead(Some(0L))(stream -> "0-0").returning[String, String]
          } yield assert(result)(
            equalTo(
              Chunk(
                StreamChunk(
                  stream,
                  Chunk(StreamEntry(firstMsg, Map("a" -> "b")), StreamEntry(secondMsg, Map("a" -> "b")))
                )
              )
            )
          )
        },
        testM("with the negative count") {
          for {
            stream    <- uuid
            firstMsg  <- xAdd(stream, "*", "a" -> "b").returning[String]
            secondMsg <- xAdd(stream, "*", "a" -> "b").returning[String]
            result    <- xRead(Some(-1L))(stream -> "0-0").returning[String, String]
          } yield assert(result)(
            equalTo(
              Chunk(
                StreamChunk(
                  stream,
                  Chunk(StreamEntry(firstMsg, Map("a" -> "b")), StreamEntry(secondMsg, Map("a" -> "b")))
                )
              )
            )
          )
        },
        // TODO: can be unignored when connection pool is introduced
        testM("with the 1 second block") {
          for {
            stream <- uuid
            _      <- xAdd(stream, "*", "a" -> "b").returning[String]
            result <- xRead(block = Some(1.second))(stream -> "$").returning[String, String]
          } yield assert(result)(isEmpty)
        } @@ ignore,
        testM("with the 0 second block") {
          for {
            stream <- uuid
            _      <- xAdd(stream, "*", "a" -> "b").returning[String]
            result <- xRead(block = Some(0.second))(stream -> "$").returning[String, String]
          } yield assert(result)(isEmpty)
        } @@ ignore,
        testM("with the -1 second block") {
          for {
            stream <- uuid
            _      <- xAdd(stream, "*", "a" -> "b").returning[String]
            result <- xRead(block = Some((-1).second))(stream -> "$").returning[String, String]
          } yield assert(result)(isEmpty)
        } @@ ignore,
        testM("error when an invalid ID") {
          for {
            stream <- uuid
            _      <- xAdd(stream, "*", "a" -> "b").returning[String]
            result <- xRead()(stream -> "invalid").returning[String, String].either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when not stream") {
          for {
            nonStream <- uuid
            _         <- set(nonStream, "value")
            result    <- xRead()(nonStream -> "0-0").returning[String, String].either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("xReadGroup")(
        testM("when stream has only one message") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- xGroupCreate(stream, group, "$", mkStream = true)
            id       <- xAdd(stream, "*", "a" -> "b").returning[String]
            result   <- xReadGroup(group, consumer)(stream -> ">").returning[String, String]
          } yield assert(result)(equalTo(Chunk(StreamChunk(stream, Chunk(StreamEntry(id, Map("a" -> "b")))))))
        },
        testM("when stream has multiple messages") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- xGroupCreate(stream, group, "$", mkStream = true)
            first    <- xAdd(stream, "*", "a" -> "b").returning[String]
            second   <- xAdd(stream, "*", "a" -> "b").returning[String]
            result   <- xReadGroup(group, consumer)(stream -> ">").returning[String, String]
          } yield assert(result)(
            equalTo(
              Chunk(
                StreamChunk(stream, Chunk(StreamEntry(first, Map("a" -> "b")), StreamEntry(second, Map("a" -> "b"))))
              )
            )
          )
        },
        testM("when empty stream") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- xGroupCreate(stream, group, "$", mkStream = true)
            result   <- xReadGroup(group, consumer)(stream -> ">").returning[String, String]
          } yield assert(result)(isEmpty)
        },
        testM("when multiple streams") {
          for {
            first     <- uuid
            second    <- uuid
            group     <- uuid
            consumer  <- uuid
            _         <- xGroupCreate(first, group, "$", mkStream = true)
            _         <- xGroupCreate(second, group, "$", mkStream = true)
            firstMsg  <- xAdd(first, "*", "a" -> "b").returning[String]
            secondMsg <- xAdd(second, "*", "a" -> "b").returning[String]
            result <-
              xReadGroup(group, consumer)(first -> ">", second -> ">").returning[String, String]
          } yield assert(result)(
            equalTo(
              Chunk(
                StreamChunk(first, Chunk(StreamEntry(firstMsg, Map("a" -> "b")))),
                StreamChunk(second, Chunk(StreamEntry(secondMsg, Map("a" -> "b"))))
              )
            )
          )
        },
        testM("with positive count") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- xGroupCreate(stream, group, "$", mkStream = true)
            first    <- xAdd(stream, "*", "a" -> "b").returning[String]
            _        <- xAdd(stream, "*", "a" -> "b").returning[String]
            result <-
              xReadGroup(group, consumer, Some(1L))(stream -> ">").returning[String, String]
          } yield assert(result)(equalTo(Chunk(StreamChunk(stream, Chunk(StreamEntry(first, Map("a" -> "b")))))))
        },
        testM("with zero count") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- xGroupCreate(stream, group, "$", mkStream = true)
            first    <- xAdd(stream, "*", "a" -> "b").returning[String]
            second   <- xAdd(stream, "*", "a" -> "b").returning[String]
            result <-
              xReadGroup(group, consumer, Some(0L))(stream -> ">").returning[String, String]
          } yield assert(result)(
            equalTo(
              Chunk(
                StreamChunk(stream, Chunk(StreamEntry(first, Map("a" -> "b")), StreamEntry(second, Map("a" -> "b"))))
              )
            )
          )
        },
        testM("with negative count") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- xGroupCreate(stream, group, "$", mkStream = true)
            first    <- xAdd(stream, "*", "a" -> "b").returning[String]
            second   <- xAdd(stream, "*", "a" -> "b").returning[String]
            result <-
              xReadGroup(group, consumer, Some(-1L))(stream -> ">").returning[String, String]
          } yield assert(result)(
            equalTo(
              Chunk(
                StreamChunk(stream, Chunk(StreamEntry(first, Map("a" -> "b")), StreamEntry(second, Map("a" -> "b"))))
              )
            )
          )
        },
        testM("with NOACK flag") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- xGroupCreate(stream, group, "$", mkStream = true)
            _ <-
              xReadGroup(group, consumer, noAck = true)(stream -> ">").returning[String, String]
            result <- xPending(stream, group)
          } yield assert(result.total)(equalTo(0L))
        },
        testM("error when group doesn't exist") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            result   <- xReadGroup(group, consumer)(stream -> ">").returning[String, String].either
          } yield assert(result)(isLeft(isSubtype[NoGroup](anything)))
        },
        testM("error when invalid ID") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- xGroupCreate(stream, group, "$", mkStream = true)
            result <-
              xReadGroup(group, consumer)(stream -> "invalid").returning[String, String].either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when not stream") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- set(stream, "value")
            result   <- xReadGroup(group, consumer)(stream -> ">").returning[String, String].either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("xRevRange")(
        testM("with an unlimited start and an unlimited end") {
          for {
            stream <- uuid
            id     <- xAdd(stream, "*", "a" -> "b").returning[String]
            result <- xRevRange(stream, "+", "-").returning[String, String]
          } yield assert(result)(equalTo(Chunk(StreamEntry(id, Map("a" -> "b")))))
        },
        testM("with the positive count") {
          for {
            stream <- uuid
            _      <- xAdd(stream, "*", "a" -> "b").returning[String]
            second <- xAdd(stream, "*", "a" -> "b").returning[String]
            result <- xRevRange(stream, "+", "-", 1L).returning[String, String]
          } yield assert(result)(equalTo(Chunk(StreamEntry(second, Map("a" -> "b")))))
        },
        testM("with the negative count") {
          for {
            stream <- uuid
            _      <- xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- xAdd(stream, "*", "a" -> "b").returning[String]
            result <- xRevRange(stream, "+", "-", -1L).returning[String, String]
          } yield assert(result)(isEmpty)
        },
        testM("with the zero count") {
          for {
            stream <- uuid
            _      <- xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- xAdd(stream, "*", "a" -> "b").returning[String]
            result <- xRevRange(stream, "+", "-", 0L).returning[String, String]
          } yield assert(result)(isEmpty)
        },
        testM("when stream doesn't exist") {
          for {
            stream <- uuid
            result <- xRevRange(stream, "+", "-").returning[String, String]
          } yield assert(result)(isEmpty)
        },
        testM("when start is greater than an end") {
          for {
            stream <- uuid
            result <- xRevRange(stream, "-", "+").returning[String, String]
          } yield assert(result)(isEmpty)
        },
        testM("error when invalid ID") {
          for {
            stream <- uuid
            result <- xRevRange(stream, "invalid", "-").returning[String, String].either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when not stream") {
          for {
            nonStream <- uuid
            _         <- set(nonStream, "value")
            result    <- xRevRange(nonStream, "+", "-").returning[String, String].either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("xTrim")(
        testM("an empty stream") {
          for {
            stream <- uuid
            result <- xTrim(stream, 1000L)
          } yield assert(result)(equalTo(0L))
        },
        testM("a non-empty stream") {
          for {
            stream <- uuid
            _      <- xAdd(stream, "*", "a" -> "b").returning[String].repeatN(3)
            result <- xTrim(stream, 2L)
          } yield assert(result)(equalTo(2L))
        },
        testM("a non-empty stream with an approximate") {
          for {
            stream <- uuid
            _      <- xAdd(stream, "*", "a" -> "b").returning[String]
            result <- xTrim(stream, 1000L, approximate = true)
          } yield assert(result)(equalTo(0L))
        },
        testM("error when negative count") {
          for {
            stream <- uuid
            _      <- xAdd(stream, "*", "a" -> "b").returning[String]
            result <- xTrim(stream, -1000L).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when not stream") {
          for {
            stream <- uuid
            _      <- set(stream, "value")
            result <- xTrim(stream, 1000L).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("xInfoStream")(
        testM("an existing stream") {
          for {
            stream <- uuid
            group  <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            id     <- xAdd(stream, "*", "a" -> "b").returning[String]
            result <- xInfoStream(stream).returning[String, String, String]
          } yield assert(result.lastEntry.map(_.id))(isSome(equalTo(id)))
        },
        testM("error when no such key") {
          for {
            stream <- uuid
            result <- xInfoStream(stream).returning[String, String, String].either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when not a stream") {
          for {
            nonStream <- uuid
            _         <- set(nonStream, "helloworld")
            result    <- xInfoStream(nonStream).returning[String, String, String].either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("xInfoGroups")(
        testM("of an existing stream") {
          for {
            stream <- uuid
            group  <- uuid
            _      <- xGroupCreate(stream, group, "$", mkStream = true)
            _      <- xAdd(stream, "*", "a" -> "b").returning[String]
            result <- xInfoGroups[String](stream)
          } yield assert(result.toList.head.name)(equalTo(group))
        },
        testM("error when no such key") {
          for {
            stream <- uuid
            result <- xInfoGroups[String](stream).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when not a stream") {
          for {
            nonStream <- uuid
            _         <- set(nonStream, "helloworld")
            result    <- xInfoGroups[String](nonStream).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("xInfoConsumers")(
        testM("of an existing stream") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- xGroupCreate(stream, group, "$", mkStream = true)
            _        <- xAdd(stream, "*", "a" -> "b").returning[String]
            _        <- xReadGroup(group, consumer)(stream -> ">").returning[String, String]
            result   <- xInfoConsumers(stream, group)
          } yield assert(result.toList.head.name)(equalTo(consumer))
        },
        testM("error when no such key") {
          for {
            stream <- uuid
            group  <- uuid
            result <- xInfoConsumers(stream, group).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when not a stream") {
          for {
            nonStream <- uuid
            group     <- uuid
            _         <- set(nonStream, "helloworld")
            result    <- xInfoConsumers(nonStream, group).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("xInfoStreamFull")(
        testM("of an existing stream") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- xGroupCreate(stream, group, "$", mkStream = true)
            id       <- xAdd(stream, "*", "a" -> "b").returning[String]
            _        <- xReadGroup(group, consumer)(stream -> ">").returning[String, String]
            result   <- xInfoStreamFull(stream).returning[String, String, String]
          } yield assert {
            val entries       = result.entries
            val length        = result.length
            val consumersName = result.groups.head.consumers.head.name
            val name          = result.groups.head.name
            (entries, length, consumersName, name)
          }(equalTo(Tuple4(Chunk.apply(StreamEntry(id, Map("a" -> "b"))), 1L, consumer, group)))
        },
        testM("error when no such key") {
          for {
            stream <- uuid
            result <- xInfoStreamFull(stream).returning[String, String, String].either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when not a stream") {
          for {
            nonStream <- uuid
            _         <- set(nonStream, "helloworld")
            result    <- xInfoStreamFull(nonStream).returning[String, String, String].either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      )
    )
}
