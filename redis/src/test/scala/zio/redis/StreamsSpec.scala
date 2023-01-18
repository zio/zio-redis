package zio.redis

import zio._
import zio.redis.RedisError._
import zio.test.Assertion._
import zio.test.TestAspect.ignore
import zio.test._

trait StreamsSpec extends BaseSpec {
  def streamsSuite: Spec[Redis, RedisError] =
    suite("streams")(
      suite("xAck")(
        test("one message") {
          for {
            redis    <- ZIO.service[Redis]
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            id       <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _        <- redis.xReadGroup(group, consumer)(stream -> ">").returning[String, String]
            result   <- redis.xAck(stream, group, id)
          } yield assert(result)(equalTo(1L))
        },
        test("multiple messages") {
          for {
            redis    <- ZIO.service[Redis]
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            first    <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            second   <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _        <- redis.xReadGroup(group, consumer)(stream -> ">").returning[String, String]
            result   <- redis.xAck(stream, group, first, second)
          } yield assert(result)(equalTo(2L))
        },
        test("when stream doesn't exist") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            id     <- uuid
            result <- redis.xAck(stream, group, id)
          } yield assert(result)(equalTo(0L))
        },
        test("when group doesn't exist") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            id     <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            result <- redis.xAck(stream, group, id)
          } yield assert(result)(equalTo(0L))
        },
        test("when message with the given ID doesn't exist") {
          for {
            redis    <- ZIO.service[Redis]
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            _        <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _        <- redis.xReadGroup(group, consumer)(stream -> ">").returning[String, String]
            result   <- redis.xAck(stream, group, "0-0")
          } yield assert(result)(equalTo(0L))
        },
        test("error when ID has an invalid format") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            id     <- uuid
            _      <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            result <- redis.xAck(stream, group, id).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when not stream") {
          for {
            redis     <- ZIO.service[Redis]
            nonStream <- uuid
            group     <- uuid
            id        <- uuid
            _         <- redis.set(nonStream, "value")
            result    <- redis.xAck(nonStream, group, id).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("xAdd")(
        test("object with one field") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            id      = "1-0"
            result <- redis.xAdd(stream, id, "a" -> "b").returning[String]
          } yield assert(result)(equalTo(id))
        },
        test("object with multiple fields") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            id      = "1-0"
            result <- redis.xAdd(stream, id, "a" -> "b", "c" -> "d").returning[String]
          } yield assert(result)(equalTo(id))
        },
        test("error when ID should be greater") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            id      = "0-0"
            result <- redis.xAdd(stream, id, "a" -> "b").returning[String].either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when invalid ID format") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            id     <- uuid
            result <- redis.xAdd(stream, id, "a" -> "b").returning[String].either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when not stream") {
          for {
            redis     <- ZIO.service[Redis]
            nonStream <- uuid
            id         = "1-0"
            _         <- redis.set(nonStream, "value")
            result    <- redis.xAdd(nonStream, id, "a" -> "b").returning[String].either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("xAddWithMaxLen")(
        test("with positive count and without approximate") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            id      = "1-0"
            result <- redis.xAddWithMaxLen(stream, id, 10)("a" -> "b").returning[String]
          } yield assert(result)(equalTo(id))
        },
        test("with positive count and with approximate") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            id      = "1-0"
            result <- redis.xAddWithMaxLen(stream, id, 10, approximate = true)("a" -> "b").returning[String]
          } yield assert(result)(equalTo(id))
        },
        test("error with negative count and without approximate") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            id      = "1-0"
            result <- redis.xAddWithMaxLen(stream, id, -10)("a" -> "b").returning[String].either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error with negative count and with approximate") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            id      = "1-0"
            result <- redis.xAddWithMaxLen(stream, id, -10, approximate = true)("a" -> "b").returning[String].either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        }
      ),
      suite("xClaim")(
        test("one pending message") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            id     <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- redis.xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <- redis.xClaim(stream, group, second, 0.millis)(id).returning[String, String]
          } yield assert(result)(equalTo(Chunk(StreamEntry(id, Map("a" -> "b")))))
        },
        test("multiple pending messages") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            id     <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            id1    <- redis.xAdd(stream, "*", "c" -> "d", "e" -> "f").returning[String]
            _      <- redis.xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <- redis.xClaim(stream, group, second, 0.millis)(id, id1).returning[String, String]
          } yield assert(result)(
            equalTo(Chunk(StreamEntry(id, Map("a" -> "b")), StreamEntry(id1, Map("c" -> "d", "e" -> "f"))))
          )
        },
        test("non-existent message") {
          for {
            redis    <- ZIO.service[Redis]
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            result   <- redis.xClaim(stream, group, consumer, 0.millis)("1-0").returning[String, String]
          } yield assert(result)(isEmpty)
        },
        test("existing message that is not in pending state") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            second <- uuid
            _      <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            id     <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            result <- redis.xClaim(stream, group, second, 0.millis)(id).returning[String, String]
          } yield assert(result)(isEmpty)
        },
        test("with non-existent group") {
          for {
            redis    <- ZIO.service[Redis]
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            result   <- redis.xClaim(stream, group, consumer, 0.millis)("1-0").returning[String, String].either
          } yield assert(result)(isLeft(isSubtype[NoGroup](anything)))
        },
        test("with positive min idle time") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            id     <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- redis.xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <- redis.xClaim(stream, group, second, 360000.millis)(id).returning[String, String]
          } yield assert(result)(isEmpty)
        },
        test("with negative min idle time") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            id     <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- redis.xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <- redis.xClaim(stream, group, second, (-360000).millis)(id).returning[String, String]
          } yield assert(result)(equalTo(Chunk(StreamEntry(id, Map("a" -> "b")))))
        },
        test("with positive idle time") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            id     <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- redis.xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <- redis.xClaim(stream, group, second, 0.millis, Some(360000.millis))(id).returning[String, String]
          } yield assert(result)(equalTo(Chunk(StreamEntry(id, Map("a" -> "b")))))
        },
        test("with negative idle time") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            id     <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- redis.xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <-
              redis.xClaim(stream, group, second, 0.millis, Some((-360000).millis))(id).returning[String, String]
          } yield assert(result)(equalTo(Chunk(StreamEntry(id, Map("a" -> "b")))))
        },
        test("with positive time") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            id     <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- redis.xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <-
              redis.xClaim(stream, group, second, 0.millis, time = Some(360000.millis))(id).returning[String, String]
          } yield assert(result)(equalTo(Chunk(StreamEntry(id, Map("a" -> "b")))))
        },
        test("with negative time") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            id     <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- redis.xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <-
              redis.xClaim(stream, group, second, 0.millis, time = Some((-360000).millis))(id).returning[String, String]
          } yield assert(result)(equalTo(Chunk(StreamEntry(id, Map("a" -> "b")))))
        },
        test("with positive retry count") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            id     <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- redis.xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <- redis.xClaim(stream, group, second, 0.millis, retryCount = Some(3))(id).returning[String, String]
          } yield assert(result)(equalTo(Chunk(StreamEntry(id, Map("a" -> "b")))))
        },
        test("with negative retry count") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            id     <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- redis.xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <- redis.xClaim(stream, group, second, 0.millis, retryCount = Some(-3))(id).returning[String, String]
          } yield assert(result)(equalTo(Chunk(StreamEntry(id, Map("a" -> "b")))))
        },
        test("with force when message is not in the pending state") {
          for {
            redis    <- ZIO.service[Redis]
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            id       <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            result   <- redis.xClaim(stream, group, consumer, 0.millis, force = true)(id).returning[String, String]
          } yield assert(result)(equalTo(Chunk(StreamEntry(id, Map("a" -> "b")))))
        },
        test("when not stream") {
          for {
            redis     <- ZIO.service[Redis]
            nonStream <- uuid
            group     <- uuid
            consumer  <- uuid
            _         <- redis.set(nonStream, "value")
            result <-
              redis.xClaim(nonStream, group, consumer, 0.millis, force = true)("1-0").returning[String, String].either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("xClaimWithJustId")(
        test("one pending message") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            id     <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- redis.xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <- redis.xClaimWithJustId(stream, group, second, 0.millis)(id).returning[String]
          } yield assert(result)(hasSameElements(Chunk.single(id)))
        },
        test("multiple pending messages") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            id     <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            id1    <- redis.xAdd(stream, "*", "c" -> "d", "e" -> "f").returning[String]
            _      <- redis.xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <- redis.xClaimWithJustId(stream, group, second, 0.millis)(id, id1).returning[String]
          } yield assert(result)(hasSameElements(Chunk(id, id1)))
        },
        test("non-existent message") {
          for {
            redis    <- ZIO.service[Redis]
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            result   <- redis.xClaimWithJustId(stream, group, consumer, 0.millis)("1-0").returning[String]
          } yield assert(result)(isEmpty)
        },
        test("existing message that is not in pending state") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            second <- uuid
            _      <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            id     <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            result <- redis.xClaimWithJustId(stream, group, second, 0.millis)(id).returning[String]
          } yield assert(result)(isEmpty)
        },
        test("with non-existent group") {
          for {
            redis    <- ZIO.service[Redis]
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            result   <- redis.xClaimWithJustId(stream, group, consumer, 0.millis)("1-0").returning[String].either
          } yield assert(result)(isLeft(isSubtype[NoGroup](anything)))
        },
        test("with positive min idle time") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            id     <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- redis.xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <- redis.xClaimWithJustId(stream, group, second, 360000.millis)(id).returning[String]
          } yield assert(result)(isEmpty)
        },
        test("with negative min idle time") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            id     <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- redis.xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <- redis.xClaimWithJustId(stream, group, second, (-360000).millis)(id).returning[String]
          } yield assert(result)(hasSameElements(Chunk.single(id)))
        },
        test("with positive idle time") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            id     <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- redis.xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <- redis.xClaimWithJustId(stream, group, second, 0.millis, Some(360000.millis))(id).returning[String]
          } yield assert(result)(hasSameElements(Chunk.single(id)))
        },
        test("with negative idle time") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            id     <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- redis.xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <-
              redis.xClaimWithJustId(stream, group, second, 0.millis, Some((-360000).millis))(id).returning[String]
          } yield assert(result)(hasSameElements(Chunk.single(id)))
        },
        test("with positive time") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            id     <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- redis.xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <- redis
                        .xClaimWithJustId(stream, group, second, 0.millis, time = Some(360000.millis))(id)
                        .returning[String]
          } yield assert(result)(hasSameElements(Chunk.single(id)))
        },
        test("with negative time") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            id     <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- redis.xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <- redis
                        .xClaimWithJustId(stream, group, second, 0.millis, time = Some((-360000).millis))(id)
                        .returning[String]
          } yield assert(result)(hasSameElements(Chunk.single(id)))
        },
        test("with positive retry count") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            id     <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- redis.xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <-
              redis.xClaimWithJustId(stream, group, second, 0.millis, retryCount = Some(3))(id).returning[String]
          } yield assert(result)(hasSameElements(Chunk.single(id)))
        },
        test("with negative retry count") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            first  <- uuid
            second <- uuid
            _      <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            id     <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- redis.xReadGroup(group, first)(stream -> ">").returning[String, String]
            result <-
              redis.xClaimWithJustId(stream, group, second, 0.millis, retryCount = Some(-3))(id).returning[String]
          } yield assert(result)(hasSameElements(Chunk.single(id)))
        },
        test("with force when message is not in the pending state") {
          for {
            redis    <- ZIO.service[Redis]
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            id       <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            result   <- redis.xClaimWithJustId(stream, group, consumer, 0.millis, force = true)(id).returning[String]
          } yield assert(result)(hasSameElements(Chunk.single(id)))
        },
        test("when not stream") {
          for {
            redis     <- ZIO.service[Redis]
            nonStream <- uuid
            group     <- uuid
            consumer  <- uuid
            _         <- redis.set(nonStream, "value")
            result <- redis
                        .xClaimWithJustId(nonStream, group, consumer, 0.millis, force = true)("1-0")
                        .returning[String]
                        .either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("xDel")(
        test("an existing message") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            id     <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            result <- redis.xDel(stream, id)
          } yield assert(result)(equalTo(1L))
        },
        test("non-existent message") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            result <- redis.xDel(stream, "1-0")
          } yield assert(result)(equalTo(0L))
        },
        test("with an invalid message ID") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            id     <- uuid
            result <- redis.xDel(stream, id)
          } yield assert(result)(equalTo(0L))
        },
        test("error when not stream") {
          for {
            redis     <- ZIO.service[Redis]
            nonStream <- uuid
            _         <- redis.set(nonStream, "value")
            result    <- redis.xDel(nonStream, "1-0").either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("xGroupCreate")(
        test("new group") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            result <- redis.xGroupCreate(stream, group, "$", mkStream = true).either
          } yield assert(result)(isRight)
        },
        test("error when stream doesn't exist") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            result <- redis.xGroupCreate(stream, group, "$").either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when invalid ID") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            id     <- uuid
            result <- redis.xGroupCreate(stream, group, id, mkStream = true).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when group already exists") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            _      <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            result <- redis.xGroupCreate(stream, group, "$").either
          } yield assert(result)(isLeft(isSubtype[BusyGroup](anything)))
        },
        test("error when not stream") {
          for {
            redis     <- ZIO.service[Redis]
            nonStream <- uuid
            group     <- uuid
            _         <- redis.set(nonStream, "value")
            result    <- redis.xGroupCreate(nonStream, group, "$").either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("xGroupSetId")(
        test("for an existing group and message") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            id     <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- redis.xGroupCreate(stream, group, "$")
            result <- redis.xGroupSetId(stream, group, id).either
          } yield assert(result)(isRight)
        },
        test("error when non-existent group and an existing message") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            id     <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            result <- redis.xGroupSetId(stream, group, id).either
          } yield assert(result)(isLeft(isSubtype[NoGroup](anything)))
        },
        test("error when an invalid ID") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            id     <- uuid
            result <- redis.xGroupSetId(stream, group, id).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when not stream") {
          for {
            redis     <- ZIO.service[Redis]
            nonStream <- uuid
            group     <- uuid
            result    <- redis.xGroupSetId(nonStream, group, "1-0").either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        }
      ),
      suite("xGroupDestroy")(
        test("an existing consumer group") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            _      <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            result <- redis.xGroupDestroy(stream, group)
          } yield assert(result)(isTrue)
        },
        test("non-existent consumer group") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            _      <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            result <- redis.xGroupDestroy(stream, group)
          } yield assert(result)(isFalse)
        },
        test("error when stream doesn't exist") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            result <- redis.xGroupDestroy(stream, group).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when not stream") {
          for {
            redis     <- ZIO.service[Redis]
            nonStream <- uuid
            group     <- uuid
            _         <- redis.set(nonStream, "value")
            result    <- redis.xGroupDestroy(nonStream, group).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("xGroupCreateConsumer")(
        test("new consumer") {
          for {
            redis    <- ZIO.service[Redis]
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            result   <- redis.xGroupCreateConsumer(stream, group, consumer).either
          } yield assert(result)(isRight)
        },
        test("error when group doesn't exist") {
          for {
            redis    <- ZIO.service[Redis]
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            result   <- redis.xGroupCreateConsumer(stream, group, consumer).either
          } yield assert(result)(isLeft)
        },
        test("error when not stream") {
          for {
            redis     <- ZIO.service[Redis]
            nonStream <- uuid
            group     <- uuid
            consumer  <- uuid
            _         <- redis.set(nonStream, "value")
            result    <- redis.xGroupCreateConsumer(nonStream, group, consumer).either
          } yield assert(result)(isLeft)
        }
      ),
      suite("xGroupDelConsumer")(
        test("non-existing consumer") {
          for {
            redis    <- ZIO.service[Redis]
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            result   <- redis.xGroupDelConsumer(stream, group, consumer)
          } yield assert(result)(equalTo(0L))
        },
        test("existing consumer with one pending message") {
          for {
            redis    <- ZIO.service[Redis]
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            _        <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _        <- redis.xReadGroup(group, consumer)(stream -> ">").returning[String, String]
            result   <- redis.xGroupDelConsumer(stream, group, consumer)
          } yield assert(result)(equalTo(1L))
        },
        test("existing consumer with multiple pending message") {
          for {
            redis    <- ZIO.service[Redis]
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            _        <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _        <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _        <- redis.xReadGroup(group, consumer)(stream -> ">").returning[String, String]
            result   <- redis.xGroupDelConsumer(stream, group, consumer)
          } yield assert(result)(equalTo(2L))
        },
        test("error when stream doesn't exist") {
          for {
            redis    <- ZIO.service[Redis]
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            result   <- redis.xGroupDelConsumer(stream, group, consumer).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when group doesn't exist") {
          for {
            redis    <- ZIO.service[Redis]
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            result   <- redis.xGroupDelConsumer(stream, group, consumer).either
          } yield assert(result)(isLeft(isSubtype[NoGroup](anything)))
        },
        test("error when not stream") {
          for {
            redis     <- ZIO.service[Redis]
            nonStream <- uuid
            group     <- uuid
            consumer  <- uuid
            _         <- redis.set(nonStream, "value")
            result    <- redis.xGroupDelConsumer(nonStream, group, consumer).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("xLen")(
        test("empty stream") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            _      <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            result <- redis.xLen(stream)
          } yield assert(result)(equalTo(0L))
        },
        test("non-empty stream") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            _      <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            result <- redis.xLen(stream)
          } yield assert(result)(equalTo(1L))
        },
        test("error when not stream") {
          for {
            redis     <- ZIO.service[Redis]
            nonStream <- uuid
            _         <- redis.set(nonStream, "value")
            result    <- redis.xLen(nonStream).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("xPending")(
        test("with an empty stream") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            _      <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            result <- redis.xPending(stream, group)
          } yield assert(result)(equalTo(PendingInfo(0L, None, None, Map.empty[String, Long])))
        },
        test("with one consumer") {
          for {
            redis    <- ZIO.service[Redis]
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            id       <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _        <- redis.xReadGroup(group, consumer)(stream -> ">").returning[String, String]
            result   <- redis.xPending(stream, group)
          } yield assert(result)(equalTo(PendingInfo(1L, Some(id), Some(id), Map(consumer -> 1L))))
        },
        test("with multiple consumers and multiple messages") {
          for {
            redis    <- ZIO.service[Redis]
            stream   <- uuid
            group    <- uuid
            first    <- uuid
            second   <- uuid
            _        <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            firstMsg <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _        <- redis.xReadGroup(group, first)(stream -> ">").returning[String, String]
            lastMsg  <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _        <- redis.xReadGroup(group, second)(stream -> ">").returning[String, String]
            result   <- redis.xPending(stream, group)
          } yield assert(result)(
            equalTo(PendingInfo(2L, Some(firstMsg), Some(lastMsg), Map(first -> 1L, second -> 1L)))
          )
        },
        test("with 0ms idle time") {
          for {
            redis               <- ZIO.service[Redis]
            stream              <- uuid
            group               <- uuid
            consumer            <- uuid
            _                   <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            id                  <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _                   <- redis.xReadGroup(group, consumer)(stream -> ">").returning[String, String]
            cons: Option[String] = None
            result              <- redis.xPending(stream, group, "-", "+", 10L, cons, Some(0.millis))
            msg                  = result.head
          } yield assert(msg.id)(equalTo(id)) && assert(msg.owner)(equalTo(consumer)) && assert(msg.counter)(
            equalTo(1L)
          )
        },
        test("with 60s idle time") {
          for {
            redis               <- ZIO.service[Redis]
            stream              <- uuid
            group               <- uuid
            consumer            <- uuid
            _                   <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            _                   <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _                   <- redis.xReadGroup(group, consumer)(stream -> ">").returning[String, String]
            cons: Option[String] = None
            result              <- redis.xPending(stream, group, "-", "+", 10L, cons, Some(1.minute))
          } yield assert(result)(isEmpty)
        },
        test("error when group doesn't exist") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            _      <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            result <- redis.xPending(stream, group).either
          } yield assert(result)(isLeft(isSubtype[NoGroup](anything)))
        },
        test("error when not stream") {
          for {
            redis     <- ZIO.service[Redis]
            nonStream <- uuid
            group     <- uuid
            _         <- redis.set(nonStream, "value")
            result    <- redis.xPending(nonStream, group).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        },
        test("with one message unlimited start, unlimited end and count with value 10") {
          for {
            redis    <- ZIO.service[Redis]
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            id       <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _        <- redis.xReadGroup(group, consumer)(stream -> ">").returning[String, String]
            messages <- redis.xPending[String, String, String, String](stream, group, "-", "+", 10L)
            result    = messages.head
          } yield assert(messages)(hasSize(equalTo(1))) &&
            assert(result.id)(equalTo(id)) &&
            assert(result.owner)(equalTo(consumer)) &&
            assert(result.lastDelivered)(isGreaterThan(0.millis)) &&
            assert(result.counter)(equalTo(1L))
        },
        test("with multiple message, unlimited start, unlimited end and count with value 10") {
          for {
            redis                      <- ZIO.service[Redis]
            stream                     <- uuid
            group                      <- uuid
            first                      <- uuid
            second                     <- uuid
            _                          <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            firstMsg                   <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _                          <- redis.xReadGroup(group, first)(stream -> ">").returning[String, String]
            secondMsg                  <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _                          <- redis.xReadGroup(group, second)(stream -> ">").returning[String, String]
            messages                   <- redis.xPending[String, String, String, String](stream, group, "-", "+", 10L)
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
        test("with unlimited start, unlimited end, count with value 10, and the specified consumer") {
          for {
            redis    <- ZIO.service[Redis]
            stream   <- uuid
            group    <- uuid
            first    <- uuid
            second   <- uuid
            _        <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            id       <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _        <- redis.xReadGroup(group, first)(stream -> ">").returning[String, String]
            _        <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _        <- redis.xReadGroup(group, second)(stream -> ">").returning[String, String]
            messages <- redis.xPending[String, String, String, String](stream, group, "-", "+", 10L, Some(first))
            result    = messages.head
          } yield assert(messages)(hasSize(equalTo(1))) &&
            assert(result.id)(equalTo(id)) &&
            assert(result.owner)(equalTo(first)) &&
            assert(result.lastDelivered)(isGreaterThan(0.millis)) &&
            assert(result.counter)(equalTo(1L))
        },
        test("error when invalid ID") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            result <- redis.xPending[String, String, String, String](stream, group, "-", "invalid", 10L).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        }
      ),
      suite("xRange")(
        test("with an unlimited start and an unlimited end") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            id     <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            result <- redis.xRange(stream, "-", "+").returning[String, String]
          } yield assert(result)(equalTo(Chunk(StreamEntry(id, Map("a" -> "b")))))
        },
        test("with the positive count") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            first  <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            result <- redis.xRange(stream, "-", "+", 1L).returning[String, String]
          } yield assert(result)(equalTo(Chunk(StreamEntry(first, Map("a" -> "b")))))
        },
        test("with the negative count") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            _      <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            result <- redis.xRange(stream, "-", "+", -1L).returning[String, String]
          } yield assert(result)(isEmpty)
        },
        test("with the zero count") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            _      <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            result <- redis.xRange(stream, "-", "+", 0L).returning[String, String]
          } yield assert(result)(isEmpty)
        },
        test("when stream doesn't exist") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            result <- redis.xRange(stream, "-", "+").returning[String, String]
          } yield assert(result)(isEmpty)
        },
        test("when start is greater than an end") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            result <- redis.xRange(stream, "+", "-").returning[String, String]
          } yield assert(result)(isEmpty)
        },
        test("error when invalid ID") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            result <- redis.xRange(stream, "invalid", "+").returning[String, String].either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when not stream") {
          for {
            redis     <- ZIO.service[Redis]
            nonStream <- uuid
            _         <- redis.set(nonStream, "value")
            result    <- redis.xRange(nonStream, "-", "+").returning[String, String].either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("xRead")(
        test("from the non-empty stream") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            id     <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            result <- redis.xRead()(stream -> "0-0").returning[String, String]
          } yield assert(result)(equalTo(Chunk(StreamChunk(stream, Chunk(StreamEntry(id, Map("a" -> "b")))))))
        },
        test("from the stream that doesn't exist") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            result <- redis.xRead()(stream -> "0-0").returning[String, String]
          } yield assert(result)(isEmpty)
        },
        test("from the multiple streams") {
          for {
            redis     <- ZIO.service[Redis]
            first     <- uuid
            second    <- uuid
            firstMsg  <- redis.xAdd(first, "*", "a" -> "b").returning[String]
            secondMsg <- redis.xAdd(second, "*", "a" -> "b").returning[String]
            result    <- redis.xRead()(first -> "0-0", second -> "0-0").returning[String, String]
          } yield assert(result)(
            equalTo(
              Chunk(
                StreamChunk(first, Chunk(StreamEntry(firstMsg, Map("a" -> "b")))),
                StreamChunk(second, Chunk(StreamEntry(secondMsg, Map("a" -> "b"))))
              )
            )
          )
        },
        test("with the positive count") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            id     <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            result <- redis.xRead(Some(1L))(stream -> "0-0").returning[String, String]
          } yield assert(result)(equalTo(Chunk(StreamChunk(stream, Chunk(StreamEntry(id, Map("a" -> "b")))))))
        },
        test("with the zero count") {
          for {
            redis     <- ZIO.service[Redis]
            stream    <- uuid
            firstMsg  <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            secondMsg <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            result    <- redis.xRead(Some(0L))(stream -> "0-0").returning[String, String]
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
        test("with the negative count") {
          for {
            redis     <- ZIO.service[Redis]
            stream    <- uuid
            firstMsg  <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            secondMsg <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            result    <- redis.xRead(Some(-1L))(stream -> "0-0").returning[String, String]
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
        test("with the 1 second block") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            _      <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            result <- redis.xRead(block = Some(1.second))(stream -> "$").returning[String, String]
          } yield assert(result)(isEmpty)
        } @@ ignore,
        test("with the 0 second block") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            _      <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            result <- redis.xRead(block = Some(0.second))(stream -> "$").returning[String, String]
          } yield assert(result)(isEmpty)
        } @@ ignore,
        test("with the -1 second block") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            _      <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            result <- redis.xRead(block = Some((-1).second))(stream -> "$").returning[String, String]
          } yield assert(result)(isEmpty)
        } @@ ignore,
        test("error when an invalid ID") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            _      <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            result <- redis.xRead()(stream -> "invalid").returning[String, String].either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when not stream") {
          for {
            redis     <- ZIO.service[Redis]
            nonStream <- uuid
            _         <- redis.set(nonStream, "value")
            result    <- redis.xRead()(nonStream -> "0-0").returning[String, String].either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("xReadGroup")(
        test("when stream has only one message") {
          for {
            redis    <- ZIO.service[Redis]
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            id       <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            result   <- redis.xReadGroup(group, consumer)(stream -> ">").returning[String, String]
          } yield assert(result)(equalTo(Chunk(StreamChunk(stream, Chunk(StreamEntry(id, Map("a" -> "b")))))))
        },
        test("when stream has multiple messages") {
          for {
            redis    <- ZIO.service[Redis]
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            first    <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            second   <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            result   <- redis.xReadGroup(group, consumer)(stream -> ">").returning[String, String]
          } yield assert(result)(
            equalTo(
              Chunk(
                StreamChunk(stream, Chunk(StreamEntry(first, Map("a" -> "b")), StreamEntry(second, Map("a" -> "b"))))
              )
            )
          )
        },
        test("when empty stream") {
          for {
            redis    <- ZIO.service[Redis]
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            result   <- redis.xReadGroup(group, consumer)(stream -> ">").returning[String, String]
          } yield assert(result)(isEmpty)
        },
        test("when multiple streams") {
          for {
            redis     <- ZIO.service[Redis]
            first     <- uuid
            second    <- uuid
            group     <- uuid
            consumer  <- uuid
            _         <- redis.xGroupCreate(first, group, "$", mkStream = true)
            _         <- redis.xGroupCreate(second, group, "$", mkStream = true)
            firstMsg  <- redis.xAdd(first, "*", "a" -> "b").returning[String]
            secondMsg <- redis.xAdd(second, "*", "a" -> "b").returning[String]
            result    <- redis.xReadGroup(group, consumer)(first -> ">", second -> ">").returning[String, String]
          } yield assert(result)(
            equalTo(
              Chunk(
                StreamChunk(first, Chunk(StreamEntry(firstMsg, Map("a" -> "b")))),
                StreamChunk(second, Chunk(StreamEntry(secondMsg, Map("a" -> "b"))))
              )
            )
          )
        },
        test("with positive count") {
          for {
            redis    <- ZIO.service[Redis]
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            first    <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _        <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            result   <- redis.xReadGroup(group, consumer, Some(1L))(stream -> ">").returning[String, String]
          } yield assert(result)(equalTo(Chunk(StreamChunk(stream, Chunk(StreamEntry(first, Map("a" -> "b")))))))
        },
        test("with zero count") {
          for {
            redis    <- ZIO.service[Redis]
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            first    <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            second   <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            result   <- redis.xReadGroup(group, consumer, Some(0L))(stream -> ">").returning[String, String]
          } yield assert(result)(
            equalTo(
              Chunk(
                StreamChunk(stream, Chunk(StreamEntry(first, Map("a" -> "b")), StreamEntry(second, Map("a" -> "b"))))
              )
            )
          )
        },
        test("with negative count") {
          for {
            redis    <- ZIO.service[Redis]
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            first    <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            second   <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            result   <- redis.xReadGroup(group, consumer, Some(-1L))(stream -> ">").returning[String, String]
          } yield assert(result)(
            equalTo(
              Chunk(
                StreamChunk(stream, Chunk(StreamEntry(first, Map("a" -> "b")), StreamEntry(second, Map("a" -> "b"))))
              )
            )
          )
        },
        test("with NOACK flag") {
          for {
            redis    <- ZIO.service[Redis]
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            _        <- redis.xReadGroup(group, consumer, noAck = true)(stream -> ">").returning[String, String]
            result   <- redis.xPending(stream, group)
          } yield assert(result.total)(equalTo(0L))
        },
        test("error when group doesn't exist") {
          for {
            redis    <- ZIO.service[Redis]
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            result   <- redis.xReadGroup(group, consumer)(stream -> ">").returning[String, String].either
          } yield assert(result)(isLeft(isSubtype[NoGroup](anything)))
        },
        test("error when invalid ID") {
          for {
            redis    <- ZIO.service[Redis]
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            result   <- redis.xReadGroup(group, consumer)(stream -> "invalid").returning[String, String].either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when not stream") {
          for {
            redis    <- ZIO.service[Redis]
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- redis.set(stream, "value")
            result   <- redis.xReadGroup(group, consumer)(stream -> ">").returning[String, String].either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("xRevRange")(
        test("with an unlimited start and an unlimited end") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            id     <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            result <- redis.xRevRange(stream, "+", "-").returning[String, String]
          } yield assert(result)(equalTo(Chunk(StreamEntry(id, Map("a" -> "b")))))
        },
        test("with the positive count") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            _      <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            second <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            result <- redis.xRevRange(stream, "+", "-", 1L).returning[String, String]
          } yield assert(result)(equalTo(Chunk(StreamEntry(second, Map("a" -> "b")))))
        },
        test("with the negative count") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            _      <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            result <- redis.xRevRange(stream, "+", "-", -1L).returning[String, String]
          } yield assert(result)(isEmpty)
        },
        test("with the zero count") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            _      <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _      <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            result <- redis.xRevRange(stream, "+", "-", 0L).returning[String, String]
          } yield assert(result)(isEmpty)
        },
        test("when stream doesn't exist") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            result <- redis.xRevRange(stream, "+", "-").returning[String, String]
          } yield assert(result)(isEmpty)
        },
        test("when start is greater than an end") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            result <- redis.xRevRange(stream, "-", "+").returning[String, String]
          } yield assert(result)(isEmpty)
        },
        test("error when invalid ID") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            result <- redis.xRevRange(stream, "invalid", "-").returning[String, String].either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when not stream") {
          for {
            redis     <- ZIO.service[Redis]
            nonStream <- uuid
            _         <- redis.set(nonStream, "value")
            result    <- redis.xRevRange(nonStream, "+", "-").returning[String, String].either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("xTrim")(
        test("an empty stream") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            result <- redis.xTrim(stream, 1000L)
          } yield assert(result)(equalTo(0L))
        },
        test("a non-empty stream") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            _      <- redis.xAdd(stream, "*", "a" -> "b").returning[String].repeatN(3)
            result <- redis.xTrim(stream, 2L)
          } yield assert(result)(equalTo(2L))
        },
        test("a non-empty stream with an approximate") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            _      <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            result <- redis.xTrim(stream, 1000L, approximate = true)
          } yield assert(result)(equalTo(0L))
        },
        test("error when negative count") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            _      <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            result <- redis.xTrim(stream, -1000L).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when not stream") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            _      <- redis.set(stream, "value")
            result <- redis.xTrim(stream, 1000L).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("xInfoStream")(
        test("an existing stream") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            _      <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            id     <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            result <- redis.xInfoStream(stream).returning[String, String, String]
          } yield assert(result.lastEntry.map(_.id))(isSome(equalTo(id)))
        },
        test("error when no such key") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            result <- redis.xInfoStream(stream).returning[String, String, String].either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when not a stream") {
          for {
            redis     <- ZIO.service[Redis]
            nonStream <- uuid
            _         <- redis.set(nonStream, "helloworld")
            result    <- redis.xInfoStream(nonStream).returning[String, String, String].either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("xInfoGroups")(
        test("of an existing stream") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            _      <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            _      <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            result <- redis.xInfoGroups[String](stream)
          } yield assert(result.toList.head.name)(equalTo(group))
        },
        test("error when no such key") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            result <- redis.xInfoGroups[String](stream).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when not a stream") {
          for {
            redis     <- ZIO.service[Redis]
            nonStream <- uuid
            _         <- redis.set(nonStream, "helloworld")
            result    <- redis.xInfoGroups[String](nonStream).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("xInfoConsumers")(
        test("of an existing stream") {
          for {
            redis    <- ZIO.service[Redis]
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            _        <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _        <- redis.xReadGroup(group, consumer)(stream -> ">").returning[String, String]
            result   <- redis.xInfoConsumers(stream, group)
          } yield assert(result.toList.head.name)(equalTo(consumer))
        },
        test("error when no such key") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            group  <- uuid
            result <- redis.xInfoConsumers(stream, group).either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when not a stream") {
          for {
            redis     <- ZIO.service[Redis]
            nonStream <- uuid
            group     <- uuid
            _         <- redis.set(nonStream, "helloworld")
            result    <- redis.xInfoConsumers(nonStream, group).either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("xInfoStreamFull")(
        test("of an existing stream") {
          for {
            redis    <- ZIO.service[Redis]
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- redis.xGroupCreate(stream, group, "$", mkStream = true)
            id       <- redis.xAdd(stream, "*", "a" -> "b").returning[String]
            _        <- redis.xReadGroup(group, consumer)(stream -> ">").returning[String, String]
            result   <- redis.xInfoStreamFull(stream).returning[String, String, String]
          } yield assert {
            val entries       = result.entries
            val length        = result.length
            val consumersName = result.groups.head.consumers.head.name
            val name          = result.groups.head.name
            (entries, length, consumersName, name)
          }(equalTo(Tuple4(Chunk.apply(StreamEntry(id, Map("a" -> "b"))), 1L, consumer, group)))
        },
        test("error when no such key") {
          for {
            redis  <- ZIO.service[Redis]
            stream <- uuid
            result <- redis.xInfoStreamFull(stream).returning[String, String, String].either
          } yield assert(result)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when not a stream") {
          for {
            redis     <- ZIO.service[Redis]
            nonStream <- uuid
            _         <- redis.set(nonStream, "helloworld")
            result    <- redis.xInfoStreamFull(nonStream).returning[String, String, String].either
          } yield assert(result)(isLeft(isSubtype[WrongType](anything)))
        }
      )
    )
}
