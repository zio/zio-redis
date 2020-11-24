package zio.redis

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
            id       <- xAdd(stream, "*", "name" -> "Sara", "surname" -> "OConnor")
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
            first    <- xAdd(stream, "*", "name" -> "Sara", "surname" -> "OConnor")
            second   <- xAdd(stream, "*", "name" -> "Sara", "surname" -> "OConnor")
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
            id     <- xAdd(stream, "*", "name" -> "Sara", "surname" -> "OConnor")
            result <- xAck(stream, group, id)
          } yield assert(result)(equalTo(0L))
        },
        testM("when message with the given ID doesn't exist") {
          for {
            stream   <- uuid
            group    <- uuid
            consumer <- uuid
            _        <- xGroupCreate(stream, group, "$", mkStream = true)
            _        <- xAdd(stream, "*", "name" -> "Sara", "surname" -> "OConnor")
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
          } yield assert(result)(isLeft)
        }
      )
    )
}
