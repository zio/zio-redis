package zio.redis

import zio.redis.RedisError.WrongType
import zio.test._
import zio.test.Assertion._

trait StringsSpec extends BaseSpec {
  val stringsSuite =
    suite("strings")(
      suite("append")(
        testM("to the end of non-empty string") {
          for {
            key <- uuid
            _   <- set(key, "val", None, None, None)
            len <- append(key, "ue")
          } yield assert(len)(equalTo(5L))
        },
        testM("to the end of empty string") {
          for {
            key <- uuid
            len <- append(key, "value")
          } yield assert(len)(equalTo(5L))
        },
        testM("empty value to the end of non-empty string") {
          for {
            key <- uuid
            _   <- set(key, "value", None, None, None)
            len <- append(key, "")
          } yield assert(len)(equalTo(5L))
        },
        testM("empty value to the end of empty string") {
          for {
            key <- uuid
            len <- append(key, "")
          } yield assert(len)(equalTo(0L))
        },
        testM("error when not string") {
          for {
            key <- uuid
            _   <- sAdd(key)("a")
            len <- append(key, "b").either
          } yield assert(len)(isLeft(isSubtype[WrongType](anything)))
        }
      )
    )
}
