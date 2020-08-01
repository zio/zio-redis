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
      ),
      suite("bitCount")(
        testM("over non-empty string") {
          for {
            key   <- uuid
            _     <- set(key, "value", None, None, None)
            count <- bitCount(key, None)
          } yield assert(count)(equalTo(21L))
        },
        testM("over empty string") {
          for {
            key   <- uuid
            count <- bitCount(key, None)
          } yield assert(count)(equalTo(0L))
        },
        testM("error when not string") {
          for {
            key   <- uuid
            _     <- sAdd(key)("a")
            count <- bitCount(key, None).either
          } yield assert(count)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("over non-empty string with range") {
          for {
            key   <- uuid
            _     <- set(key, "value", None, None, None)
            count <- bitCount(key, Some(1 to 3))
          } yield assert(count)(equalTo(12L))
        },
        testM("over non-empty string with range that is too large") {
          for {
            key   <- uuid
            _     <- set(key, "value", None, None, None)
            count <- bitCount(key, Some(1 to 20))
          } yield assert(count)(equalTo(16L))
        },
        testM("over non-empty string with range that ends with the string") {
          for {
            key   <- uuid
            _     <- set(key, "value", None, None, None)
            count <- bitCount(key, Some(1 to -1))
          } yield assert(count)(equalTo(16L))
        },
        testM("over non-empty string with range whose start is bigger then end") {
          for {
            key   <- uuid
            _     <- set(key, "value", None, None, None)
            count <- bitCount(key, Some(3 to 1))
          } yield assert(count)(equalTo(0L))
        },
        testM("over empty string with range") {
          for {
            key   <- uuid
            count <- bitCount(key, Some(1 to 3))
          } yield assert(count)(equalTo(0L))
        },
        testM("over not string with range") {
          for {
            key   <- uuid
            _     <- sAdd(key)("a")
            count <- bitCount(key, Some(1 to 3)).either
          } yield assert(count)(isLeft(isSubtype[WrongType](anything)))
        }
      )
    )
}
