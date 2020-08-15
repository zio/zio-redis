package zio.redis

import zio.Chunk
import zio.test._
import zio.test.Assertion._

trait HashSpec extends BaseSpec {

  val hashSuite =
    suite("hash")(
      suite("hSet, hGet, hGetAll and hDel")(
        testM("set followed by get") {
          for {
            hash   <- uuid
            field  <- uuid
            value  <- uuid
            _      <- hSet(hash)((field, value))
            result <- hGet(hash, field)
          } yield assert(result)(isSome(equalTo(value)))
        },
        testM("set multiple fields for hash") {
          for {
            hash   <- uuid
            field1 <- uuid
            field2 <- uuid
            value  <- uuid
            result <- hSet(hash)((field1, value), (field2, value))
          } yield assert(result)(equalTo(2L))
        },
        testM("get all fields for hash") {
          for {
            hash   <- uuid
            field1 <- uuid
            field2 <- uuid
            value  <- uuid
            _      <- hSet(hash)((field1, value), (field2, value))
            result <- hGetAll(hash)
          } yield assert(Chunk.fromIterable(result.values))(hasSameElements(Chunk(value, value)))
        },
        testM("delete field for hash") {
          for {
            hash    <- uuid
            field   <- uuid
            value   <- uuid
            added   <- hSet(hash)((field, value))
            deleted <- hDel(hash)(field)
            result  <- hGet(hash, field)
          } yield assert(added)(equalTo(1L)) && assert(deleted)(equalTo(1L)) && assert(result)(isNone)
        },
        testM("delete multiple fields for hash") {
          for {
            hash    <- uuid
            field1  <- uuid
            field2  <- uuid
            value   <- uuid
            _       <- hSet(hash)((field1, value), (field2, value))
            deleted <- hDel(hash)(field1, field2)
          } yield assert(deleted)(equalTo(2L))
        }
      ),
      suite("hExists")(
        testM("field should exist") {
          for {
            hash   <- uuid
            field  <- uuid
            value  <- uuid
            _      <- hSet(hash)((field, value))
            result <- hExists(hash, field)
          } yield assert(result)(isTrue)
        },
        testM("field shouldn't exist") {
          for {
            hash   <- uuid
            field  <- uuid
            result <- hExists(hash, field)
          } yield assert(result)(isFalse)
        }
      ),
      suite("hIncrBy and hIncrByFloat")(
        testM("existing field should be incremented by 1") {
          for {
            hash   <- uuid
            field  <- uuid
            _      <- hSet(hash)((field, "1"))
            result <- hIncrBy(hash, field, 1L)
          } yield assert(result)(equalTo(2L))
        },
        testM("incrementing value of non-existing hash and filed should create them") {
          for {
            hash   <- uuid
            field  <- uuid
            result <- hIncrBy(hash, field, 1L)
          } yield assert(result)(equalTo(1L))
        },
        testM("existing field should be incremented by 1.5") {
          for {
            hash   <- uuid
            field  <- uuid
            _      <- hSet(hash)((field, "1"))
            result <- hIncrByFloat(hash, field, 1.5)
          } yield assert(result)(equalTo(2.5))
        },
        testM("incrementing value of float for non-existing hash and field should create them") {
          for {
            hash   <- uuid
            field  <- uuid
            result <- hIncrByFloat(hash, field, 1.5)
          } yield assert(result)(equalTo(1.5))
        },
        testM("incrementing value of float for non-existing hash and field with negative value") {
          for {
            hash   <- uuid
            field  <- uuid
            result <- hIncrByFloat(hash, field, -1.5)
          } yield assert(result)(equalTo(-1.5))
        }
      ),
      suite("hKeys and hLen")(
        testM("get field names for existing hash") {
          for {
            hash   <- uuid
            field  <- uuid
            value  <- uuid
            _      <- hSet(hash)((field, value))
            result <- hKeys(hash)
          } yield assert(result)(hasSameElements(Chunk(field)))
        },
        testM("get field names for non-existing hash") {
          for {
            hash   <- uuid
            result <- hKeys(hash)
          } yield assert(result)(isEmpty)
        },
        testM("get field count for existing hash") {
          for {
            hash   <- uuid
            field  <- uuid
            value  <- uuid
            _      <- hSet(hash)((field, value))
            result <- hLen(hash)
          } yield assert(result)(equalTo(1L))
        },
        testM("get field count for non-existing hash") {
          for {
            hash   <- uuid
            result <- hLen(hash)
          } yield assert(result)(equalTo(0L))
        }
      ),
      suite("hSetNx")(
        testM("set value for non-existing field") {
          for {
            hash   <- uuid
            field  <- uuid
            value  <- uuid
            result <- hSetNx(hash, field, value)
          } yield assert(result)(isTrue)
        },
        testM("set value for existing field") {
          for {
            hash   <- uuid
            field  <- uuid
            value  <- uuid
            _      <- hSet(hash)((field, value))
            result <- hSetNx(hash, field, value)
          } yield assert(result)(isFalse)
        }
      ),
      suite("hStrLen")(
        testM("get value length for existing field") {
          for {
            hash   <- uuid
            field  <- uuid
            value  <- uuid
            _      <- hSet(hash)((field, value))
            result <- hStrLen(hash, field)
          } yield assert(result)(equalTo(value.length.toLong))
        },
        testM("get value length for non-existing field") {
          for {
            hash   <- uuid
            field  <- uuid
            result <- hStrLen(hash, field)
          } yield assert(result)(equalTo(0L))
        }
      ),
      suite("hVals")(
        testM("get all values from existing hash") {
          for {
            hash   <- uuid
            field  <- uuid
            value  <- uuid
            _      <- hSet(hash)((field, value))
            result <- hVals(hash)
          } yield assert(result)(hasSameElements(Chunk(value)))
        },
        testM("get all values from non-existing hash") {
          for {
            hash   <- uuid
            result <- hVals(hash)
          } yield assert(result)(isEmpty)
        }
      )
    )
}
