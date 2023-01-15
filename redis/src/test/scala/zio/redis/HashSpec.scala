package zio.redis

import zio.{Chunk, ZIO}
import zio.test.Assertion._
import zio.test._

trait HashSpec extends BaseSpec {
  def hashSuite: Spec[Redis, RedisError] =
    suite("hash")(
      suite("hSet, hGet, hGetAll and hDel")(
        test("set followed by get") {
          for {
            redis         <- ZIO.service[Redis]
            hash   <- uuid
            field  <- uuid
            value  <- uuid
            _      <- redis.hSet(hash, field -> value)
            result <- redis.hGet(hash, field).returning[String]
          } yield assert(result)(isSome(equalTo(value)))
        },
        test("set multiple fields for hash") {
          for {
            redis         <- ZIO.service[Redis]
            hash   <- uuid
            field1 <- uuid
            field2 <- uuid
            value  <- uuid
            result <- redis.hSet(hash, field1 -> value, field2 -> value)
          } yield assert(result)(equalTo(2L))
        },
        test("get all fields for hash") {
          for {
            redis         <- ZIO.service[Redis]
            hash   <- uuid
            field1 <- uuid
            field2 <- uuid
            value  <- uuid
            _      <- redis.hSet(hash, field1 -> value, field2 -> value)
            result <- redis.hGetAll(hash).returning[String, String]
          } yield assert(Chunk.fromIterable(result.values))(hasSameElements(Chunk(value, value)))
        },
        test("delete field for hash") {
          for {
            redis         <- ZIO.service[Redis]
            hash    <- uuid
            field   <- uuid
            value   <- uuid
            _       <- redis.hSet(hash, field -> value)
            deleted <- redis.hDel(hash, field)
            result  <- redis.hGet(hash, field).returning[String]
          } yield assert(deleted)(equalTo(1L)) && assert(result)(isNone)
        },
        test("delete multiple fields for hash") {
          for {
            redis         <- ZIO.service[Redis]
            hash    <- uuid
            field1  <- uuid
            field2  <- uuid
            value   <- uuid
            _       <- redis.hSet(hash, field1 -> value, field2 -> value)
            deleted <- redis.hDel(hash, field1, field2)
          } yield assert(deleted)(equalTo(2L))
        }
      ),
      suite("hmSet and hmGet")(
        test("set followed by get") {
          for {
            redis         <- ZIO.service[Redis]
            hash   <- uuid
            field  <- uuid
            value  <- uuid
            _      <- redis.hmSet(hash, field -> value)
            result <- redis.hmGet(hash, field).returning[String]
          } yield assert(result)(hasSameElements(Chunk(Some(value))))
        },
        test("set multiple fields for hash") {
          for {
            redis         <- ZIO.service[Redis]
            hash    <- uuid
            field1  <- uuid
            field2  <- uuid
            value   <- uuid
            _       <- redis.hmSet(hash, field1 -> value, field2 -> value)
            result1 <- redis.hmGet(hash, field1).returning[String]
            result2 <- redis.hmGet(hash, field2).returning[String]
          } yield assert(result1)(hasSameElements(Chunk(Some(value)))) &&
            assert(result2)(hasSameElements(Chunk(Some(value))))
        },
        test("get multiple fields for hash") {
          for {
            redis         <- ZIO.service[Redis]
            hash   <- uuid
            field1 <- uuid
            field2 <- uuid
            value1 <- uuid
            value2 <- uuid
            _      <- redis.hmSet(hash, field1 -> value1, field2 -> value2)
            result <- redis.hmGet(hash, field1, field2).returning[String]
          } yield assert(result)(hasSameElements(Chunk(Some(value1), Some(value2))))
        },
        test("delete field for hash") {
          for {
            redis         <- ZIO.service[Redis]
            hash    <- uuid
            field   <- uuid
            value   <- uuid
            _       <- redis.hmSet(hash, field -> value)
            deleted <- redis.hDel(hash, field)
            result  <- redis.hmGet(hash, field).returning[String]
          } yield assert(deleted)(equalTo(1L)) && assert(result)(hasSameElements(Chunk(None)))
        },
        test("delete multiple fields for hash") {
          for {
            redis         <- ZIO.service[Redis]
            hash    <- uuid
            field1  <- uuid
            field2  <- uuid
            field3  <- uuid
            value   <- uuid
            _       <- redis.hmSet(hash, field1 -> value, field2 -> value, field3 -> value)
            deleted <- redis.hDel(hash, field1, field3)
            result  <- redis.hmGet(hash, field1, field2, field3).returning[String]
          } yield assert(deleted)(equalTo(2L)) &&
            assert(result)(hasSameElements(Chunk(None, Some(value), None)))
        }
      ),
      suite("hExists")(
        test("field should exist") {
          for {
            redis         <- ZIO.service[Redis]
            hash   <- uuid
            field  <- uuid
            value  <- uuid
            _      <- redis.hSet(hash, field -> value)
            result <- redis.hExists(hash, field)
          } yield assert(result)(isTrue)
        },
        test("field shouldn't exist") {
          for {
            redis         <- ZIO.service[Redis]
            hash   <- uuid
            field  <- uuid
            result <- redis.hExists(hash, field)
          } yield assert(result)(isFalse)
        }
      ),
      suite("hIncrBy and hIncrByFloat")(
        test("existing field should be incremented by 1") {
          for {
            redis         <- ZIO.service[Redis]
            hash   <- uuid
            field  <- uuid
            _      <- redis.hSet(hash, field -> "1")
            result <- redis.hIncrBy(hash, field, 1L)
          } yield assert(result)(equalTo(2L))
        },
        test("incrementing value of non-existing hash and filed should create them") {
          for {
            redis         <- ZIO.service[Redis]
            hash   <- uuid
            field  <- uuid
            result <- redis.hIncrBy(hash, field, 1L)
          } yield assert(result)(equalTo(1L))
        },
        test("existing field should be incremented by 1.5") {
          for {
            redis         <- ZIO.service[Redis]
            hash   <- uuid
            field  <- uuid
            _      <- redis.hSet(hash, field -> "1")
            result <- redis.hIncrByFloat(hash, field, 1.5)
          } yield assert(result)(equalTo(2.5))
        },
        test("incrementing value of float for non-existing hash and field should create them") {
          for {
            redis         <- ZIO.service[Redis]
            hash   <- uuid
            field  <- uuid
            result <- redis.hIncrByFloat(hash, field, 1.5)
          } yield assert(result)(equalTo(1.5))
        },
        test("incrementing value of float for non-existing hash and field with negative value") {
          for {
            redis         <- ZIO.service[Redis]
            hash   <- uuid
            field  <- uuid
            result <- redis.hIncrByFloat(hash, field, -1.5)
          } yield assert(result)(equalTo(-1.5))
        }
      ),
      suite("hKeys and hLen")(
        test("get field names for existing hash") {
          for {
            redis         <- ZIO.service[Redis]
            hash   <- uuid
            field  <- uuid
            value  <- uuid
            _      <- redis.hSet(hash, field -> value)
            result <- redis.hKeys(hash).returning[String]
          } yield assert(result)(hasSameElements(Chunk(field)))
        },
        test("get field names for non-existing hash") {
          for {
            redis         <- ZIO.service[Redis]
            hash   <- uuid
            result <- redis.hKeys(hash).returning[String]
          } yield assert(result)(isEmpty)
        },
        test("get field count for existing hash") {
          for {
            redis         <- ZIO.service[Redis]
            hash   <- uuid
            field  <- uuid
            value  <- uuid
            _      <- redis.hSet(hash, field -> value)
            result <- redis.hLen(hash)
          } yield assert(result)(equalTo(1L))
        },
        test("get field count for non-existing hash") {
          for {
            redis         <- ZIO.service[Redis]
            hash   <- uuid
            result <- redis.hLen(hash)
          } yield assert(result)(equalTo(0L))
        }
      ),
      suite("hSetNx")(
        test("set value for non-existing field") {
          for {
            redis         <- ZIO.service[Redis]
            hash   <- uuid
            field  <- uuid
            value  <- uuid
            result <- redis.hSetNx(hash, field, value)
          } yield assert(result)(isTrue)
        },
        test("set value for existing field") {
          for {
            redis         <- ZIO.service[Redis]
            hash   <- uuid
            field  <- uuid
            value  <- uuid
            _      <- redis.hSet(hash, field -> value)
            result <- redis.hSetNx(hash, field, value)
          } yield assert(result)(isFalse)
        }
      ),
      suite("hStrLen")(
        test("get value length for existing field") {
          for {
            redis         <- ZIO.service[Redis]
            hash   <- uuid
            field  <- uuid
            value  <- uuid
            _      <- redis.hSet(hash, field -> value)
            result <- redis.hStrLen(hash, field)
          } yield assert(result)(equalTo(value.length.toLong))
        },
        test("get value length for non-existing field") {
          for {
            redis         <- ZIO.service[Redis]
            hash   <- uuid
            field  <- uuid
            result <- redis.hStrLen(hash, field)
          } yield assert(result)(equalTo(0L))
        }
      ),
      suite("hVals")(
        test("get all values from existing hash") {
          for {
            redis         <- ZIO.service[Redis]
            hash   <- uuid
            field  <- uuid
            value  <- uuid
            _      <- redis.hSet(hash, field -> value)
            result <- redis.hVals(hash).returning[String]
          } yield assert(result)(hasSameElements(Chunk(value)))
        },
        test("get all values from non-existing hash") {
          for {
            redis         <- ZIO.service[Redis]
            hash   <- uuid
            result <- redis.hVals(hash).returning[String]
          } yield assert(result)(isEmpty)
        }
      ),
      suite("hScan")(
        test("hScan entries with match and count options")(check(genPatternOption, genCountOption) { (pattern, count) =>
          for {
            redis         <- ZIO.service[Redis]
            hash            <- uuid
            field           <- uuid
            value           <- uuid
            _               <- redis.hSet(hash, field -> value)
            scan            <- redis.hScan(hash, 0L, pattern, count).returning[String, String]
            (next, elements) = scan
          } yield assert(next)(isGreaterThanEqualTo(0L)) && assert(elements)(isNonEmpty)
        })
      ),
      suite("hRandField")(
        test("randomly select one field") {
          for {
            redis         <- ZIO.service[Redis]
            hash   <- uuid
            field1 <- uuid
            value1 <- uuid
            field2 <- uuid
            value2 <- uuid
            _      <- redis.hSet(hash, field1 -> value1, field2 -> value2)
            field  <- redis.hRandField(hash).returning[String]
          } yield assert(Seq(field1, field2))(contains(field.get))
        },
        test("returns None if key does not exists") {
          for {
            redis         <- ZIO.service[Redis]
            hash    <- uuid
            field   <- uuid
            value   <- uuid
            badHash <- uuid
            _       <- redis.hSet(hash, field -> value)
            field   <- redis.hRandField(badHash).returning[String]
          } yield assert(field)(isNone)
        },
        test("returns n different fields if count is provided") {
          for {
            redis         <- ZIO.service[Redis]
            hash   <- uuid
            field1 <- uuid
            value1 <- uuid
            field2 <- uuid
            value2 <- uuid
            _      <- redis.hSet(hash, field1 -> value1, field2 -> value2)
            fields <- redis.hRandField(hash, 2).returning[String]
          } yield assert(fields)(hasSize(equalTo(2)))
        },
        test("returns all hash fields if count is provided and is greater or equal than hash size") {
          for {
            redis         <- ZIO.service[Redis]
            hash   <- uuid
            field1 <- uuid
            value1 <- uuid
            field2 <- uuid
            value2 <- uuid
            _      <- redis.hSet(hash, field1 -> value1, field2 -> value2)
            fields <- redis.hRandField(hash, 4).returning[String]
          } yield assert(fields)(hasSize(equalTo(2)))
        },
        test("returns repeated fields if count is negative") {
          for {
            redis         <- ZIO.service[Redis]
            hash   <- uuid
            field  <- uuid
            value  <- uuid
            _      <- redis.hSet(hash, field -> value)
            fields <- redis.hRandField(hash, -2).returning[String]
          } yield assert(fields)(hasSameElements(Chunk(field, field)))
        },
        test("returns n different fields and values with 'withvalues' option") {
          for {
            redis         <- ZIO.service[Redis]
            hash   <- uuid
            field1 <- uuid
            value1 <- uuid
            field2 <- uuid
            value2 <- uuid
            _      <- redis.hSet(hash, field1 -> value1, field2 -> value2)
            fields <- redis.hRandField(hash, 4, withValues = true).returning[String]
          } yield assert(fields)(hasSize(equalTo(4)))
        }
      )
    )
}
