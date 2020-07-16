package zio.redis

import zio.{ Chunk, ZIO }
import zio.duration._
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect._

trait KeysSpec extends BaseSpec {

  val keysSuite =
    suite("keys")(
      testM("set followed by get") {
        for {
          key   <- uuid
          value <- uuid
          _     <- set(key, value, None, None, None)
          v     <- get(key)
        } yield assert(v)(isSome(equalTo(value)))
      },
      testM("get non-existing key") {
        for {
          key <- uuid
          v   <- get(key)
        } yield assert(v)(isNone)
      },
      testM("handles wrong types") {
        for {
          key <- uuid
          _   <- sAdd(key)("1", "2", "3")
          v   <- get(key).either
        } yield assert(v)(isLeft)
      },
      testM("check whether or not key exists") {
        for {
          key   <- uuid
          value <- uuid
          _     <- set(key, value, None, None, None)
          e1    <- exists(key, Nil)
          e2    <- exists("unknown", Nil)
        } yield assert(e1)(isTrue) && assert(e2)(isFalse)
      },
      testM("delete existing key") {
        for {
          key     <- uuid
          value   <- uuid
          _       <- set(key, value, None, None, None)
          deleted <- del(key, Nil)
        } yield assert(deleted)(equalTo(1L))
      },
      testM("find all keys matching pattern") {
        for {
          value    <- uuid
          key1      = "custom_key_1"
          key2      = "custom_key_2"
          _        <- set(key1, value, None, None, None)
          _        <- set(key2, value, None, None, None)
          response <- keys("*custom*")
        } yield assert(response)(hasSameElements(Chunk(key1, key2)))
      },
      testM("unlink existing key") {
        for {
          key     <- uuid
          value   <- uuid
          _       <- set(key, value, None, None, None)
          removed <- unlink(key, Nil)
        } yield assert(removed)(equalTo(1L))
      },
      testM("touch two existing keys") {
        for {
          key1    <- uuid
          value1  <- uuid
          _       <- set(key1, value1, None, None, None)
          key2    <- uuid
          value2  <- uuid
          _       <- set(key2, value2, None, None, None)
          touched <- touch(key1, List(key2))
        } yield assert(touched)(equalTo(2L))
      },
      testM("scan entries") {
        for {
          key             <- uuid
          value           <- uuid
          _               <- set(key, value, None, None, None)
          scan            <- scan(0L, None, None, None)
          (next, elements) = scan
        } yield assert(next)(isNonEmptyString) && assert(elements)(isNonEmpty)
      },
      testM("fetch random key") {
        for {
          key       <- uuid
          value     <- uuid
          _         <- set(key, value, None, None, None)
          allKeys   <- keys("*")
          randomKey <- randomKey()
        } yield assert(allKeys)(contains(randomKey.get))
      } @@ ignore,
      testM("dump followed by restore") {
        for {
          key      <- uuid
          value    <- uuid
          _        <- set(key, value, None, None, None)
          dumped   <- dump(key)
          _        <- del(key, Nil)
          restore  <- restore(key, 0L, dumped, None, None, None, None).either
          restored <- get(key)
        } yield assert(restore)(isRight) && assert(restored)(isSome(equalTo(value)))
      } @@ ignore,
      suite("ttl")(
        testM("check ttl for existing key") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value, None, None, None)
            _     <- pExpire(key, 1000.millis)
            ttl   <- ttl(key).either
          } yield assert(ttl)(isRight)
        },
        testM("check ttl for non-existing key") {
          for {
            key <- uuid
            ttl <- ttl(key).either
          } yield assert(ttl)(isLeft)
        },
        testM("check pTtl for existing key") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value, None, None, None)
            _     <- pExpire(key, 1000.millis)
            pTtl  <- pTtl(key).either
          } yield assert(pTtl)(isRight)
        },
        testM("check pTtl for non-existing key") {
          for {
            key  <- uuid
            pTtl <- pTtl(key).either
          } yield assert(pTtl)(isLeft)
        }
      ),
      suite("expiration")(
        testM("set key expiration with pExpire command") {
          for {
            key       <- uuid
            value     <- uuid
            _         <- set(key, value, None, None, None)
            exp       <- pExpire(key, 2000.millis)
            response1 <- exists(key, Nil)
            _         <- ZIO.sleep(2010.millis)
            response2 <- exists(key, Nil)
          } yield assert(exp)(isTrue) && assert(response1)(isTrue) && assert(response2)(isFalse)
        },
        testM("set key expiration with pExpireAt command") {
          for {
            key       <- uuid
            value     <- uuid
            expiresAt <- instantOf(2000)
            _         <- set(key, value, None, None, None)
            exp       <- pExpireAt(key, expiresAt)
            response1 <- exists(key, Nil)
            _         <- ZIO.sleep(2010.millis)
            response2 <- exists(key, Nil)
          } yield assert(exp)(isTrue) && assert(response1)(isTrue) && assert(response2)(isFalse)
        },
        testM("expire followed by persist") {
          for {
            key       <- uuid
            value     <- uuid
            _         <- set(key, value, None, None, None)
            exp       <- expire(key, 10000.millis)
            persisted <- persist(key)
          } yield assert(exp)(isTrue) && assert(persisted)(isTrue)
        },
        testM("set key expiration with expireAt command") {
          for {
            key       <- uuid
            value     <- uuid
            expiresAt <- instantOf(2000)
            _         <- set(key, value, None, None, None)
            exp       <- expireAt(key, expiresAt)
            response1 <- exists(key, Nil)
            _         <- ZIO.sleep(2010.millis)
            response2 <- exists(key, Nil)
          } yield assert(exp)(isTrue) && assert(response1)(isTrue) && assert(response2)(isFalse)
        } @@ eventually
      ),
      suite("renaming")(
        testM("rename existing key") {
          for {
            key     <- uuid
            newKey  <- uuid
            value   <- uuid
            _       <- set(key, value, None, None, None)
            renamed <- rename(key, newKey).either
            v       <- get(newKey)
          } yield assert(renamed)(isRight) && assert(v)(isSome(equalTo(value)))
        },
        testM("try to rename non-existing key") {
          for {
            key     <- uuid
            newKey  <- uuid
            renamed <- rename(key, newKey).either
          } yield assert(renamed)(isLeft)
        },
        testM("rename key to newkey if newkey does not yet exist") {
          for {
            key     <- uuid
            newKey  <- uuid
            value   <- uuid
            _       <- set(key, value, None, None, None)
            renamed <- renameNx(key, newKey)
            v       <- get(newKey)
          } yield assert(renamed)(isTrue) && assert(v)(isSome(equalTo(value)))
        },
        testM("try to rename non-existing key with renameNx command") {
          for {
            key     <- uuid
            newKey  <- uuid
            renamed <- renameNx(key, newKey).either
          } yield assert(renamed)(isLeft)
        }
      ),
      suite("types")(
        testM("string type") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- set(key, value, None, None, None)
            string <- typeOf(key)
          } yield assert(string)(equalTo(RedisType.String))
        },
        testM("list type") {
          for {
            key   <- uuid
            value <- uuid
            _     <- lPush(key)(value)
            list  <- typeOf(key)
          } yield assert(list)(equalTo(RedisType.List))
        },
        testM("set type") {
          for {
            key   <- uuid
            value <- uuid
            _     <- sAdd(key)(value)
            set   <- typeOf(key)
          } yield assert(set)(equalTo(RedisType.Set))
        },
        testM("sorted set type") {
          for {
            key   <- uuid
            value <- uuid
            _     <- zAdd(key, None, None, None, (MemberScore(1d, value), Nil))
            zset  <- typeOf(key)
          } yield assert(zset)(equalTo(RedisType.SortedSet))
        },
        testM("hash type") {
          for {
            key   <- uuid
            field <- uuid
            value <- uuid
            _     <- hSet(key)((field, value))
            hash  <- typeOf(key)
          } yield assert(hash)(equalTo(RedisType.Hash))
        }
      )
    )
}
