package zio.redis

import zio.duration._
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._
import zio.{Chunk, ZIO}
import java.time.{Duration => JavaDuration}
import SecondRedisExecutorLayer._
import zio.redis.Output.OptionalOutput
import zio.redis.Output.MultiStringOutput
// import zio.redis.RedisError.ProtocolError
//import zio.Has

trait KeysSpec extends BaseSpec {

  val keysSuite =
    suite("keys")(
      testM("set followed by get") {
        for {
          key   <- uuid
          value <- uuid
          _     <- set(key, value)
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
          _   <- sAdd(key, "1", "2", "3")
          v   <- get(key).either
        } yield assert(v)(isLeft)
      },
      testM("check whether or not key exists") {
        for {
          key   <- uuid
          value <- uuid
          _     <- set(key, value)
          e1    <- exists(key)
          e2    <- exists("unknown")
        } yield assert(e1)(isTrue) && assert(e2)(isFalse)
      },
      testM("delete existing key") {
        for {
          key     <- uuid
          value   <- uuid
          _       <- set(key, value)
          deleted <- del(key)
        } yield assert(deleted)(equalTo(1L))
      },
      testM("find all keys matching pattern") {
        for {
          value    <- uuid
          key1      = "custom_key_1"
          key2      = "custom_key_2"
          _        <- set(key1, value)
          _        <- set(key2, value)
          response <- keys("*custom*")
        } yield assert(response)(hasSameElements(Chunk(key1, key2)))
      },
      testM("unlink existing key") {
        for {
          key     <- uuid
          value   <- uuid
          _       <- set(key, value)
          removed <- unlink(key)
        } yield assert(removed)(equalTo(1L))
      },
      testM("touch two existing keys") {
        for {
          key1    <- uuid
          value1  <- uuid
          _       <- set(key1, value1)
          key2    <- uuid
          value2  <- uuid
          _       <- set(key2, value2)
          touched <- touch(key1, key2)
        } yield assert(touched)(equalTo(2L))
      },
      testM("scan entries") {
        for {
          key             <- uuid
          value           <- uuid
          _               <- set(key, value)
          scan            <- scan(0L)
          (next, elements) = scan
        } yield assert(next)(isGreaterThan(0L)) && assert(elements)(isNonEmpty)
      },
      testM("fetch random key") {
        for {
          key       <- uuid
          value     <- uuid
          _         <- set(key, value)
          allKeys   <- keys("*")
          randomKey <- randomKey()
        } yield assert(allKeys)(contains(randomKey.get))
      } @@ ignore,
      testM("dump followed by restore") {
        for {
          key      <- uuid
          value    <- uuid
          _        <- set(key, value)
          dumped   <- dump(key)
          _        <- del(key)
          restore  <- restore(key, 0L, dumped).either
          restored <- get(key)
        } yield assert(restore)(isRight) && assert(restored)(isSome(equalTo(value)))
      },
      suite("migrate")(
        testM("migrate key to another redis server (copy and replace)") {
          for {
            key       <- uuid
            value     <- uuid
            _         <- set(key, value)
            response  <- migrate("redis1",
                                  6379,
                                  key,
                                  0L,
                                  JavaDuration.ofMillis(5000),
                                  copy = Option(Copy),
                                  replace = Option(Replace),
                                  keys = None)
            out       <- ZIO.access[SecondRedisExecutor](_.get.execute(Input.StringInput.encode("GET") ++ Input.StringInput.encode(key)))
            value2    <- out.map(respValue => OptionalOutput(MultiStringOutput).unsafeDecode(respValue))
            value3    <- get(key)
          } yield assert(response)(equalTo("OK")) &&
              assert(value2)(isSome(equalTo(value))) &&
              assert(value3)(isSome(equalTo(value)))
        },
        testM("migrate key to another redis server (move and replace)") {
          for {
            key       <- uuid
            value     <- uuid
            _         <- set(key, value)
            response  <- migrate("redis1",
                                  6379,
                                  key,
                                  0L,
                                  JavaDuration.ofMillis(5000),
                                  copy = None,
                                  replace = Option(Replace),
                                  keys = None)
            out       <- ZIO.access[SecondRedisExecutor](_.get.execute(Input.StringInput.encode("GET") ++ Input.StringInput.encode(key)))
            value2    <- out.map(respValue => OptionalOutput(MultiStringOutput).unsafeDecode(respValue))
            value3    <- get(key)
          } yield assert(response)(equalTo("OK")) &&
              assert(value2)(isSome(equalTo(value))) &&
              assert(value3)(isNone)
        },
      ),
      suite("ttl")(
        testM("check ttl for existing key") {
          for {
            key   <- uuid
            value <- uuid
            _     <- pSetEx(key, 1000.millis, value)
            ttl   <- ttl(key).either
          } yield assert(ttl)(isRight)
        } @@ eventually,
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
            _     <- pSetEx(key, 1000.millis, value)
            pTtl  <- pTtl(key).either
          } yield assert(pTtl)(isRight)
        } @@ eventually,
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
            _         <- set(key, value)
            exp       <- pExpire(key, 2000.millis)
            response1 <- exists(key)
            _         <- ZIO.sleep(2050.millis)
            response2 <- exists(key)
          } yield assert(exp)(isTrue) && assert(response1)(isTrue) && assert(response2)(isFalse)
        } @@ eventually,
        testM("set key expiration with pExpireAt command") {
          for {
            key       <- uuid
            value     <- uuid
            expiresAt <- instantOf(2000)
            _         <- set(key, value)
            exp       <- pExpireAt(key, expiresAt)
            response1 <- exists(key)
            _         <- ZIO.sleep(2050.millis)
            response2 <- exists(key)
          } yield assert(exp)(isTrue) && assert(response1)(isTrue) && assert(response2)(isFalse)
        } @@ eventually,
        testM("expire followed by persist") {
          for {
            key       <- uuid
            value     <- uuid
            _         <- set(key, value)
            exp       <- expire(key, 10000.millis)
            persisted <- persist(key)
          } yield assert(exp)(isTrue) && assert(persisted)(isTrue)
        },
        testM("set key expiration with expireAt command") {
          for {
            key       <- uuid
            value     <- uuid
            expiresAt <- instantOf(2000)
            _         <- set(key, value)
            exp       <- expireAt(key, expiresAt)
            response1 <- exists(key)
            _         <- ZIO.sleep(2050.millis)
            response2 <- exists(key)
          } yield assert(exp)(isTrue) && assert(response1)(isTrue) && assert(response2)(isFalse)
        } @@ eventually
      ),
      suite("renaming")(
        testM("rename existing key") {
          for {
            key     <- uuid
            newKey  <- uuid
            value   <- uuid
            _       <- set(key, value)
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
            _       <- set(key, value)
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
            _      <- set(key, value)
            string <- typeOf(key)
          } yield assert(string)(equalTo(RedisType.String))
        },
        testM("list type") {
          for {
            key   <- uuid
            value <- uuid
            _     <- lPush(key, value)
            list  <- typeOf(key)
          } yield assert(list)(equalTo(RedisType.List))
        },
        testM("set type") {
          for {
            key   <- uuid
            value <- uuid
            _     <- sAdd(key, value)
            set   <- typeOf(key)
          } yield assert(set)(equalTo(RedisType.Set))
        },
        testM("sorted set type") {
          for {
            key   <- uuid
            value <- uuid
            _     <- zAdd(key)(MemberScore(1d, value))
            zset  <- typeOf(key)
          } yield assert(zset)(equalTo(RedisType.SortedSet))
        },
        testM("hash type") {
          for {
            key   <- uuid
            field <- uuid
            value <- uuid
            _     <- hSet(key, (field, value))
            hash  <- typeOf(key)
          } yield assert(hash)(equalTo(RedisType.Hash))
        }
      )
    )
}
