package zio.redis

import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit

import zio.clock.Clock
import zio.clock.currentTime
import zio.{Chunk, UIO, ZIO}
import zio.duration._
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test.environment.TestClock

object ApiSpec extends BaseSpec {

  def spec =
    suite("Redis commands")(
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
          }
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
      ),
      suite("lists")(
        testM("lIndex first element") {
          for {
            key   <- uuid
            _     <- lPush(key)("world", "hello")
            index <- lIndex(key, 0l)
          } yield assert(index)(isSome(equalTo("hello")))
        },
        testM("lIndex last element") {
          for {
            key    <- uuid
            _      <- lPush(key)("world", "hello")
            index  <- lIndex(key, -1l)
          } yield assert(index)(isSome(equalTo("world")))
        },
        testM("lIndex no existing element") {
          for {
            key   <- uuid
            _     <- lPush(key)("world", "hello")
            index <- lIndex(key, 3)
          } yield assert(index)(isNone)
        },
        testM("lIndex error when not list") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value, None, None, None)
            index <- lIndex(key, -1l).either
          } yield assert(index)(isLeft)
        },
        testM("lLen two") {
          for {
            key   <- uuid
            _     <- lPush(key)("world", "hello")
            len   <- lLen(key)
          } yield assert(len)(equalTo(2l))
        },
        testM("lLen 0 when no key") {
          for {
            len   <- lLen("unknown")
          } yield assert(len)(equalTo(0l))
        },
        testM("lLen error when not list") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value, None, None, None)
            index <- lLen(key).either
          } yield assert(index)(isLeft)
        },
        testM("lPop element") {
          for {
            key    <- uuid
            _      <- lPush(key)("world", "hello")
            popped <- lPop(key)
          } yield assert(popped)(isSome(equalTo("hello")))
        },
        testM("lPop no element") {
          for {
            popped <- lPop("unknown")
          } yield assert(popped)(isNone)
        },
        testM("lPop error not list") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value, None, None, None)
            pop   <- lPop(key).either
          } yield assert(pop)(isLeft)
        },
        testM("lPush element") {
          for {
            key   <- uuid
            push  <- lPush(key)("hello")
          } yield assert(push)(equalTo(1l))
        },
        testM("lPush error when not list") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value, None, None, None)
            push  <- lPush(key)("hello").either
          } yield assert(push)(isLeft)
        },
        testM("lPushX element") {
          for {
            key   <- uuid
            _     <- lPush(key)("world")
            px    <- lPushX(key)("hello")
          } yield assert(px)(equalTo(2l))
        },
        testM("lPushX nothing when key doesn't exist") {
          for {
            key   <- uuid
            px    <- lPushX(key)("world")
          } yield assert(px)(equalTo(0l))
        },
        testM("lPushX error when not list") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value, None, None, None)
            push  <- lPushX(key)("hello").either
          } yield assert(push)(isLeft)
        },
        testM("lRange two elements") {
          for {
            key   <- uuid
            _     <- lPush(key)("world", "hello")
            range <- lRange(key, Range(0,1))
          } yield assert(range)(equalTo(Chunk("hello", "world")))
        },
        testM("lRange two elements negative indices") {
          for {
            key   <- uuid
            _     <- lPush(key)("world", "hello")
            range <- lRange(key, Range(-2,-1))
          } yield assert(range)(equalTo(Chunk("hello", "world")))
        },
        testM("lRange start invalid") {
          for {
            key   <- uuid
            _     <- lPush(key)("world", "hello")
            range <- lRange(key, Range(2, 3))
          } yield assert(range)(equalTo(Chunk()))
        },
        testM("lRange end invalid") {
          for {
            key   <- uuid
            _     <- lPush(key)("world", "hello")
            range <- lRange(key, Range(1, 2))
          } yield assert(range)(equalTo(Chunk("world")))
        },
        testM("lRange error when not list") {
          for {
            key   <- uuid
            _     <- set(key, "hello", None, None, None)
            range <- lRange(key, Range(1, 2)).either
          } yield assert(range)(isLeft)
        },
        testM("lRem 2 elements moving from head") {
          for {
            key     <- uuid
            _       <- lPush(key)("world", "hello", "hello", "hello")
            removed <- lRem(key, 2, "hello")
            range   <- lRange(key, Range(0,1))
          } yield assert(removed)(equalTo(2l)) && assert(range)(equalTo(Chunk("hello", "world")))
        },
        testM("lRem 2 elements moving from tail") {
          for {
            key     <- uuid
            _       <- lPush(key)("hello", "hello", "world", "hello")
            removed <- lRem(key, -2, "hello")
            range   <- lRange(key, Range(0,1))
          } yield assert(removed)(equalTo(2l)) && assert(range)(equalTo(Chunk("hello", "world")))
        },
        testM("lRem all 3 'hello' elements") {
          for {
            key     <- uuid
            _       <- lPush(key)("hello", "hello", "world", "hello")
            removed <- lRem(key, 0, "hello")
            range   <- lRange(key, Range(0,1))
          } yield assert(removed)(equalTo(3l)) && assert(range)(equalTo(Chunk("world")))
        },
        testM("lRem nothing when key does not exist") {
          for {
            key     <- uuid
            _       <- lPush(key)("world", "hello")
            removed <- lRem(key, 0, "goodbye")
            range   <- lRange(key, Range(0,1))
          } yield assert(removed)(equalTo(0l)) && assert(range)(equalTo(Chunk("hello", "world")))
        },
        testM("lRem error when not list") {
          for {
            key     <- uuid
            _       <- set(key, "hello", None, None, None)
            removed <- lRem(key, 0, "hello").either
          } yield assert(removed)(isLeft)
        },
        testM("lSet element") {
          for {
            key     <- uuid
            _       <- lPush(key)("world", "hello")
            _       <- lSet(key, 1, "goodbye")
            range   <- lRange(key, Range(0,1))
          } yield assert(range)(equalTo(Chunk("hello", "goodbye")))
        },
        testM("lSet error when out of range index") {
          for {
            key     <- uuid
            _       <- lPush(key)("world", "hello")
            set     <- lSet(key, 2, "goodbye").either
          } yield assert(set)(isLeft)
        },
        testM("lSet error when not list") {
          for {
            key   <- uuid
            _     <- set(key, "hello", None, None, None)
            set   <- lSet(key, 0, "goodbye").either
          } yield assert(set)(isLeft)
        },
        testM("lTrim element") {
          for {
            key   <- uuid
            _     <- lPush(key)("world", "hello")
            _     <- lTrim(key, Range(0,0))
            range <- lRange(key, Range(0,1))
          } yield assert(range)(equalTo(Chunk("hello")))
        },
        testM("lTrim start index out of range") {
          for {
            key   <- uuid
            _     <- lPush(key)("world", "hello")
            _     <- lTrim(key, Range(2,5))
            range <- lRange(key, Range(0,1))
          } yield assert(range)(equalTo(Chunk()))
        },
        testM("lTrim end index out of range") {
          for {
            key   <- uuid
            _     <- lPush(key)("world", "hello")
            _     <- lTrim(key, Range(0,3))
            range <- lRange(key, Range(0,1))
          } yield assert(range)(equalTo(Chunk("hello", "world")))
        },
        testM("lTrim error when not list") {
          for {
            key   <- uuid
            _     <- set(key, "hello", None, None, None)
            trim  <- lTrim(key, Range(0,3)).either
          } yield assert(trim)(isLeft)
        },
        testM("rPop element") {
          for {
            key     <- uuid
            _       <- rPush(key)("world", "hello")
            pop  <- rPop(key)
          } yield assert(pop)(isSome(equalTo("hello")))
        },
        testM("rPop no element") {
          for {
            pop  <- rPop("unknown")
          } yield assert(pop)(isNone)
        },
        testM("rPop error not list") {
          for {
            key     <- uuid
            value   <- uuid
            _       <- set(key, value, None, None, None)
            pop     <- rPop(key).either
          } yield assert(pop)(isLeft)
        },
        testM("rPush element") {
          for {
            key     <- uuid
            push    <- rPush(key)("hello")
          } yield assert(push)(equalTo(1l))
        },
        testM("rPush error when not list") {
          for {
            key     <- uuid
            value   <- uuid
            _       <- set(key, value, None, None, None)
            push    <- rPush(key)("hello").either
          } yield assert(push)(isLeft)
        },
        testM("rPushX element") {
          for {
            key     <- uuid
            _       <- rPush(key)("world")
            px      <- rPushX(key)("hello")
          } yield assert(px)(equalTo(2l))
        },
        testM("rPushX nothing when key doesn't exist") {
          for {
            key     <- uuid
            px      <- rPushX(key)("world")
          } yield assert(px)(equalTo(0l))
        },
        testM("rPushX error when not list") {
          for {
            key     <- uuid
            value   <- uuid
            _       <- set(key, value, None, None, None)
            push    <- rPushX(key)("hello").either
          } yield assert(push)(isLeft)
        },
        testM("rPopLPush element") {
          for {
            key     <- uuid
            dest    <- uuid
            _       <- rPush(key)("one", "two", "three")
            _       <- rPush(dest)("four")
            _       <- rPopLPush(key, dest)
            r       <- lRange(key, Range(0, -1))
            l       <- lRange(dest, Range(0, -1))
          } yield assert(r)(equalTo(Chunk("one", "two"))) && assert(l)(equalTo(Chunk("three", "four")))
        },
        testM("rPopLPush nothing when source does not exist") {
          for {
            key     <- uuid
            dest    <- uuid
            _       <- rPush(dest)("four")
            _       <- rPopLPush(key, dest)
            l       <- lRange(dest, Range(0, -1))
          } yield assert(l)(equalTo(Chunk("four")))
        },
        testM("rPopLPush error when not list") {
          for {
            key     <- uuid
            dest    <- uuid
            value   <- uuid
            _       <- set(key, value, None, None, None)
            rpp     <- rPopLPush(key, dest).either
          } yield assert(rpp)(isLeft)
        },
        testM("brPopLPush element") {
          for {
            key     <- uuid
            dest    <- uuid
            _       <- rPush(key)("one", "two", "three")
            _       <- rPush(dest)("four")
            _       <- brPopLPush(key, dest, Duration(1, TimeUnit.SECONDS))
            r       <- lRange(key, Range(0, -1))
            l       <- lRange(dest, Range(0, -1))
          } yield assert(r)(equalTo(Chunk("one", "two"))) && assert(l)(equalTo(Chunk("three", "four")))
        },
        testM("brPopLPush block for 1 second when source does not exist") {
          for {
            key     <- uuid
            dest    <- uuid
            _       <- rPush(dest)("four")
            st      <-  currentTime(TimeUnit.SECONDS)
            s       <- brPopLPush(key, dest, Duration(1, TimeUnit.SECONDS)).either
            _       <- TestClock.adjust(1.seconds)
            endTime <- currentTime(TimeUnit.SECONDS)
          } yield assert(s)(isRight) && assert(endTime - st)(equalTo(1L))
        },
        testM("brPopLPush error when not list") {
          for {
            key     <- uuid
            dest    <- uuid
            value   <- uuid
            _       <- set(key, value, None, None, None)
            bpp     <- brPopLPush(key, dest, Duration(1, TimeUnit.SECONDS)).either
          } yield assert(bpp)(isLeft)
        },
      )
    ).provideCustomLayerShared(Executor ++ Clock.live)

  private val Executor                              = RedisExecutor.live("127.0.0.1", 6379).orDie
  private val uuid                                  = UIO(UUID.randomUUID().toString)
  private def instantOf(millis: Long): UIO[Instant] = UIO(Instant.now().plusMillis(millis))
}
