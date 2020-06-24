package zio.redis

import java.time.Instant
import java.util.UUID

import zio.test.testM
import java.util.concurrent.TimeUnit

import zio.UIO
import zio.duration.Duration
import zio.test._
import zio.test.Assertion._

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
            _        <- set("custom_key_1", value, None, None, None)
            _        <- set("custom_key_2", value, None, None, None)
            response <- keys("*custom*")
          } yield assert(response.length)(equalTo(2))
        },
        testM("pExpire followed by pTtl") {
          for {
            key         <- uuid
            value       <- uuid
            expireAfter <- durationInMillis(100L)
            _           <- set(key, value, None, None, None)
            exp         <- pExpire(key, expireAfter)
            ttl         <- pTtl(key).either
          } yield assert(exp)(isTrue) && assert(ttl)(isRight)
        },
        testM("pExpireAt followed by get and pTtl") {
          for {
            key      <- uuid
            value    <- uuid
            timeout  <- instantForNextMillis(100L)
            _        <- set(key, value, None, None, None)
            exp      <- pExpireAt(key, timeout)
            response <- get(key)
            ttl      <- pTtl(key)
          } yield assert(exp)(isTrue) &&
            assert(response)(isSome) &&
            assert(ttl.toMillis)(isLessThan(timeout.toEpochMilli))
        },
        testM("expire followed by ttl and persist") {
          for {
            key         <- uuid
            value       <- uuid
            expireAfter <- durationInMillis(10000L)
            _           <- set(key, value, None, None, None)
            exp         <- expire(key, expireAfter)
            ttl1        <- ttl(key).either
            persisted   <- persist(key)
            ttl2        <- ttl(key).either
          } yield assert(exp)(isTrue) && assert(ttl1)(isRight) && assert(persisted)(isTrue) && assert(ttl2)(isLeft)
        },
        testM("expireAt followed by get and ttl") {
          for {
            key      <- uuid
            value    <- uuid
            timeout  <- instantForNextMillis(5000L)
            _        <- set(key, value, None, None, None)
            exp      <- expireAt(key, timeout)
            response <- get(key)
            ttl      <- ttl(key)
          } yield assert(exp)(isTrue) &&
            assert(response)(isSome) &&
            assert(ttl.toMillis)(isLessThan(timeout.toEpochMilli))
        },
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
        testM("try to rename non-existing key with RENAMENX command") {
          for {
            key     <- uuid
            newKey  <- uuid
            renamed <- renameNx(key, newKey).either
          } yield assert(renamed)(isLeft)
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
        testM("fetch random key") {
          for {
            key       <- uuid
            value     <- uuid
            _         <- set(key, value, None, None, None)
            allKeys   <- keys("*")
            randomKey <- randomKey()
          } yield assert(allKeys)(contains(randomKey.get))
        },
        testM("check value type by key") {
          import zio.console._
          for {
            key       <- uuid
            value     <- uuid
            _         <- set(key, value, None, None, None)
            valueType <- typeOf(key).either
            _         <- putStrLn(valueType.toString)
          } yield assert(valueType)(isRight)
        },
        testM("dump followed by restore") {
          for {
            key      <- uuid
            value    <- uuid
            _        <- set(key, value, None, None, None)
            dumped   <- dump(key)
            _        <- del(key, Nil)
            restore  <- restore((key, 0L, dumped, None, None, None, None)).either
            restored <- get(key)
          } yield assert(restore)(isRight) && assert(restored)(isSome(equalTo(value)))
        }
      )
    ).provideCustomLayerShared(Executor)

  private val Executor                                          = RedisExecutor.live("127.0.0.1", 6379).orDie
  private val uuid                                              = UIO(UUID.randomUUID().toString)
  private def durationInMillis(seconds: Long): UIO[Duration]    = UIO(Duration(seconds, TimeUnit.MILLISECONDS))
  private def instantForNextMillis(seconds: Long): UIO[Instant] = UIO(Instant.now().plusMillis(seconds))
}
