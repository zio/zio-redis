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
            key      <- uuid
            value    <- uuid
            _        <- set(key, value, None, None, None)
            response <- del(key, Nil)
          } yield assert(response)(equalTo(1L))
        },
        testM("find all keys matching pattern") {
          for {
            value    <- uuid
            _        <- set("custom_key_1", value, None, None, None)
            _        <- set("custom_key_2", value, None, None, None)
            response <- keys("*custom*")
          } yield assert(response.length)(equalTo(2))
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
        testM("expire followed by ttl and persist") {
          for {
            key         <- uuid
            value       <- uuid
            expireAfter <- durationInMillis(10000L)
            _           <- set(key, value, None, None, None)
            exp         <- expire(key, expireAfter)
            ttl1        <- ttl(key).either
            persist     <- persist(key)
            ttl2        <- ttl(key).either
          } yield assert(exp)(isTrue) && assert(ttl1)(isRight) && assert(persist)(isTrue) && assert(ttl2)(isLeft)
        },
        testM("check value type by key") {
          //import zio.console._
          for {
            key       <- uuid
            value     <- uuid
            _         <- set(key, value, None, None, None)
            valueType <- typeOf(key).either
          } yield assert(valueType)(isRight)
        }
      )
    ).provideCustomLayerShared(Executor)

  private val Executor                                          = RedisExecutor.live("127.0.0.1", 6379).orDie
  private val uuid                                              = UIO(UUID.randomUUID().toString)
  private def durationInMillis(seconds: Long): UIO[Duration]    = UIO(Duration(seconds, TimeUnit.MILLISECONDS))
  private def instantForNextMillis(seconds: Long): UIO[Instant] = UIO(Instant.now().plusMillis(seconds))
}
