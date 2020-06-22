package zio.redis

import java.util.UUID

import zio.UIO
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
        }
      )
    ).provideCustomLayerShared(Executor)

  private val Executor = RedisExecutor.live("127.0.0.1", 6379).orDie
  private val uuid     = UIO(UUID.randomUUID().toString)
}
