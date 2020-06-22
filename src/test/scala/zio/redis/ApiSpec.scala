package zio.redis

import zio.test._
import zio.test.Assertion._

object ApiSpec extends BaseSpec {
  def spec =
    suite("Redis commands")(
      suite("keys")(
        testM("set followed by get") {
          val key   = "key"
          val value = "value"

          for {
            _ <- set(key, value, None, None, None)
            v <- get(key)
          } yield assert(v)(isSome(equalTo(value)))
        },
        testM("get non-existing key") {
          assertM(get("non-existent"))(isNone)
        },
        testM("handles wrong types") {
          val set = "test-set"

          for {
            _ <- sAdd(set)("1", "2", "3")
            v <- get(set).either
          } yield assert(v)(isLeft)
        },
        testM("check whether or not key exists") {
          val key = "existing"

          for {
            _  <- set(key, key, None, None, None)
            k1 <- exists(key, Nil)
            k2 <- exists("unknown", Nil)
          } yield assert(k1)(isTrue) && assert(k2)(isFalse)
        }
      )
    ).provideCustomLayerShared(Executor)

  private val Executor = RedisExecutor.live("127.0.0.1", 6379).orDie
}
