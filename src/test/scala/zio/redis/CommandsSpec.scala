package zio.redis

import zio.test._
import zio.test.Assertion._

object CommandsSpec extends BaseSpec {
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
        testM("handles wront types") {
          val set = "test-set"

          for {
            _ <- sAdd(set)("1", "2", "3")
            v <- get(set).either
          } yield assert(v)(isLeft)
        }
      )
    ).provideCustomLayerShared(Executor)

  private val Executor = RedisExecutor.live("127.0.0.1", 6379).orDie
}
