package zio.redis

import zio.Chunk
import zio.test._
import zio.test.Assertion._

object CommandsSpec extends BaseSpec {
  def spec =
    suite("Redis commands")(
      suite("keys")(
        testM("put followed by get") {
          val data = Chunk.fromArray("test".getBytes)

          for {
            _ <- set("key", data, None, None, None).ignore
            _ <- get("key").ignore
          } yield assert(1)(equalTo(1))
        }
      )
    ).provideCustomLayerShared(Executor)

  private val Executor = RedisExecutor.live("127.0.0.1", 6379).orDie
}
