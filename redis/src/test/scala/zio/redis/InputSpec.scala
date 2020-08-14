package zio.redis

import zio.{ Chunk, Task }
import zio.redis.Input._
import zio.test._
import zio.test.Assertion._

object InputSpec extends BaseSpec {
  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("Input encoders")(
      suite("abs ttl")(
        testM("valid value") {
          for {
            result <- Task(AbsTtlInput.encode(AbsTtl))
          } yield assert(result)(equalTo(Chunk.single("$6\r\nABSTTL\r\n")))
        }
      ),
      suite("aggregate")(
        testM("max") {
          for {
            result <- Task(AggregateInput.encode(Aggregate.Max))
          } yield assert(result)(equalTo(Chunk.single("$3\r\nMAX\r\n")))
        },
        testM("min") {
          for {
            result <- Task(AggregateInput.encode(Aggregate.Min))
          } yield assert(result)(equalTo(Chunk.single("$3\r\nMIN\r\n")))
        },
        testM("sum") {
          for {
            result <- Task(AggregateInput.encode(Aggregate.Sum))
          } yield assert(result)(equalTo(Chunk.single("$3\r\nSUM\r\n")))
        }
      )
    )
}
