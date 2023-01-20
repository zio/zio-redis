package zio.redis.transactional

import zio.redis.{BaseSpec, RedisError}
import zio.test.Assertion._
import zio.test.{Spec, _}
import zio.{Chunk, ZIO}

trait RedisTransactionSpec extends BaseSpec {
  def transactionsSuite: Spec[Redis, RedisError] =
    suite("RedisTransaction")(
      suite("zip")(
        test("nested commands")(
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            result <-
              redis.sAdd(key, "hello").zip(redis.sAdd(key, "world").zip(redis.sMembers(key).returning[String])).commit
          } yield assert(result._1)(equalTo(1L)) &&
            assert(result._2._1)(equalTo(1L)) &&
            assert(result._2._2)(hasSameElements(Chunk("hello", "world")))
        )
      ),
      suite("zipLeft")(
        test("nested command")(
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            result <- redis
                        .sAdd(key, "hello")
                        .zip(redis.sAdd(key, "world").zipLeft(redis.sMembers(key).returning[String]))
                        .commit
          } yield assert(result)(equalTo((1L, 1L)))
        ),
        test("ignore nested command")(
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            result <- redis
                        .sAdd(key, "hello")
                        .zipLeft(redis.sAdd(key, "world").zipRight(redis.sMembers(key).returning[String]))
                        .commit
          } yield assert(result)(equalTo(1L))
        )
      ),
      suite("zipRight")(
        test("nested command")(
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            result <- redis
                        .sAdd(key, "hello")
                        .zip(redis.sAdd(key, "world").zipRight(redis.sMembers(key).returning[String]))
                        .commit
          } yield assert(result._1)(equalTo(1L)) && assert(result._2)(hasSameElements(Chunk("hello", "world")))
        )
      )
    )
}
