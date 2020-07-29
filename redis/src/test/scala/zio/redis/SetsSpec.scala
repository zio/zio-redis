package zio.redis

import zio.Chunk
import zio.redis.RedisError.WrongType
import zio.test._
import zio.test.Assertion._

trait SetsSpec extends BaseSpec {
  val setsSuite =
    suite("sets")(
      suite("add")(
        testM("sAdd to the empty set") {
          for {
            key <- uuid
            added <- sAdd(key)("hello")
            members <- sMembers(key)
          } yield assert(added)(equalTo(1L)) &&
              assert(members)(hasSameElements(Chunk("hello")))
        },
        testM("sAdd to the non-empty set") {
          for {
            key <- uuid
            _ <- sAdd(key)("hello")
            added <- sAdd(key)("world")
            members <- sMembers(key)
          } yield assert(added)(equalTo(1L)) &&
              assert(members)(hasSameElements(Chunk("hello", "world")))
        },
        testM("sAdd existing element to the set") {
          for {
            key <- uuid
            _ <- sAdd(key)("hello")
            added <- sAdd(key)("hello")
            members <- sMembers(key)
          } yield assert(added)(equalTo(0L)) &&
            assert(members)(hasSameElements(Chunk("hello")))
        },
        testM("sAdd error when not set") {
          for {
            key <- uuid
            value <- uuid
            _ <- set(key, value, None, None, None)
            added <- sAdd(key)("hello").either
          } yield assert(added)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("cardinality")(
        testM("sCard non-empty set") {
          for {
            key <- uuid
            _ <- sAdd(key)("hello")
            card <- sCard(key)
          } yield assert(card)(equalTo(1L))
        },
        testM("sCard 0 when key doesn't exist") {
          assertM(sCard("unknown"))(equalTo(0L))
        },
        testM("sCard error when not set") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value, None, None, None)
            card <- sCard(key).either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        }
      )
    )
}
