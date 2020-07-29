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
      ),
      suite("difference")(
        testM("sDiff two non-empty sets") {
          for {
            first <- uuid
            second <- uuid
            _ <- sAdd(first)("a", "b", "c", "d")
            _ <- sAdd(second)("a", "c")
            diff <- sDiff(first, List(second))
          } yield assert(diff)(hasSameElements(Chunk("b", "d")))
        },
        testM("sDiff non-empty set and empty set") {
          for {
            first <- uuid
            second <- uuid
            _ <- sAdd(first)("a", "b")
            diff <- sDiff(first, List(second))
          } yield assert(diff)(hasSameElements(Chunk("a", "b")))
        },
        testM("sDiff empty set and non-empty set") {
          for {
            first <- uuid
            second <- uuid
            _ <- sAdd(second)("a", "b")
            diff <- sDiff(first, List(second))
          } yield assert(diff)(isEmpty)
        },
        testM("sDiff empty when both sets are empty") {
          for {
            first <- uuid
            second <- uuid
            diff <- sDiff(first, List(second))
          } yield assert(diff)(isEmpty)
        },
        testM("sDiff non-empty set with multiple non-empty sets") {
          for {
            first <- uuid
            second <- uuid
            third <- uuid
            _ <- sAdd(first)("a", "b", "c", "d")
            _ <- sAdd(second)("b", "d")
            _ <- sAdd(third)("b", "c")
            diff <- sDiff(first, List(second, third))
          } yield assert(diff)(hasSameElements(Chunk("a")))
        },
        testM("sDiff error when first parameter is not set") {
          for {
            first <- uuid
            second <- uuid
            value <- uuid
            _ <- set(first, value, None, None, None)
            diff <- sDiff(first, List(second)).either
          } yield assert(diff)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("sDiff error when second parameter is not set") {
          for {
            first <- uuid
            second <- uuid
            value <- uuid
            _ <- set(second, value, None, None, None)
            diff <- sDiff(first, List(second)).either
          } yield assert(diff)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("store difference")(
        testM("sDiffStore two non-empty sets") {
          for {
            key <- uuid
            first <- uuid
            second <- uuid
            _ <- sAdd(first)("a", "b", "c", "d")
            _ <- sAdd(second)("a", "c")
            card <- sDiffStore(key)(first, second)
            diff <- sMembers(key)
          } yield assert(card)(equalTo(2L)) &&
              assert(diff)(hasSameElements(Chunk("b", "d")))
        },
        testM("sDiffStore non-empty set and empty set") {
          for {
            key <- uuid
            first <- uuid
            second <- uuid
            _ <- sAdd(first)("a", "b")
            card <- sDiffStore(key)(first, second)
            diff <- sMembers(key)
          } yield assert(card)(equalTo(2L)) &&
              assert(diff)(hasSameElements(Chunk("a", "b")))
        },
        testM("sDiffStore empty set and non-empty set") {
          for {
            key <- uuid
            first <- uuid
            second <- uuid
            _ <- sAdd(second)("a", "b")
            card <- sDiffStore(key)(first, second)
            diff <- sMembers(key)
          } yield assert(card)(equalTo(0L)) &&
              assert(diff)(isEmpty)
        },
        testM("sDiffStore empty when both sets are empty") {
          for {
            key <- uuid
            first <- uuid
            second <- uuid
            card <- sDiffStore(key)(first, second)
            diff <- sMembers(key)
          } yield assert(card)(equalTo(0L)) &&
              assert(diff)(isEmpty)
        },
        testM("sDiffStore non-empty set with multiple non-empty sets") {
          for {
            key <- uuid
            first <- uuid
            second <- uuid
            third <- uuid
            _ <- sAdd(first)("a", "b", "c", "d")
            _ <- sAdd(second)("b", "d")
            _ <- sAdd(third)("b", "c")
            card <- sDiffStore(key)(first, second, third)
            diff <- sMembers(key)
          } yield assert(card)(equalTo(1L)) &&
              assert(diff)(hasSameElements(Chunk("a")))
        },
        testM("sDiffStore error when first parameter is not set") {
          for {
            key <- uuid
            first <- uuid
            second <- uuid
            value <- uuid
            _ <- set(first, value, None, None, None)
            card <- sDiffStore(key)(first, second).either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("sDiffStore error when second parameter is not set") {
          for {
            key <- uuid
            first <- uuid
            second <- uuid
            value <- uuid
            _ <- set(second, value, None, None, None)
            card <- sDiffStore(key)(first, second).either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("intersection")(
        testM("sInter two non-empty sets") {
          for {
            first <- uuid
            second <- uuid
            _ <- sAdd(first)("a", "b", "c", "d")
            _ <- sAdd(second)("a", "c", "e")
            inter <- sInter(first, List(second))
          } yield assert(inter)(hasSameElements(Chunk("a", "c")))
        },
        testM("sInter empty when one of the sets is empty") {
          for {
            first <- uuid
            second <- uuid
            _ <- sAdd(first)("a", "b")
            inter <- sInter(first, List(second))
          } yield assert(inter)(isEmpty)
        },
        testM("sInter empty when both sets are empty") {
          for {
            first <- uuid
            second <- uuid
            inter <- sInter(first, List(second))
          } yield assert(inter)(isEmpty)
        },
        testM("sInter non-empty set with multiple non-empty sets") {
          for {
            first <- uuid
            second <- uuid
            third <- uuid
            _ <- sAdd(first)("a", "b", "c", "d")
            _ <- sAdd(second)("b", "d")
            _ <- sAdd(third)("b", "c")
            inter <- sInter(first, List(second, third))
          } yield assert(inter)(hasSameElements(Chunk("b")))
        },
        testM("sInter error when first parameter is not set") {
          for {
            first <- uuid
            second <- uuid
            value <- uuid
            _ <- set(first, value, None, None, None)
            inter <- sInter(first, List(second)).either
          } yield assert(inter)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("sInter empty with empty first set and second parameter is not set") {
          for {
            first <- uuid
            second <- uuid
            value <- uuid
            _ <- set(second, value, None, None, None)
            inter <- sInter(first, List(second))
          } yield assert(inter)(isEmpty)
        },
        testM("sInter error with non-empty first set and second parameter is not set") {
          for {
            first <- uuid
            second <- uuid
            value <- uuid
            _ <- sAdd(first)("a")
            _ <- set(second, value, None, None, None)
            inter <- sInter(first, List(second)).either
          } yield assert(inter)(isLeft(isSubtype[WrongType](anything)))
        }
      )
    )
}
