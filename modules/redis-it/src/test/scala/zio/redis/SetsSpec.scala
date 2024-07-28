package zio.redis

import zio._
import zio.redis.RedisError.WrongType
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test._

trait SetsSpec extends IntegrationSpec {
  def setsSuite: Spec[Redis, RedisError] =
    suite("sets")(
      suite("sAdd")(
        test("to empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            added <- redis.sAdd(key, "hello")
          } yield assert(added)(equalTo(1L))
        },
        test("to the non-empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.sAdd(key, "hello")
            added <- redis.sAdd(key, "world")
          } yield assert(added)(equalTo(1L))
        },
        test("existing element to set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.sAdd(key, "hello")
            added <- redis.sAdd(key, "hello")
          } yield assert(added)(equalTo(0L))
        },
        test("multiple elements to set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            added <- redis.sAdd(key, "a", "b", "c")
          } yield assert(added)(equalTo(3L))
        },
        test("error when not set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            value <- uuid
            _     <- redis.set(key, value)
            added <- redis.sAdd(key, "hello").either
          } yield assert(added)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("sCard")(
        test("non-empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.sAdd(key, "hello")
            card  <- redis.sCard(key)
          } yield assert(card)(equalTo(1L))
        },
        test("0 when key doesn't exist") {
          for {
            redis   <- ZIO.service[Redis]
            unknown <- uuid
            card    <- redis.sCard(unknown)
          } yield assert(card)(equalTo(0L))
        },
        test("error when not set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            value <- uuid
            _     <- redis.set(key, value)
            card  <- redis.sCard(key).either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("sDiff")(
        test("two non-empty sets") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            _      <- redis.sAdd(first, "a", "b", "c", "d")
            _      <- redis.sAdd(second, "a", "c")
            diff   <- redis.sDiff(first, second).returning[String]
          } yield assert(diff)(hasSameElements(Chunk("b", "d")))
        },
        test("non-empty set and empty set") {
          for {
            redis    <- ZIO.service[Redis]
            nonEmpty <- uuid
            empty    <- uuid
            _        <- redis.sAdd(nonEmpty, "a", "b")
            diff     <- redis.sDiff(nonEmpty, empty).returning[String]
          } yield assert(diff)(hasSameElements(Chunk("a", "b")))
        },
        test("empty set and non-empty set") {
          for {
            redis    <- ZIO.service[Redis]
            empty    <- uuid
            nonEmpty <- uuid
            _        <- redis.sAdd(nonEmpty, "a", "b")
            diff     <- redis.sDiff(empty, nonEmpty).returning[String]
          } yield assert(diff)(isEmpty)
        },
        test("empty when both sets are empty") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            diff   <- redis.sDiff(first, second).returning[String]
          } yield assert(diff)(isEmpty)
        },
        test("non-empty set with multiple non-empty sets") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            third  <- uuid
            _      <- redis.sAdd(first, "a", "b", "c", "d")
            _      <- redis.sAdd(second, "b", "d")
            _      <- redis.sAdd(third, "b", "c")
            diff   <- redis.sDiff(first, second, third).returning[String]
          } yield assert(diff)(hasSameElements(Chunk("a")))
        },
        test("error when first parameter is not set") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- redis.set(first, value)
            diff   <- redis.sDiff(first, second).returning[String].either
          } yield assert(diff)(isLeft(isSubtype[WrongType](anything)))
        },
        test("error when second parameter is not set") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- redis.set(second, value)
            diff   <- redis.sDiff(first, second).returning[String].either
          } yield assert(diff)(isLeft(isSubtype[WrongType](anything)))
        }
      ) @@ clusterExecutorUnsupported,
      suite("sDiffStore")(
        test("two non-empty sets") {
          for {
            redis  <- ZIO.service[Redis]
            dest   <- uuid
            first  <- uuid
            second <- uuid
            _      <- redis.sAdd(first, "a", "b", "c", "d")
            _      <- redis.sAdd(second, "a", "c")
            card   <- redis.sDiffStore(dest, first, second)
          } yield assert(card)(equalTo(2L))
        },
        test("non-empty set and empty set") {
          for {
            redis    <- ZIO.service[Redis]
            dest     <- uuid
            nonEmpty <- uuid
            empty    <- uuid
            _        <- redis.sAdd(nonEmpty, "a", "b")
            card     <- redis.sDiffStore(dest, nonEmpty, empty)
          } yield assert(card)(equalTo(2L))
        },
        test("empty set and non-empty set") {
          for {
            redis    <- ZIO.service[Redis]
            dest     <- uuid
            empty    <- uuid
            nonEmpty <- uuid
            _        <- redis.sAdd(nonEmpty, "a", "b")
            card     <- redis.sDiffStore(dest, empty, nonEmpty)
          } yield assert(card)(equalTo(0L))
        },
        test("empty when both sets are empty") {
          for {
            redis  <- ZIO.service[Redis]
            dest   <- uuid
            first  <- uuid
            second <- uuid
            card   <- redis.sDiffStore(dest, first, second)
          } yield assert(card)(equalTo(0L))
        },
        test("non-empty set with multiple non-empty sets") {
          for {
            redis  <- ZIO.service[Redis]
            dest   <- uuid
            first  <- uuid
            second <- uuid
            third  <- uuid
            _      <- redis.sAdd(first, "a", "b", "c", "d")
            _      <- redis.sAdd(second, "b", "d")
            _      <- redis.sAdd(third, "b", "c")
            card   <- redis.sDiffStore(dest, first, second, third)
          } yield assert(card)(equalTo(1L))
        },
        test("error when first parameter is not set") {
          for {
            redis  <- ZIO.service[Redis]
            dest   <- uuid
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- redis.set(first, value)
            card   <- redis.sDiffStore(dest, first, second).either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        },
        test("error when second parameter is not set") {
          for {
            redis  <- ZIO.service[Redis]
            dest   <- uuid
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- redis.set(second, value)
            card   <- redis.sDiffStore(dest, first, second).either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        }
      ) @@ clusterExecutorUnsupported,
      suite("sInter")(
        test("two non-empty sets") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            _      <- redis.sAdd(first, "a", "b", "c", "d")
            _      <- redis.sAdd(second, "a", "c", "e")
            inter  <- redis.sInter(first, second).returning[String]
          } yield assert(inter)(hasSameElements(Chunk("a", "c")))
        },
        test("empty when one of the sets is empty") {
          for {
            redis    <- ZIO.service[Redis]
            nonEmpty <- uuid
            empty    <- uuid
            _        <- redis.sAdd(nonEmpty, "a", "b")
            inter    <- redis.sInter(nonEmpty, empty).returning[String]
          } yield assert(inter)(isEmpty)
        },
        test("empty when both sets are empty") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            inter  <- redis.sInter(first, second).returning[String]
          } yield assert(inter)(isEmpty)
        },
        test("non-empty set with multiple non-empty sets") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            third  <- uuid
            _      <- redis.sAdd(first, "a", "b", "c", "d")
            _      <- redis.sAdd(second, "b", "d")
            _      <- redis.sAdd(third, "b", "c")
            inter  <- redis.sInter(first, second, third).returning[String]
          } yield assert(inter)(hasSameElements(Chunk("b")))
        },
        test("error when first parameter is not set") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- redis.set(first, value)
            inter  <- redis.sInter(first, second).returning[String].either
          } yield assert(inter)(isLeft(isSubtype[WrongType](anything)))
        },
        test("error when empty first set and second parameter is not set") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- redis.set(second, value)
            inter  <- redis.sInter(first, second).returning[String].either
          } yield assert(inter)(isLeft(isSubtype[WrongType](anything)))
        },
        test("error with non-empty first set and second parameter is not set") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- redis.sAdd(first, "a")
            _      <- redis.set(second, value)
            inter  <- redis.sInter(first, second).returning[String].either
          } yield assert(inter)(isLeft(isSubtype[WrongType](anything)))
        }
      ) @@ clusterExecutorUnsupported,
      suite("sInterStore")(
        test("two non-empty sets") {
          for {
            redis  <- ZIO.service[Redis]
            dest   <- uuid
            first  <- uuid
            second <- uuid
            _      <- redis.sAdd(first, "a", "b", "c", "d")
            _      <- redis.sAdd(second, "a", "c", "e")
            card   <- redis.sInterStore(dest, first, second)
          } yield assert(card)(equalTo(2L))
        },
        test("empty when one of the sets is empty") {
          for {
            redis    <- ZIO.service[Redis]
            dest     <- uuid
            nonEmpty <- uuid
            empty    <- uuid
            _        <- redis.sAdd(nonEmpty, "a", "b")
            card     <- redis.sInterStore(dest, nonEmpty, empty)
          } yield assert(card)(equalTo(0L))
        },
        test("empty when both sets are empty") {
          for {
            redis  <- ZIO.service[Redis]
            dest   <- uuid
            first  <- uuid
            second <- uuid
            card   <- redis.sInterStore(dest, first, second)
          } yield assert(card)(equalTo(0L))
        },
        test("non-empty set with multiple non-empty sets") {
          for {
            redis  <- ZIO.service[Redis]
            dest   <- uuid
            first  <- uuid
            second <- uuid
            third  <- uuid
            _      <- redis.sAdd(first, "a", "b", "c", "d")
            _      <- redis.sAdd(second, "b", "d")
            _      <- redis.sAdd(third, "b", "c")
            card   <- redis.sInterStore(dest, first, second, third)
          } yield assert(card)(equalTo(1L))
        },
        test("error when first parameter is not set") {
          for {
            redis  <- ZIO.service[Redis]
            dest   <- uuid
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- redis.set(first, value)
            card   <- redis.sInterStore(dest, first, second).either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        },
        test("error when empty first set and second parameter is not set") {
          for {
            redis  <- ZIO.service[Redis]
            dest   <- uuid
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- redis.set(second, value)
            card   <- redis.sInterStore(dest, first, second).either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        },
        test("error with non-empty first set and second parameter is not set") {
          for {
            redis  <- ZIO.service[Redis]
            dest   <- uuid
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- redis.sAdd(first, "a")
            _      <- redis.set(second, value)
            card   <- redis.sInterStore(dest, first, second).either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        }
      ) @@ clusterExecutorUnsupported,
      suite("sIsMember")(
        test("actual element of the non-empty set") {
          for {
            redis    <- ZIO.service[Redis]
            key      <- uuid
            _        <- redis.sAdd(key, "a", "b", "c")
            isMember <- redis.sIsMember(key, "b")
          } yield assert(isMember)(isTrue)
        },
        test("element that is not present in the set") {
          for {
            redis    <- ZIO.service[Redis]
            key      <- uuid
            _        <- redis.sAdd(key, "a", "b", "c")
            isMember <- redis.sIsMember(key, "unknown")
          } yield assert(isMember)(isFalse)
        },
        test("of an empty set") {
          for {
            redis    <- ZIO.service[Redis]
            key      <- uuid
            isMember <- redis.sIsMember(key, "a")
          } yield assert(isMember)(isFalse)
        },
        test("when not set") {
          for {
            redis    <- ZIO.service[Redis]
            key      <- uuid
            value    <- uuid
            _        <- redis.set(key, value)
            isMember <- redis.sIsMember(key, "a").either
          } yield assert(isMember)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("sMembers")(
        test("non-empty set") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            _       <- redis.sAdd(key, "a", "b", "c")
            members <- redis.sMembers(key).returning[String]
          } yield assert(members)(hasSameElements(Chunk("a", "b", "c")))
        },
        test("empty set") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            members <- redis.sMembers(key).returning[String]
          } yield assert(members)(isEmpty)
        },
        test("when not set") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            value   <- uuid
            _       <- redis.set(key, value)
            members <- redis.sMembers(key).returning[String].either
          } yield assert(members)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("sMove")(
        test("from non-empty source to non-empty destination") {
          for {
            redis <- ZIO.service[Redis]
            src   <- uuid
            dest  <- uuid
            _     <- redis.sAdd(src, "a", "b", "c")
            _     <- redis.sAdd(dest, "d", "e", "f")
            moved <- redis.sMove(src, dest, "a")
          } yield assert(moved)(isTrue)
        },
        test("from non-empty source to empty destination") {
          for {
            redis <- ZIO.service[Redis]
            src   <- uuid
            dest  <- uuid
            _     <- redis.sAdd(src, "a", "b", "c")
            moved <- redis.sMove(src, dest, "a")
          } yield assert(moved)(isTrue)
        },
        test("element already present in the destination") {
          for {
            redis <- ZIO.service[Redis]
            src   <- uuid
            dest  <- uuid
            _     <- redis.sAdd(src, "a", "b", "c")
            _     <- redis.sAdd(dest, "a", "d", "e")
            moved <- redis.sMove(src, dest, "a")
          } yield assert(moved)(isTrue)
        },
        test("from empty source to non-empty destination") {
          for {
            redis <- ZIO.service[Redis]
            src   <- uuid
            dest  <- uuid
            _     <- redis.sAdd(dest, "b", "c")
            moved <- redis.sMove(src, dest, "a")
          } yield assert(moved)(isFalse)
        },
        test("non-existent element") {
          for {
            redis <- ZIO.service[Redis]
            src   <- uuid
            dest  <- uuid
            _     <- redis.sAdd(src, "a", "b")
            _     <- redis.sAdd(dest, "c", "d")
            moved <- redis.sMove(src, dest, "unknown")
          } yield assert(moved)(isFalse)
        },
        test("from empty source to not set destination") {
          for {
            redis <- ZIO.service[Redis]
            src   <- uuid
            dest  <- uuid
            value <- uuid
            _     <- redis.set(dest, value)
            moved <- redis.sMove(src, dest, "unknown")
          } yield assert(moved)(isFalse)
        },
        test("error when non-empty source and not set destination") {
          for {
            redis <- ZIO.service[Redis]
            src   <- uuid
            dest  <- uuid
            value <- uuid
            _     <- redis.sAdd(src, "a", "b", "c")
            _     <- redis.set(dest, value)
            moved <- redis.sMove(src, dest, "a").either
          } yield assert(moved)(isLeft(isSubtype[WrongType](anything)))
        },
        test("error when not set source to non-empty destination") {
          for {
            redis <- ZIO.service[Redis]
            src   <- uuid
            dest  <- uuid
            value <- uuid
            _     <- redis.set(src, value)
            _     <- redis.sAdd(dest, "a", "b", "c")
            moved <- redis.sMove(src, dest, "a").either
          } yield assert(moved)(isLeft(isSubtype[WrongType](anything)))
        }
      ) @@ clusterExecutorUnsupported,
      suite("sPop")(
        test("one element from non-empty set") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.sAdd(key, "a", "b", "c")
            popped <- redis.sPop(key).returning[String]
          } yield assert(popped)(isNonEmpty)
        },
        test("one element from an empty set") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            popped <- redis.sPop(key).returning[String]
          } yield assert(popped)(isEmpty)
        },
        test("error when poping one element from not set") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            value  <- uuid
            _      <- redis.set(key, value)
            popped <- redis.sPop(key).returning[String].either
          } yield assert(popped)(isLeft(isSubtype[WrongType](anything)))
        },
        test("multiple elements from non-empty set") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.sAdd(key, "a", "b", "c")
            popped <- redis.sPop(key, Some(2L)).returning[String]
          } yield assert(popped)(hasSize(equalTo(2)))
        },
        test("more elements then there is in non-empty set") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.sAdd(key, "a", "b", "c")
            popped <- redis.sPop(key, Some(5L)).returning[String]
          } yield assert(popped)(hasSize(equalTo(3)))
        },
        test("multiple elements from empty set") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            popped <- redis.sPop(key, Some(3)).returning[String]
          } yield assert(popped)(isEmpty)
        },
        test("error when poping multiple elements from not set") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            value  <- uuid
            _      <- redis.set(key, value)
            popped <- redis.sPop(key, Some(3)).returning[String].either
          } yield assert(popped)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("sRandMember")(
        test("one element from non-empty set") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.sAdd(key, "a", "b", "c")
            member <- redis.sRandMember(key).returning[String]
          } yield assert(member)(hasSize(equalTo(1)))
        },
        test("one element from an empty set") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            member <- redis.sRandMember(key).returning[String]
          } yield assert(member)(isEmpty)
        },
        test("error when one element from not set") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            value  <- uuid
            _      <- redis.set(key, value)
            member <- redis.sRandMember(key).returning[String].either
          } yield assert(member)(isLeft(isSubtype[WrongType](anything)))
        },
        test("multiple elements from non-empty set") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            _       <- redis.sAdd(key, "a", "b", "c")
            members <- redis.sRandMember(key, Some(2L)).returning[String]
          } yield assert(members)(hasSize(equalTo(2)))
        },
        test("more elements than there is present in the non-empty set") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            _       <- redis.sAdd(key, "a", "b", "c")
            members <- redis.sRandMember(key, Some(5L)).returning[String]
          } yield assert(members)(hasSize(equalTo(3)))
        },
        test("multiple elements from an empty set") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            members <- redis.sRandMember(key, Some(3L)).returning[String]
          } yield assert(members)(isEmpty)
        },
        test("repeated elements from non-empty set") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            _       <- redis.sAdd(key, "a", "b", "c")
            members <- redis.sRandMember(key, Some(-5L)).returning[String]
          } yield assert(members)(hasSize(equalTo(5)))
        },
        test("repeated elements from an empty set") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            members <- redis.sRandMember(key, Some(-3L)).returning[String]
          } yield assert(members)(isEmpty)
        },
        test("error multiple elements from not set") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            value   <- uuid
            _       <- redis.set(key, value)
            members <- redis.sRandMember(key, Some(3L)).returning[String].either
          } yield assert(members)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("sRem")(
        test("existing elements from non-empty set") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            _       <- redis.sAdd(key, "a", "b", "c")
            removed <- redis.sRem(key, "b", "c")
          } yield assert(removed)(equalTo(2L))
        },
        test("when just part of elements are present in the non-empty set") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            _       <- redis.sAdd(key, "a", "b", "c")
            removed <- redis.sRem(key, "b", "d")
          } yield assert(removed)(equalTo(1L))
        },
        test("when none of the elements are present in the non-empty set") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            _       <- redis.sAdd(key, "a", "b", "c")
            removed <- redis.sRem(key, "d", "e")
          } yield assert(removed)(equalTo(0L))
        },
        test("elements from an empty set") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            removed <- redis.sRem(key, "a", "b")
          } yield assert(removed)(equalTo(0L))
        },
        test("elements from not set") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            value   <- uuid
            _       <- redis.set(key, value)
            removed <- redis.sRem(key, "a", "b").either
          } yield assert(removed)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("sUnion")(
        test("two non-empty sets") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            _      <- redis.sAdd(first, "a", "b", "c", "d")
            _      <- redis.sAdd(second, "a", "c", "e")
            union  <- redis.sUnion(first, second).returning[String]
          } yield assert(union)(hasSameElements(Chunk("a", "b", "c", "d", "e")))
        },
        test("equal to the non-empty set when the other one is empty") {
          for {
            redis    <- ZIO.service[Redis]
            nonEmpty <- uuid
            empty    <- uuid
            _        <- redis.sAdd(nonEmpty, "a", "b")
            union    <- redis.sUnion(nonEmpty, empty).returning[String]
          } yield assert(union)(hasSameElements(Chunk("a", "b")))
        },
        test("empty when both sets are empty") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            union  <- redis.sUnion(first, second).returning[String]
          } yield assert(union)(isEmpty)
        },
        test("non-empty set with multiple non-empty sets") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            third  <- uuid
            _      <- redis.sAdd(first, "a", "b", "c", "d")
            _      <- redis.sAdd(second, "b", "d")
            _      <- redis.sAdd(third, "b", "c", "e")
            union  <- redis.sUnion(first, second, third).returning[String]
          } yield assert(union)(hasSameElements(Chunk("a", "b", "c", "d", "e")))
        },
        test("error when first parameter is not set") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- redis.set(first, value)
            union  <- redis.sUnion(first, second).returning[String].either
          } yield assert(union)(isLeft(isSubtype[WrongType](anything)))
        },
        test("error when the first parameter is set and the second parameter is not set") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- redis.sAdd(first, "a")
            _      <- redis.set(second, value)
            union  <- redis.sUnion(first, second).returning[String].either
          } yield assert(union)(isLeft(isSubtype[WrongType](anything)))
        }
      ) @@ clusterExecutorUnsupported,
      suite("sUnionStore")(
        test("two non-empty sets") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- redis.sAdd(first, "a", "b", "c", "d")
            _      <- redis.sAdd(second, "a", "c", "e")
            card   <- redis.sUnionStore(dest, first, second)
          } yield assert(card)(equalTo(5L))
        },
        test("equal to the non-empty set when the other one is empty") {
          for {
            redis    <- ZIO.service[Redis]
            nonEmpty <- uuid
            empty    <- uuid
            dest     <- uuid
            _        <- redis.sAdd(nonEmpty, "a", "b")
            card     <- redis.sUnionStore(dest, nonEmpty, empty)
          } yield assert(card)(equalTo(2L))
        },
        test("empty when both sets are empty") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            dest   <- uuid
            card   <- redis.sUnionStore(dest, first, second)
          } yield assert(card)(equalTo(0L))
        },
        test("non-empty set with multiple non-empty sets") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            third  <- uuid
            dest   <- uuid
            _      <- redis.sAdd(first, "a", "b", "c", "d")
            _      <- redis.sAdd(second, "b", "d")
            _      <- redis.sAdd(third, "b", "c", "e")
            card   <- redis.sUnionStore(dest, first, second, third)
          } yield assert(card)(equalTo(5L))
        },
        test("error when the first parameter is not set") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            dest   <- uuid
            value  <- uuid
            _      <- redis.set(first, value)
            card   <- redis.sUnionStore(dest, first, second).either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        },
        test("error when the first parameter is set and the second parameter is not set") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            dest   <- uuid
            value  <- uuid
            _      <- redis.sAdd(first, "a")
            _      <- redis.set(second, value)
            card   <- redis.sUnionStore(dest, first, second).either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        }
      ) @@ clusterExecutorUnsupported,
      suite("sScan")(
        test("non-empty set") {
          val testData = NonEmptyChunk("a", "b", "c")
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            _       <- redis.sAdd(key, testData.head, testData.tail: _*)
            members <- scanAll(key)
          } yield assert(members.toSet)(equalTo(testData.toSet))
        },
        test("empty set") {
          for {
            redis            <- ZIO.service[Redis]
            key              <- uuid
            scan             <- redis.sScan(key, 0L).returning[String]
            (cursor, members) = scan
          } yield assert(cursor)(isZero) &&
            assert(members)(isEmpty)
        },
        test("with match over non-empty set") {
          val testData = NonEmptyChunk("one", "two", "three")
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            _       <- redis.sAdd(key, testData.head, testData.tail: _*)
            members <- scanAll(key, Some("t[a-z]*"))
          } yield assert(members.toSet)(equalTo(Set("two", "three")))
        },
        test("with count over non-empty set") {
          val testData = NonEmptyChunk("a", "b", "c", "d", "e")
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            _       <- redis.sAdd(key, testData.head, testData.tail: _*)
            members <- scanAll(key, None, Some(Count(3L)))
          } yield assert(members.toSet)(equalTo(testData.toSet))
        },
        test("error when not set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            value <- uuid
            _     <- redis.set(key, value)
            scan  <- redis.sScan(key, 0L).returning[String].either
          } yield assert(scan)(isLeft(isSubtype[WrongType](anything)))
        }
      )
    )

  private def scanAll(
    key: String,
    pattern: Option[String] = None,
    count: Option[Count] = None
  ): ZIO[Redis, RedisError, Chunk[String]] =
    ZStream
      .paginateChunkZIO(0L) { cursor =>
        ZIO.serviceWithZIO[Redis](_.sScan(key, cursor, pattern, count).returning[String]).map {
          case (nc, nm) if nc == 0 => (nm, None)
          case (nc, nm)            => (nm, Some(nc))
        }
      }
      .runCollect
}
