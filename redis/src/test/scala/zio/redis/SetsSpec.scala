package zio.redis

import zio.Chunk
import zio.redis.RedisError.WrongType
import zio.test._
import zio.test.Assertion._

trait SetsSpec extends BaseSpec {
  val setsSuite =
    suite("sets")(
      suite("sAdd")(
        testM("to empty set") {
          for {
            key     <- uuid
            added   <- sAdd(key)("hello")
          } yield assert(added)(equalTo(1L))
        },
        testM("to the non-empty set") {
          for {
            key     <- uuid
            _       <- sAdd(key)("hello")
            added   <- sAdd(key)("world")
          } yield assert(added)(equalTo(1L))
        },
        testM("existing element to set") {
          for {
            key     <- uuid
            _       <- sAdd(key)("hello")
            added   <- sAdd(key)("hello")
          } yield assert(added)(equalTo(0L))
        },
        testM("error when not set") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value, None, None, None)
            added <- sAdd(key)("hello").either
          } yield assert(added)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("sCard")(
        testM("non-empty set") {
          for {
            key  <- uuid
            _    <- sAdd(key)("hello")
            card <- sCard(key)
          } yield assert(card)(equalTo(1L))
        },
        testM("0 when key doesn't exist") {
          assertM(sCard("unknown"))(equalTo(0L))
        },
        testM("error when not set") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value, None, None, None)
            card  <- sCard(key).either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("sDiff")(
        testM("two non-empty sets") {
          for {
            first  <- uuid
            second <- uuid
            _      <- sAdd(first)("a", "b", "c", "d")
            _      <- sAdd(second)("a", "c")
            diff   <- sDiff(first, List(second))
          } yield assert(diff)(hasSameElements(Chunk("b", "d")))
        },
        testM("non-empty set and empty set") {
          for {
            first  <- uuid
            second <- uuid
            _      <- sAdd(first)("a", "b")
            diff   <- sDiff(first, List(second))
          } yield assert(diff)(hasSameElements(Chunk("a", "b")))
        },
        testM("empty set and non-empty set") {
          for {
            first  <- uuid
            second <- uuid
            _      <- sAdd(second)("a", "b")
            diff   <- sDiff(first, List(second))
          } yield assert(diff)(isEmpty)
        },
        testM("empty when both sets are empty") {
          for {
            first  <- uuid
            second <- uuid
            diff   <- sDiff(first, List(second))
          } yield assert(diff)(isEmpty)
        },
        testM("non-empty set with multiple non-empty sets") {
          for {
            first  <- uuid
            second <- uuid
            third  <- uuid
            _      <- sAdd(first)("a", "b", "c", "d")
            _      <- sAdd(second)("b", "d")
            _      <- sAdd(third)("b", "c")
            diff   <- sDiff(first, List(second, third))
          } yield assert(diff)(hasSameElements(Chunk("a")))
        },
        testM("error when first parameter is not set") {
          for {
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- set(first, value, None, None, None)
            diff   <- sDiff(first, List(second)).either
          } yield assert(diff)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("error when second parameter is not set") {
          for {
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- set(second, value, None, None, None)
            diff   <- sDiff(first, List(second)).either
          } yield assert(diff)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("sDiffStore")(
        testM("two non-empty sets") {
          for {
            key    <- uuid
            first  <- uuid
            second <- uuid
            _      <- sAdd(first)("a", "b", "c", "d")
            _      <- sAdd(second)("a", "c")
            card   <- sDiffStore(key)(first, second)
          } yield assert(card)(equalTo(2L))
        },
        testM("non-empty set and empty set") {
          for {
            key    <- uuid
            first  <- uuid
            second <- uuid
            _      <- sAdd(first)("a", "b")
            card   <- sDiffStore(key)(first, second)
          } yield assert(card)(equalTo(2L))
        },
        testM("empty set and non-empty set") {
          for {
            key    <- uuid
            first  <- uuid
            second <- uuid
            _      <- sAdd(second)("a", "b")
            card   <- sDiffStore(key)(first, second)
          } yield assert(card)(equalTo(0L))
        },
        testM("empty when both sets are empty") {
          for {
            key    <- uuid
            first  <- uuid
            second <- uuid
            card   <- sDiffStore(key)(first, second)
          } yield assert(card)(equalTo(0L))
        },
        testM("non-empty set with multiple non-empty sets") {
          for {
            key    <- uuid
            first  <- uuid
            second <- uuid
            third  <- uuid
            _      <- sAdd(first)("a", "b", "c", "d")
            _      <- sAdd(second)("b", "d")
            _      <- sAdd(third)("b", "c")
            card   <- sDiffStore(key)(first, second, third)
          } yield assert(card)(equalTo(1L))
        },
        testM("error when first parameter is not set") {
          for {
            key    <- uuid
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- set(first, value, None, None, None)
            card   <- sDiffStore(key)(first, second).either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("error when second parameter is not set") {
          for {
            key    <- uuid
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- set(second, value, None, None, None)
            card   <- sDiffStore(key)(first, second).either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("sInter")(
        testM("two non-empty sets") {
          for {
            first  <- uuid
            second <- uuid
            _      <- sAdd(first)("a", "b", "c", "d")
            _      <- sAdd(second)("a", "c", "e")
            inter  <- sInter(first, List(second))
          } yield assert(inter)(hasSameElements(Chunk("a", "c")))
        },
        testM("empty when one of the sets is empty") {
          for {
            first  <- uuid
            second <- uuid
            _      <- sAdd(first)("a", "b")
            inter  <- sInter(first, List(second))
          } yield assert(inter)(isEmpty)
        },
        testM("empty when both sets are empty") {
          for {
            first  <- uuid
            second <- uuid
            inter  <- sInter(first, List(second))
          } yield assert(inter)(isEmpty)
        },
        testM("non-empty set with multiple non-empty sets") {
          for {
            first  <- uuid
            second <- uuid
            third  <- uuid
            _      <- sAdd(first)("a", "b", "c", "d")
            _      <- sAdd(second)("b", "d")
            _      <- sAdd(third)("b", "c")
            inter  <- sInter(first, List(second, third))
          } yield assert(inter)(hasSameElements(Chunk("b")))
        },
        testM("error when first parameter is not set") {
          for {
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- set(first, value, None, None, None)
            inter  <- sInter(first, List(second)).either
          } yield assert(inter)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("empty with empty first set and second parameter is not set") {
          for {
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- set(second, value, None, None, None)
            inter  <- sInter(first, List(second))
          } yield assert(inter)(isEmpty)
        },
        testM("error with non-empty first set and second parameter is not set") {
          for {
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- sAdd(first)("a")
            _      <- set(second, value, None, None, None)
            inter  <- sInter(first, List(second)).either
          } yield assert(inter)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("sInterStore")(
        testM("two non-empty sets") {
          for {
            key    <- uuid
            first  <- uuid
            second <- uuid
            _      <- sAdd(first)("a", "b", "c", "d")
            _      <- sAdd(second)("a", "c", "e")
            card   <- sInterStore(key)(first, second)
          } yield assert(card)(equalTo(2L))
        },
        testM("empty when one of the sets is empty") {
          for {
            key    <- uuid
            first  <- uuid
            second <- uuid
            _      <- sAdd(first)("a", "b")
            card   <- sInterStore(key)(first, second)
          } yield assert(card)(equalTo(0L))
        },
        testM("empty when both sets are empty") {
          for {
            key    <- uuid
            first  <- uuid
            second <- uuid
            card   <- sInterStore(key)(first, second)
          } yield assert(card)(equalTo(0L))
        },
        testM("non-empty set with multiple non-empty sets") {
          for {
            key    <- uuid
            first  <- uuid
            second <- uuid
            third  <- uuid
            _      <- sAdd(first)("a", "b", "c", "d")
            _      <- sAdd(second)("b", "d")
            _      <- sAdd(third)("b", "c")
            card   <- sInterStore(key)(first, second, third)
          } yield assert(card)(equalTo(1L))
        },
        testM("error when first parameter is not set") {
          for {
            key    <- uuid
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- set(first, value, None, None, None)
            card   <- sInterStore(key)(first, second).either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("empty with empty first set and second parameter is not set") {
          for {
            key    <- uuid
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- set(second, value, None, None, None)
            card   <- sInterStore(key)(first, second)
          } yield assert(card)(equalTo(0L))
        },
        testM("error with non-empty first set and second parameter is not set") {
          for {
            key    <- uuid
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- sAdd(first)("a")
            _      <- set(second, value, None, None, None)
            card   <- sInterStore(key)(first, second).either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("sIsMember")(
        testM("actual element of the non-empty set") {
          for {
            key      <- uuid
            _        <- sAdd(key)("a", "b", "c")
            isMember <- sIsMember(key, "b")
          } yield assert(isMember)(isTrue)
        },
        testM("element that is not present in the set") {
          for {
            key      <- uuid
            _        <- sAdd(key)("a", "b", "c")
            isMember <- sIsMember(key, "unknown")
          } yield assert(isMember)(isFalse)
        },
        testM("of an empty set") {
          for {
            key      <- uuid
            isMember <- sIsMember(key, "a")
          } yield assert(isMember)(isFalse)
        },
        testM("when not set") {
          for {
            key      <- uuid
            value    <- uuid
            _        <- set(key, value, None, None, None)
            isMember <- sIsMember(key, "a").either
          } yield assert(isMember)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("sMembers")(
        testM("non-empty set") {
          for {
            key     <- uuid
            _       <- sAdd(key)("a", "b", "c")
            members <- sMembers(key)
          } yield assert(members)(hasSameElements(Chunk("a", "b", "c")))
        },
        testM("empty set") {
          for {
            key     <- uuid
            members <- sMembers(key)
          } yield assert(members)(isEmpty)
        },
        testM("when not set") {
          for {
            key     <- uuid
            value   <- uuid
            _       <- set(key, value, None, None, None)
            members <- sMembers(key).either
          } yield assert(members)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("sMove")(
        testM("from non-empty source to non-empty destination") {
          for {
            src         <- uuid
            dest        <- uuid
            _           <- sAdd(src)("a", "b", "c")
            _           <- sAdd(dest)("d", "e", "f")
            moved       <- sMove(src, dest, "a")
          } yield assert(moved)(isTrue)
        },
        testM("from non-empty source to empty destination") {
          for {
            src         <- uuid
            dest        <- uuid
            _           <- sAdd(src)("a", "b", "c")
            moved       <- sMove(src, dest, "a")
          } yield assert(moved)(isTrue)
        },
        testM("element already present in the destination") {
          for {
            src         <- uuid
            dest        <- uuid
            _           <- sAdd(src)("a", "b", "c")
            _           <- sAdd(dest)("a", "d", "e")
            moved       <- sMove(src, dest, "a")
          } yield assert(moved)(isTrue)
        },
        testM("from empty source to non-empty destination") {
          for {
            src         <- uuid
            dest        <- uuid
            _           <- sAdd(dest)("b", "c")
            moved       <- sMove(src, dest, "a")
          } yield assert(moved)(isFalse)
        },
        testM("non-existent element") {
          for {
            src         <- uuid
            dest        <- uuid
            _           <- sAdd(src)("a", "b")
            _           <- sAdd(dest)("c", "d")
            moved       <- sMove(src, dest, "unknown")
          } yield assert(moved)(isFalse)
        },
        testM("from empty source to not set destination") {
          for {
            src   <- uuid
            dest  <- uuid
            value <- uuid
            _     <- set(dest, value, None, None, None)
            moved <- sMove(src, dest, "unknown")
          } yield assert(moved)(isFalse)
        },
        testM("error when non-empty source and not set destination") {
          for {
            src   <- uuid
            dest  <- uuid
            value <- uuid
            _     <- sAdd(src)("a", "b", "c")
            _     <- set(dest, value, None, None, None)
            moved <- sMove(src, dest, "a").either
          } yield assert(moved)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("error when not set source to non-empty destination") {
          for {
            src   <- uuid
            dest  <- uuid
            value <- uuid
            _     <- set(src, value, None, None, None)
            _     <- sAdd(dest)("a", "b", "c")
            moved <- sMove(src, dest, "a").either
          } yield assert(moved)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("sPop")(
        testM("one element from non-empty set") {
          for {
            key     <- uuid
            _       <- sAdd(key)("a", "b", "c")
            poped   <- sPop(key, None)
          } yield assert(poped)(isNonEmpty)
        },
        testM("one element from an empty set") {
          for {
            key   <- uuid
            poped <- sPop(key, None)
          } yield assert(poped)(isEmpty)
        },
        testM("error when poping one element from not set") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value, None, None, None)
            poped <- sPop(key, None).either
          } yield assert(poped)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("multiple elements from non-empty set") {
          for {
            key     <- uuid
            _       <- sAdd(key)("a", "b", "c")
            poped   <- sPop(key, Some(2L))
          } yield assert(poped)(hasSize(equalTo(2)))
        },
        testM("more elements then there is in non-empty set") {
          for {
            key     <- uuid
            _       <- sAdd(key)("a", "b", "c")
            poped   <- sPop(key, Some(5L))
          } yield assert(poped)(hasSize(equalTo(3)))
        },
        testM("multiple elements from empty set") {
          for {
            key     <- uuid
            poped   <- sPop(key, Some(3))
          } yield assert(poped)(isEmpty)
        },
        testM("error when poping multiple elements from not set") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value, None, None, None)
            poped <- sPop(key, Some(3)).either
          } yield assert(poped)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("sRandMember")(
        testM("one element from non-empty set") {
          for {
            key    <- uuid
            _      <- sAdd(key)("a", "b", "c")
            member <- sRandMember(key, None)
          } yield assert(member)(hasSize(equalTo(1)))
        },
        testM("one element from an empty set") {
          for {
            key    <- uuid
            member <- sRandMember(key, None)
          } yield assert(member)(isEmpty)
        },
        testM("error when one element from not set") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- set(key, value, None, None, None)
            member <- sRandMember(key, None).either
          } yield assert(member)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("multiple elements from non-empty set") {
          for {
            key     <- uuid
            _       <- sAdd(key)("a", "b", "c")
            members <- sRandMember(key, Some(2L))
          } yield assert(members)(hasSize(equalTo(2)))
        },
        testM("more elements than there is present in the non-empty set") {
          for {
            key     <- uuid
            _       <- sAdd(key)("a", "b", "c")
            members <- sRandMember(key, Some(5L))
          } yield assert(members)(hasSize(equalTo(3)))
        },
        testM("multiple elements from an empty set") {
          for {
            key     <- uuid
            members <- sRandMember(key, Some(3L))
          } yield assert(members)(isEmpty)
        },
        testM("repeated elements from non-empty set") {
          for {
            key     <- uuid
            _       <- sAdd(key)("a", "b", "c")
            members <- sRandMember(key, Some(-5L))
          } yield assert(members)(hasSize(equalTo(5)))
        },
        testM("repeated elements from an empty set") {
          for {
            key     <- uuid
            members <- sRandMember(key, Some(-3L))
          } yield assert(members)(isEmpty)
        },
        testM("error multiple elements from not set") {
          for {
            key     <- uuid
            value   <- uuid
            _       <- set(key, value, None, None, None)
            members <- sRandMember(key, Some(3L)).either
          } yield assert(members)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("sRem")(
        testM("existing elements from non-empty set") {
          for {
            key     <- uuid
            _       <- sAdd(key)("a", "b", "c")
            removed <- sRem(key)("b", "c")
          } yield assert(removed)(equalTo(2L))
        },
        testM("when just part of elements are present in the non-empty set") {
          for {
            key     <- uuid
            _       <- sAdd(key)("a", "b", "c")
            removed <- sRem(key)("b", "d")
          } yield assert(removed)(equalTo(1L))
        },
        testM("when none of the elements are present in the non-empty set") {
          for {
            key     <- uuid
            _       <- sAdd(key)("a", "b", "c")
            removed <- sRem(key)("d", "e")
          } yield assert(removed)(equalTo(0L))
        },
        testM("elements from an empty set") {
          for {
            key     <- uuid
            removed <- sRem(key)("a", "b")
          } yield assert(removed)(equalTo(0L))
        },
        testM("elements from not set") {
          for {
            key     <- uuid
            value   <- uuid
            _       <- set(key, value, None, None, None)
            removed <- sRem(key)("a", "b").either
          } yield assert(removed)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("sUnion")(
        testM("two non-empty sets") {
          for {
            first  <- uuid
            second <- uuid
            _      <- sAdd(first)("a", "b", "c", "d")
            _      <- sAdd(second)("a", "c", "e")
            union  <- sUnion(first, List(second))
          } yield assert(union)(hasSameElements(Chunk("a", "b", "c", "d", "e")))
        },
        testM("equal to the non-empty set when the other one is empty") {
          for {
            first  <- uuid
            second <- uuid
            _      <- sAdd(first)("a", "b")
            union  <- sUnion(first, List(second))
          } yield assert(union)(hasSameElements(Chunk("a", "b")))
        },
        testM("empty when both sets are empty") {
          for {
            first  <- uuid
            second <- uuid
            union  <- sUnion(first, List(second))
          } yield assert(union)(isEmpty)
        },
        testM("non-empty set with multiple non-empty sets") {
          for {
            first  <- uuid
            second <- uuid
            third  <- uuid
            _      <- sAdd(first)("a", "b", "c", "d")
            _      <- sAdd(second)("b", "d")
            _      <- sAdd(third)("b", "c", "e")
            union  <- sUnion(first, List(second, third))
          } yield assert(union)(hasSameElements(Chunk("a", "b", "c", "d", "e")))
        },
        testM("error when first parameter is not set") {
          for {
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- set(first, value, None, None, None)
            union  <- sUnion(first, List(second)).either
          } yield assert(union)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("error when the first parameter is set and the second parameter is not set") {
          for {
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- sAdd(first)("a")
            _      <- set(second, value, None, None, None)
            union  <- sUnion(first, List(second)).either
          } yield assert(union)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("sUnionStore")(
        testM("two non-empty sets") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- sAdd(first)("a", "b", "c", "d")
            _      <- sAdd(second)("a", "c", "e")
            card   <- sUnionStore(dest)(first, second)
          } yield assert(card)(equalTo(5L))
        },
        testM("equal to the non-empty set when the other one is empty") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- sAdd(first)("a", "b")
            card   <- sUnionStore(dest)(first, second)
          } yield assert(card)(equalTo(2L))
        },
        testM("empty when both sets are empty") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            card   <- sUnionStore(dest)(first, second)
          } yield assert(card)(equalTo(0L))
        },
        testM("non-empty set with multiple non-empty sets") {
          for {
            first  <- uuid
            second <- uuid
            third  <- uuid
            dest   <- uuid
            _      <- sAdd(first)("a", "b", "c", "d")
            _      <- sAdd(second)("b", "d")
            _      <- sAdd(third)("b", "c", "e")
            card   <- sUnionStore(dest)(first, second, third)
          } yield assert(card)(equalTo(5L))
        },
        testM("error when the first parameter is not set") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            value  <- uuid
            _      <- set(first, value, None, None, None)
            card   <- sUnionStore(dest)(first, second).either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("error when the first parameter is set and the second parameter is not set") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            value  <- uuid
            _      <- sAdd(first)("a")
            _      <- set(second, value, None, None, None)
            card   <- sUnionStore(dest)(first, second).either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("sScan")(
        testM("non-empty set") {
          for {
            key <- uuid
            _ <- sAdd(key)("a", "b", "c")
            scan <- sScan(key, 0L, None, None)
            (cursor, members) = scan
          } yield assert(cursor)(isNonEmptyString) &&
            assert(members)(isNonEmpty)
        },
        testM("empty set") {
          for {
            key <- uuid
            scan <- sScan(key, 0L, None, None)
            (cursor, members) = scan
          } yield assert(cursor)(equalTo("0")) &&
            assert(members)(isEmpty)
        },
        testM("with match over non-empty set") {
          for {
            key <- uuid
            _ <- sAdd(key)("one", "two", "three")
            scan <- sScan(key, 0L, Some("t[a-z]*".r), None)
            (cursor, members) = scan
          } yield assert(cursor)(isNonEmptyString) &&
            assert(members)(isNonEmpty)
        },
        testM("with count over non-empty set") {
          for {
            key <- uuid
            _ <- sAdd(key)("a", "b", "c", "d", "e")
            scan <- sScan(key, 0L, None, Some(Count(3L)))
            (cursor, members) = scan
          } yield assert(cursor)(isNonEmptyString) &&
            assert(members)(isNonEmpty)
        },
        testM("error when not set") {
          for {
            key <- uuid
            value <- uuid
            _ <- set(key, value, None, None, None)
            scan <- sScan(key, 0L, None, None).either
          } yield assert(scan)(isLeft(isSubtype[WrongType](anything)))
        }
      )
    )
}
