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
      ),
      suite("store intersection")(
        testM("sInterStore two non-empty sets") {
          for {
            key <- uuid
            first <- uuid
            second <- uuid
            _ <- sAdd(first)("a", "b", "c", "d")
            _ <- sAdd(second)("a", "c", "e")
            card <- sInterStore(key)(first, second)
            inter <- sMembers(key)
          } yield assert(card)(equalTo(2L)) &&
              assert(inter)(hasSameElements(Chunk("a", "c")))
        },
        testM("sInterStore empty when one of the sets is empty") {
          for {
            key <- uuid
            first <- uuid
            second <- uuid
            _ <- sAdd(first)("a", "b")
            card <- sInterStore(key)(first, second)
            inter <- sMembers(key)
          } yield assert(card)(equalTo(0L)) &&
              assert(inter)(isEmpty)
        },
        testM("sInterStore empty when both sets are empty") {
          for {
            key <- uuid
            first <- uuid
            second <- uuid
            card <- sInterStore(key)(first, second)
            inter <- sMembers(key)
          } yield assert(card)(equalTo(0L)) &&
              assert(inter)(isEmpty)
        },
        testM("sInterStore non-empty set with multiple non-empty sets") {
          for {
            key <- uuid
            first <- uuid
            second <- uuid
            third <- uuid
            _ <- sAdd(first)("a", "b", "c", "d")
            _ <- sAdd(second)("b", "d")
            _ <- sAdd(third)("b", "c")
            card <- sInterStore(key)(first, second, third)
            inter <- sMembers(key)
          } yield assert(card)(equalTo(1L)) &&
              assert(inter)(hasSameElements(Chunk("b")))
        },
        testM("sInterStore error when first parameter is not set") {
          for {
            key <- uuid
            first <- uuid
            second <- uuid
            value <- uuid
            _ <- set(first, value, None, None, None)
            card <- sInterStore(key)(first, second).either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("sInterStore empty with empty first set and second parameter is not set") {
          for {
            key <- uuid
            first <- uuid
            second <- uuid
            value <- uuid
            _ <- set(second, value, None, None, None)
            card <- sInterStore(key)(first, second)
            inter <- sMembers(key)
          } yield assert(card)(equalTo(0L)) &&
              assert(inter)(isEmpty)
        },
        testM("sInterStore error with non-empty first set and second parameter is not set") {
          for {
            key <- uuid
            first <- uuid
            second <- uuid
            value <- uuid
            _ <- sAdd(first)("a")
            _ <- set(second, value, None, None, None)
            card <- sInterStore(key)(first, second).either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("is member")(
        testM("sIsMember actual element of the non-empty set") {
          for {
            key <- uuid
            _ <- sAdd(key)("a", "b", "c")
            isMember <- sIsMember(key, "b")
          } yield assert(isMember)(isTrue)
        },
        testM("sIsMember element that is not present in the set") {
          for {
            key <- uuid
            _ <- sAdd(key)("a", "b", "c")
            isMember <- sIsMember(key, "unknown")
          } yield assert(isMember)(isFalse)
        },
        testM("sIsMember of an empty set") {
          for {
            key <- uuid
            isMember <- sIsMember(key, "a")
          } yield assert(isMember)(isFalse)
        },
        testM("sIsMember when not set") {
          for {
            key <- uuid
            value <- uuid
            _ <- set(key, value, None, None, None)
            isMember <- sIsMember(key, "a").either
          } yield assert(isMember)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("members")(
        testM("sMembers non-empty set") {
          for {
            key <- uuid
            _ <- sAdd(key)("a", "b", "c")
            members <- sMembers(key)
          } yield assert(members)(hasSameElements(Chunk("a", "b", "c")))
        },
        testM("sMembers empty set") {
          for {
            key <- uuid
            members <- sMembers(key)
          } yield assert(members)(isEmpty)
        },
        testM("sMembers when not set") {
          for {
            key <- uuid
            value <- uuid
            _ <- set(key, value, None, None, None)
            members <- sMembers(key).either
          } yield assert(members)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("move")(
        testM("sMove from non-empty source to non-empty destination") {
          for {
            src <- uuid
            dest <- uuid
            _ <- sAdd(src)("a", "b", "c")
            _ <- sAdd(dest)("d", "e", "f")
            moved <- sMove(src, dest, "a")
            srcMembers <- sMembers(src)
            destMembers <- sMembers(dest)
          } yield assert(moved)(isTrue) &&
              assert(srcMembers)(hasSameElements(Chunk("b", "c"))) &&
              assert(destMembers)(hasSameElements(Chunk("a", "d", "e", "f")))
        },
        testM("sMove from non-empty source to empty destination") {
          for {
            src <- uuid
            dest <- uuid
            _ <- sAdd(src)("a", "b", "c")
            moved <- sMove(src, dest, "a")
            srcMembers <- sMembers(src)
            destMembers <- sMembers(dest)
          } yield assert(moved)(isTrue) &&
            assert(srcMembers)(hasSameElements(Chunk("b", "c"))) &&
            assert(destMembers)(hasSameElements(Chunk("a")))
        },
        testM("sMove element already present in the destination") {
          for {
            src <- uuid
            dest <- uuid
            _ <- sAdd(src)("a", "b", "c")
            _ <- sAdd(dest)("a", "d", "e")
            moved <- sMove(src, dest, "a")
            srcMembers <- sMembers(src)
            destMembers <- sMembers(dest)
          } yield assert(moved)(isTrue) &&
              assert(srcMembers)(hasSameElements(Chunk("b", "c"))) &&
              assert(destMembers)(hasSameElements(Chunk("a", "d", "e")))
        },
        testM("sMove from empty source to non-empty destination") {
          for {
            src <- uuid
            dest <- uuid
            _ <- sAdd(dest)("b", "c")
            moved <- sMove(src, dest, "a")
            srcMembers <- sMembers(src)
            destMembers <- sMembers(dest)
          } yield assert(moved)(isFalse) &&
            assert(srcMembers)(isEmpty) &&
            assert(destMembers)(hasSameElements(Chunk("b", "c")))
        },
        testM("sMove non-existent element") {
          for {
            src <- uuid
            dest <- uuid
            _ <- sAdd(src)("a", "b")
            _ <- sAdd(dest)("c", "d")
            moved <- sMove(src, dest, "unknown")
            srcMembers <- sMembers(src)
            destMembers <- sMembers(dest)
          } yield assert(moved)(isFalse) &&
              assert(srcMembers)(hasSameElements(Chunk("a", "b"))) &&
              assert(destMembers)(hasSameElements(Chunk("c", "d")))
        },
        testM("sMove from empty source to not set destination") {
          for {
            src <- uuid
            dest <- uuid
            value <- uuid
            _ <- set(dest, value, None, None, None)
            moved <- sMove(src, dest, "unknown")
          } yield assert(moved)(isFalse)
        },
        testM("sMove error when non-empty source and not set destination") {
          for {
            src <- uuid
            dest <- uuid
            value <- uuid
            _ <- sAdd(src)("a", "b", "c")
            _ <- set(dest, value, None, None, None)
            moved <- sMove(src, dest, "a").either
          } yield assert(moved)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("sMove error when not set source to non-empty destination") {
          for {
            src <- uuid
            dest <- uuid
            value <- uuid
            _ <- set(src, value, None, None, None)
            _ <- sAdd(dest)("a", "b", "c")
            moved <- sMove(src, dest, "a").either
          } yield assert(moved)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("pop")(
        testM("sPop one element from non-empty set") {
          for {
            key <- uuid
            _ <- sAdd(key)("a", "b", "c")
            poped <- sPop(key, None)
            members <- sMembers(key)
          } yield assert(poped)(isSome) &&
              assert(members)(hasSize(equalTo(2)))
        },
        testM("sPop one element from an empty set") {
          for {
            key <- uuid
            poped <- sPop(key, None)
          } yield assert(poped)(isNone)
        },
        testM("sPop error when poping one element from not set") {
          for {
            key <- uuid
            value <- uuid
            _ <- set(key, value, None, None, None)
            poped <- sPop(key, None).either
          } yield assert(poped)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("sPop multiple elements from non-empty set") {
          for {
            key <- uuid
            _ <- sAdd(key)("a", "b", "c")
            poped <- sPop(key, Some(2)).either
            members <- sMembers(key)
          } yield assert(poped)(isRight(isSome)) &&
              assert(members)(hasSize(equalTo(1)))
        },
        testM("sPop more elements then there is in non-empty set") {
          for {
            key <- uuid
            _ <- sAdd(key)("a", "b", "c")
            poped <- sPop(key, Some(5)).either
            members <- sMembers(key)
          } yield assert(poped)(isRight(isSome)) &&
              assert(members)(isEmpty)
        },
        testM("sPop multiple elements from empty set") {
          for {
            key <- uuid
            poped <- sPop(key, Some(3)).either
            members <- sMembers(key)
          } yield assert(poped)(isRight(isNone)) &&
              assert(members)(isEmpty)
        },
        testM("sPop error when poping multiple elements from not set") {
          for {
            key <- uuid
            value <- uuid
            _ <- set(key, value, None, None, None)
            poped <- sPop(key, Some(3)).either
          } yield assert(poped)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("random member")(
        testM("sRandMember one element from non-empty set") {
          for {
            key <- uuid
            _ <- sAdd(key)("a", "b", "c")
            member <- sRandMember(key, None).either
          } yield assert(member)(isRight(hasSize(equalTo(1))))
        },
        testM("sRandMember one element from an empty set") {
          for {
            key <- uuid
            member <- sRandMember(key, None).either
          } yield assert(member)(isRight(isEmpty))
        },
        testM("sRandMember one element from not set") {
          for {
            key <- uuid
            value <- uuid
            _ <- set(key, value, None, None, None)
            member <- sRandMember(key, None).either
          } yield assert(member)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("sRandMember multiple elements from non-empty set") {
          for {
            key <- uuid
            _ <- sAdd(key)("a", "b", "c")
            members <- sRandMember(key, Some(2L))
          } yield assert(members)(hasSize(equalTo(2)))
        },
        testM("sRandMember more elements than there is present in the non-empty set") {
          for {
            key <- uuid
            _ <- sAdd(key)("a", "b", "c")
            members <- sRandMember(key, Some(5L))
          } yield assert(members)(hasSize(equalTo(3)))
        },
        testM("sRandMember multiple elements from an empty set") {
          for {
            key <- uuid
            members <- sRandMember(key, Some(3L))
          } yield assert(members)(isEmpty)
        },
        testM("sRandMember repeated elements from non-empty set") {
          for {
            key <- uuid
            _ <- sAdd(key)("a", "b", "c")
            members <- sRandMember(key, Some(-5L))
          } yield assert(members)(hasSize(equalTo(5)))
        },
        testM("sRandMember repeated elements from an empty set") {
          for {
            key <- uuid
            members <- sRandMember(key, Some(-3L))
          } yield assert(members)(isEmpty)
        },
        testM("sRandMember error multiple elements from not set") {
          for {
            key <- uuid
            value <- uuid
            _ <- set(key, value, None, None, None)
            members <- sRandMember(key, Some(3L)).either
          } yield assert(members)(isLeft(isSubtype[WrongType](anything)))
        }
      )
    )
}
