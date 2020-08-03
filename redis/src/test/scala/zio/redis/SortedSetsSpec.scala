package zio.redis

import zio.redis.RedisError.WrongType
import zio.test.Assertion._
import zio.test._

trait SortedSetsSpec extends BaseSpec {
  val sortedSetsSuite =
    suite("sorted sets")(
      suite("zAdd")(
        testM("to empty set") {
          for {
            key   <- uuid
            value <- uuid
            added <- zAdd(key, None, None, None, (MemberScore(1d, value), Nil))
          } yield assert(added)(equalTo(1L))
        },
        testM("to the non-empty set") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- zAdd(key, None, None, None, (MemberScore(1d, value), Nil))
            value2 <- uuid
            added  <- zAdd(key, None, None, None, (MemberScore(2d, value2), Nil))
          } yield assert(added)(equalTo(1L))
        },
        testM("existing element to set") {
          for {
            key   <- uuid
            value <- uuid
            _     <- zAdd(key, None, None, None, (MemberScore(1d, value), Nil))
            added <- zAdd(key, None, None, None, (MemberScore(2d, value), Nil))
          } yield assert(added)(equalTo(0L))
        },
        testM("multiple elements to set") {
          for {
            key   <- uuid
            added <- zAdd(
                       key,
                       None,
                       None,
                       None,
                       (MemberScore(1d, "a"), List(MemberScore(2d, "b"), MemberScore(3d, "c")))
                     )
          } yield assert(added)(equalTo(3L))
        },
        testM("error when not set") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value, None, None, None)
            added <- zAdd(key, None, None, None, (MemberScore(1d, value), Nil)).either
          } yield assert(added)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("zCard")(
        testM("non-empty set") {
          for {
            key  <- uuid
            _    <- zAdd(key, None, None, None, (MemberScore(1d, "hello"), Nil))
            card <- zCard(key)
          } yield assert(card)(equalTo(1L))
        },
        testM("0 when key doesn't exist") {
          assertM(zCard("unknown"))(equalTo(0L))
        },
        testM("error when not set") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value, None, None, None)
            card  <- zCard(key).either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("zInterStore")(
        testM("two non-empty sets") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            _      <- zAdd(
                   first,
                   None,
                   None,
                   None,
                   (MemberScore(1d, "a"), List(MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d")))
                 )
            _      <-
              zAdd(second, None, None, None, (MemberScore(1d, "a"), List(MemberScore(3d, "c"), MemberScore(5d, "e"))))
            card   <- zInterStore(s"out_$dest", (first, List(second)), None, None)
          } yield assert(card)(equalTo(2L))
        },
        testM("empty when one of the sets is empty") {
          for {
            dest     <- uuid
            nonEmpty <- uuid
            empty    <- uuid
            _        <- zAdd(
                   nonEmpty,
                   None,
                   None,
                   None,
                   (MemberScore(1d, "a"), List(MemberScore(2d, "b")))
                 )
            card     <- zInterStore(dest, (nonEmpty, List(empty)), None, None)
          } yield assert(card)(equalTo(0L))
        },
        testM("empty when both sets are empty") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            card   <- zInterStore(dest, (first, List(second)), None, None)
          } yield assert(card)(equalTo(0L))
        },
        testM("non-empty set with multiple non-empty sets") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            third  <- uuid
            _      <- zAdd(
                   first,
                   None,
                   None,
                   None,
                   (MemberScore(1d, "a"), List(MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d")))
                 )
            _      <- zAdd(
                   second,
                   None,
                   None,
                   None,
                   (MemberScore(2d, "b"), List(MemberScore(2d, "b"), MemberScore(4d, "d")))
                 )
            _      <- zAdd(
                   third,
                   None,
                   None,
                   None,
                   (MemberScore(1d, "a"), List(MemberScore(2d, "b"), MemberScore(3d, "c")))
                 )
            card   <- zInterStore(dest, (first, List(second, third)), None, None)
          } yield assert(card)(equalTo(1L))
        },
        testM("error when first parameter is not set") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- set(first, value, None, None, None)
            card   <- zInterStore(dest, (first, List(second)), None, None).either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("empty with empty first set and second parameter is not set") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- set(second, value, None, None, None)
            card   <- zInterStore(dest, (first, List(second)), None, None)
          } yield assert(card)(equalTo(0L))
        },
        testM("error with non-empty first set and second parameter is not set") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- zAdd(
                   first,
                   None,
                   None,
                   None,
                   (MemberScore(1d, "a"), Nil)
                 )
            _      <- set(second, value, None, None, None)
            card   <- zInterStore(dest, (first, List(second)), None, None).either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        }
      ) @@ TestAspect.ignore,
      suite("zRem")(
        testM("existing elements from non-empty set") {
          for {
            key     <- uuid
            _       <- zAdd(
                   key,
                   None,
                   None,
                   None,
                   (MemberScore(1d, "a"), List(MemberScore(2d, "b"), MemberScore(3d, "c")))
                 )
            removed <- zRem(key)("b", "c")
          } yield assert(removed)(equalTo(2L))
        },
        testM("when just part of elements are present in the non-empty set") {
          for {
            key     <- uuid
            _       <- zAdd(
                   key,
                   None,
                   None,
                   None,
                   (MemberScore(1d, "a"), List(MemberScore(2d, "b"), MemberScore(3d, "c")))
                 )
            removed <- zRem(key)("b", "d")
          } yield assert(removed)(equalTo(1L))
        },
        testM("when none of the elements are present in the non-empty set") {
          for {
            key     <- uuid
            _       <- zAdd(
                   key,
                   None,
                   None,
                   None,
                   (MemberScore(1d, "a"), List(MemberScore(2d, "b"), MemberScore(3d, "c")))
                 )
            removed <- zRem(key)("d", "e")
          } yield assert(removed)(equalTo(0L))
        },
        testM("elements from an empty set") {
          for {
            key     <- uuid
            removed <- zRem(key)("a", "b")
          } yield assert(removed)(equalTo(0L))
        },
        testM("elements from not set") {
          for {
            key     <- uuid
            value   <- uuid
            _       <- set(key, value, None, None, None)
            removed <- zRem(key)("a", "b").either
          } yield assert(removed)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("zUnionStore")(
        testM("two non-empty sets") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- zAdd(
                   first,
                   None,
                   None,
                   None,
                   (MemberScore(1d, "a"), List(MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d")))
                 )
            _      <- zAdd(
                   second,
                   None,
                   None,
                   None,
                   (MemberScore(1d, "a"), List(MemberScore(3d, "c"), MemberScore(5d, "e")))
                 )
            card   <- zUnionStore(dest, (first, List(second)), None, None)
          } yield assert(card)(equalTo(5L))
        },
        testM("equal to the non-empty set when the other one is empty") {
          for {
            nonEmpty <- uuid
            empty    <- uuid
            dest     <- uuid
            _        <- zAdd(
                   nonEmpty,
                   None,
                   None,
                   None,
                   (MemberScore(1d, "a"), List(MemberScore(2d, "b")))
                 )
            card     <- zUnionStore(dest, (nonEmpty, List(empty)), None, None)
          } yield assert(card)(equalTo(2L))
        },
        testM("empty when both sets are empty") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            card   <- zUnionStore(dest, (first, List(second)), None, None)
          } yield assert(card)(equalTo(0L))
        },
        testM("non-empty set with multiple non-empty sets") {
          for {
            first  <- uuid
            second <- uuid
            third  <- uuid
            dest   <- uuid
            _      <- zAdd(
                   first,
                   None,
                   None,
                   None,
                   (MemberScore(1d, "a"), List(MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d")))
                 )
            _      <- zAdd(
                   second,
                   None,
                   None,
                   None,
                   (MemberScore(2, "b"), List(MemberScore(4d, "d")))
                 )
            _      <- zAdd(
                   third,
                   None,
                   None,
                   None,
                   (MemberScore(2, "b"), List(MemberScore(3d, "c"), MemberScore(5d, "e")))
                 )
            card   <- zUnionStore(dest, (first, List(second, third)), None, None)
          } yield assert(card)(equalTo(5L))
        },
        testM("error when the first parameter is not set") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            value  <- uuid
            _      <- set(first, value, None, None, None)
            card   <- zUnionStore(dest, (first, List(second)), None, None).either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("error when the first parameter is set and the second parameter is not set") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            value  <- uuid
            _      <- zAdd(
                   first,
                   None,
                   None,
                   None,
                   (MemberScore(1, "a"), Nil)
                 )
            _      <- set(second, value, None, None, None)
            card   <- zUnionStore(dest, (first, List(second)), None, None).either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        }
      ) @@ TestAspect.ignore,
      suite("zScan")(
        testM("non-empty set") {
          for {
            key              <- uuid
            _                <- zAdd(
                   key,
                   None,
                   None,
                   None,
                   (MemberScore(1d, "atest"), List(MemberScore(2d, "btest"), MemberScore(3d, "ctest")))
                 )
            scan             <- zScan(0L, Some("test".r), Some(0L), None)
            (cursor, members) = scan
          } yield assert(cursor)(isNonEmptyString) &&
            assert(members)(isNonEmpty)
        },
        testM("empty set") {
          for {
            key              <- uuid
            scan             <- sScan(key, 0L, None, None)
            (cursor, members) = scan
          } yield assert(cursor)(equalTo("0")) &&
            assert(members)(isEmpty)
        },
        testM("with match over non-empty set") {
          for {
            key              <- uuid
            _                <- zAdd(
                   key,
                   None,
                   None,
                   None,
                   (MemberScore(1d, "one"), List(MemberScore(2d, "two"), MemberScore(3d, "three")))
                 )
            scan             <- sScan(key, 0L, Some("t[a-z]*".r), None)
            (cursor, members) = scan
          } yield assert(cursor)(isNonEmptyString) &&
            assert(members)(isNonEmpty)
        },
        testM("with count over non-empty set") {
          for {
            key              <- uuid
            _                <- zAdd(
                   key,
                   None,
                   None,
                   None,
                   (
                     MemberScore(1d, "a"),
                     List(MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"), MemberScore(5d, "e"))
                   )
                 )
            scan             <- sScan(key, 0L, None, Some(Count(3L)))
            (cursor, members) = scan
          } yield assert(cursor)(isNonEmptyString) &&
            assert(members)(isNonEmpty)
        },
        testM("error when not set") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value, None, None, None)
            scan  <- sScan(key, 0L, None, None).either
          } yield assert(scan)(isLeft(isSubtype[WrongType](anything)))
        }
      ) @@ TestAspect.ignore
    )
}
