package zio.redis

import zio.redis.RedisError.{ ProtocolError, WrongType }
import zio.redis.Output.StringOutput
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
            added <- zAdd()(key, None, None, None, (MemberScore(1d, value), Nil))
          } yield assert(added)(equalTo(1L))
        },
        testM("to the non-empty set") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- zAdd()(key, None, None, None, (MemberScore(1d, value), Nil))
            value2 <- uuid
            added  <- zAdd()(key, None, None, None, (MemberScore(2d, value2), Nil))
          } yield assert(added)(equalTo(1L))
        },
        testM("existing element to set") {
          for {
            key   <- uuid
            value <- uuid
            _     <- zAdd()(key, None, None, None, (MemberScore(1d, value), Nil))
            added <- zAdd()(key, None, None, None, (MemberScore(2d, value), Nil))
          } yield assert(added)(equalTo(0L))
        },
        testM("multiple elements to set") {
          for {
            key   <- uuid
            added <- zAdd()(
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
            added <- zAdd()(key, None, None, None, (MemberScore(1d, value), Nil)).either
          } yield assert(added)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("NX - do not to update existing members, only add new") {
          for {
            key    <- uuid
            _      <- zAdd()(key, None, None, None, (MemberScore(1d, "v1"), Nil))
            _      <- zAdd()(key, None, None, None, (MemberScore(2d, "v2"), Nil))
            added  <- zAdd()(key, Some(Update.SetNew), None, None, (MemberScore(3d, "v3"), List(MemberScore(22d, "v2"))))
            result <- zRange(key, Range(0, -1), None)
          } yield assert(added)(equalTo(1L)) &&
            assert(result.toList)(equalTo(List("v1", "v2", "v3")))
        },
        testM("XX - update existing members, not add new") {
          for {
            key    <- uuid
            _      <- zAdd()(key, None, None, None, (MemberScore(1d, "v1"), Nil))
            _      <- zAdd()(key, None, None, None, (MemberScore(2d, "v2"), Nil))
            added  <-
              zAdd()(key, Some(Update.SetExisting), None, None, (MemberScore(3d, "v3"), List(MemberScore(11d, "v1"))))
            result <- zRange(key, Range(0, -1), None)
          } yield assert(added)(equalTo(0L)) &&
            assert(result.toList)(equalTo(List("v2", "v1")))
        },
        testM("CH - return number of new and updated members") {
          for {
            key    <- uuid
            _      <- zAdd()(key, None, None, None, (MemberScore(1d, "v1"), Nil))
            _      <- zAdd()(key, None, None, None, (MemberScore(2d, "v2"), Nil))
            added  <- zAdd()(key, None, Some(Changed), None, (MemberScore(3d, "v3"), List(MemberScore(11d, "v1"))))
            result <- zRange(key, Range(0, -1), None)
          } yield assert(added)(equalTo(2L)) &&
            assert(result.toList)(equalTo(List("v2", "v3", "v1")))
        },
        testM("INCR - increment by score") {
          for {
            key      <- uuid
            _        <- zAdd()(key, None, None, None, (MemberScore(1d, "v1"), Nil))
            _        <- zAdd()(key, None, None, None, (MemberScore(2d, "v2"), Nil))
            newScore <- zAdd(StringOutput)(key, None, None, Some(Increment), (MemberScore(3d, "v1"), Nil))
            result   <- zRange(key, Range(0, -1), None)
          } yield assert(newScore)(equalTo("4")) &&
            assert(result.toList)(equalTo(List("v2", "v1")))
        }
      ),
      suite("zCard")(
        testM("non-empty set") {
          for {
            key  <- uuid
            _    <- zAdd()(key, None, None, None, (MemberScore(1d, "hello"), List(MemberScore(2d, "world"))))
            card <- zCard(key)
          } yield assert(card)(equalTo(2L))
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
      suite("zCount")(
        testM("non-empty set") {
          for {
            key   <- uuid
            _     <- zAdd()(
                   key,
                   None,
                   None,
                   None,
                   (
                     MemberScore(1d, "a"),
                     List(
                       MemberScore(2d, "b"),
                       MemberScore(3d, "c"),
                       MemberScore(4d, "d"),
                       MemberScore(5d, "e")
                     )
                   )
                 )
            count <- zCount(key, Range(0, 3))
          } yield assert(count)(equalTo(3L))
        },
        testM("empty set") {
          for {
            key   <- uuid
            count <- zCount(key, Range(0, 3))
          } yield assert(count)(equalTo(0L))
        }
      ),
      suite("zIncrBy")(
        testM("non-empty set") {
          for {
            key     <- uuid
            _       <- zAdd()(
                   key,
                   None,
                   None,
                   None,
                   (
                     MemberScore(1d, "a"),
                     List(
                       MemberScore(2d, "b"),
                       MemberScore(3d, "c"),
                       MemberScore(4d, "d"),
                       MemberScore(5d, "e")
                     )
                   )
                 )
            incrRes <- zIncrBy(key, 10, "a")
            count   <- zCount(key, Range(10, 11))
          } yield assert(count)(equalTo(1L)) && assert(incrRes)(equalTo(11.0))
        },
        testM("empty set") {
          for {
            key     <- uuid
            incrRes <- zIncrBy(key, 10, "a")
            count   <- zCount(key, Range(0, -1))
          } yield assert(count)(equalTo(0L)) && assert(incrRes)(equalTo(10.0))
        }
      ),
      suite("zInterStore")(
        testM("two non-empty sets") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            _      <- zAdd()(
                   first,
                   None,
                   None,
                   None,
                   (MemberScore(1d, "a"), List(MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d")))
                 )
            _      <-
              zAdd()(second, None, None, None, (MemberScore(1d, "a"), List(MemberScore(3d, "c"), MemberScore(5d, "e"))))
            card   <- zInterStore(s"out_$dest", (first, List(second)), None, None)
          } yield assert(card)(equalTo(2L))
        },
        testM("empty when one of the sets is empty") {
          for {
            dest     <- uuid
            nonEmpty <- uuid
            empty    <- uuid
            _        <- zAdd()(
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
            _      <- zAdd()(
                   first,
                   None,
                   None,
                   None,
                   (MemberScore(1d, "a"), List(MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d")))
                 )
            _      <- zAdd()(
                   second,
                   None,
                   None,
                   None,
                   (MemberScore(2d, "b"), List(MemberScore(2d, "b"), MemberScore(4d, "d")))
                 )
            _      <- zAdd()(
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
        testM("error with empty first set and second parameter is not set") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- set(second, value, None, None, None)
            card   <- zInterStore(dest, (first, List(second)), None, None).either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("error with non-empty first set and second parameter is not set") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- zAdd()(
                   first,
                   None,
                   None,
                   None,
                   (MemberScore(1d, "a"), Nil)
                 )
            _      <- set(second, value, None, None, None)
            card   <- zInterStore(dest, (first, List(second)), None, None).either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("parameter weights provided") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- zAdd()(
                   first,
                   None,
                   None,
                   None,
                   (MemberScore(5d, "M"), List(MemberScore(6d, "N"), MemberScore(7d, "O")))
                 )
            _      <- zAdd()(
                   second,
                   None,
                   None,
                   None,
                   (MemberScore(3d, "N"), List(MemberScore(2d, "O"), MemberScore(4d, "P")))
                 )
            card   <- zInterStore(dest, (first, List(second)), None, Some(Weights(Seq(2, 3))))
          } yield assert(card)(equalTo(2L))
        },
        testM("error when invalid weights provided ( less than sets number )") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- zAdd()(
                   first,
                   None,
                   None,
                   None,
                   (MemberScore(5d, "M"), List(MemberScore(6d, "N"), MemberScore(7d, "O")))
                 )
            _      <- zAdd()(
                   second,
                   None,
                   None,
                   None,
                   (MemberScore(3d, "N"), List(MemberScore(2d, "O"), MemberScore(4d, "P")))
                 )
            card   <- zInterStore(dest, (first, List(second)), None, Some(Weights(Seq(2)))).either
          } yield assert(card)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when invalid weights provided ( more than sets number )") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- zAdd()(
                   first,
                   None,
                   None,
                   None,
                   (MemberScore(5d, "M"), List(MemberScore(6d, "N"), MemberScore(7d, "O")))
                 )
            _      <- zAdd()(
                   second,
                   None,
                   None,
                   None,
                   (MemberScore(3d, "N"), List(MemberScore(2d, "O"), MemberScore(4d, "P")))
                 )
            card   <- zInterStore(dest, (first, List(second)), None, Some(Weights(Seq(2, 3, 5)))).either
          } yield assert(card)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("set aggregate parameter MAX") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- zAdd()(
                   first,
                   None,
                   None,
                   None,
                   (MemberScore(5d, "M"), List(MemberScore(6d, "N"), MemberScore(7d, "O")))
                 )
            _      <- zAdd()(
                   second,
                   None,
                   None,
                   None,
                   (MemberScore(3d, "N"), List(MemberScore(2d, "O"), MemberScore(4d, "P")))
                 )
            card   <- zInterStore(dest, (first, List(second)), Some(Aggregate.Max), None)
          } yield assert(card)(equalTo(2L))
        },
        testM("set aggregate parameter MIN") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- zAdd()(
                   first,
                   None,
                   None,
                   None,
                   (MemberScore(5d, "M"), List(MemberScore(6d, "N"), MemberScore(7d, "O")))
                 )
            _      <- zAdd()(
                   second,
                   None,
                   None,
                   None,
                   (MemberScore(3d, "N"), List(MemberScore(2d, "O"), MemberScore(4d, "P")))
                 )
            card   <- zInterStore(dest, (first, List(second)), Some(Aggregate.Min), None)
          } yield assert(card)(equalTo(2L))
        }
      ),
      suite("zLexCount")(
        testM("non-empty set") {
          for {
            key   <- uuid
            _     <- zAdd()(
                   key,
                   None,
                   None,
                   None,
                   (
                     MemberScore(1d, "Delhi"),
                     List(
                       MemberScore(2d, "Mumbai"),
                       MemberScore(3d, "London"),
                       MemberScore(4d, "Paris"),
                       MemberScore(5d, "Tokyo")
                     )
                   )
                 )
            count <- zLexCount(key, LexRange(min = LexMinimum.Closed("London"), max = LexMaximum.Open("Paris")))
          } yield assert(count)(equalTo(2L))
        },
        testM("empty set") {
          for {
            key   <- uuid
            count <- zLexCount(key, LexRange(min = LexMinimum.Closed("London"), max = LexMaximum.Open("Paris")))
          } yield assert(count)(equalTo(0L))
        }
      ),
      suite("zPopMax")(
        testM("non-empty set")(
          for {
            key    <- uuid
            _      <- zAdd()(
                   key,
                   None,
                   None,
                   None,
                   (
                     MemberScore(1d, "Delhi"),
                     List(
                       MemberScore(2d, "Mumbai"),
                       MemberScore(3d, "London"),
                       MemberScore(4d, "Paris"),
                       MemberScore(5d, "Tokyo")
                     )
                   )
                 )
            result <- zPopMax(key, None)
          } yield assert(result.toList)(equalTo(List("Tokyo", "5")))
        ),
        testM("non-empty set with count param")(
          for {
            key    <- uuid
            _      <- zAdd()(
                   key,
                   None,
                   None,
                   None,
                   (
                     MemberScore(1d, "Delhi"),
                     List(
                       MemberScore(2d, "Mumbai"),
                       MemberScore(3d, "London"),
                       MemberScore(4d, "Paris"),
                       MemberScore(5d, "Tokyo")
                     )
                   )
                 )
            result <- zPopMax(key, Some(3))
          } yield assert(result.toList)(equalTo(List("Tokyo", "5", "Paris", "4", "London", "3")))
        ),
        testM("empty set")(for {
          key    <- uuid
          result <- zPopMax(key, None)
        } yield assert(result.toList)(equalTo(Nil)))
      ),
      suite("zPopMin")(
        testM("non-empty set")(
          for {
            key    <- uuid
            _      <- zAdd()(
                   key,
                   None,
                   None,
                   None,
                   (
                     MemberScore(1d, "Delhi"),
                     List(
                       MemberScore(2d, "Mumbai"),
                       MemberScore(3d, "London"),
                       MemberScore(4d, "Paris"),
                       MemberScore(5d, "Tokyo")
                     )
                   )
                 )
            result <- zPopMin(key, None)
          } yield assert(result.toList)(equalTo(List("Delhi", "1")))
        ),
        testM("non-empty set with count param")(
          for {
            key    <- uuid
            _      <- zAdd()(
                   key,
                   None,
                   None,
                   None,
                   (
                     MemberScore(1d, "Delhi"),
                     List(
                       MemberScore(2d, "Mumbai"),
                       MemberScore(3d, "London"),
                       MemberScore(4d, "Paris"),
                       MemberScore(5d, "Tokyo")
                     )
                   )
                 )
            result <- zPopMin(key, Some(3))
          } yield assert(result.toList)(equalTo(List("Delhi", "1", "Mumbai", "2", "London", "3")))
        ),
        testM("empty set")(for {
          key    <- uuid
          result <- zPopMin(key, None)
        } yield assert(result.toList)(equalTo(Nil)))
      ),
      suite("zRange")(
        testM("non-empty set") {
          for {
            key    <- uuid
            _      <- zAdd()(
                   key,
                   None,
                   None,
                   None,
                   (
                     MemberScore(1d, "Delhi"),
                     List(
                       MemberScore(2d, "Mumbai"),
                       MemberScore(3d, "London"),
                       MemberScore(4d, "Paris"),
                       MemberScore(5d, "Tokyo")
                     )
                   )
                 )
            result <- zRange(key, Range(0, -1), None)
          } yield assert(result.toList)(equalTo(List("Delhi", "Mumbai", "London", "Paris", "Tokyo")))
        },
        testM("non-empty set, with scores") {
          for {
            key    <- uuid
            _      <- zAdd()(
                   key,
                   None,
                   None,
                   None,
                   (
                     MemberScore(1d, "Delhi"),
                     List(
                       MemberScore(2d, "Mumbai"),
                       MemberScore(3d, "London"),
                       MemberScore(4d, "Paris"),
                       MemberScore(5d, "Tokyo")
                     )
                   )
                 )
            result <- zRange(key, Range(0, -1), Some(WithScores))
          } yield assert(result.toList)(
            equalTo(List("Delhi", "1", "Mumbai", "2", "London", "3", "Paris", "4", "Tokyo", "5"))
          )
        },
        testM("empty set") {
          for {
            key    <- uuid
            result <- zRange(key, Range(0, -1), None)
          } yield assert(result.toList)(isEmpty)
        }
      ),
      suite("zRangeByLex")(
        testM("non-empty set") {
          for {
            key    <- uuid
            _      <- zAdd()(
                   key,
                   None,
                   None,
                   None,
                   (
                     MemberScore(1d, "Delhi"),
                     List(
                       MemberScore(2d, "London"),
                       MemberScore(3d, "Paris"),
                       MemberScore(4d, "Tokyo"),
                       MemberScore(5d, "NewYork"),
                       MemberScore(6d, "Seoul")
                     )
                   )
                 )
            result <-
              zRangeByLex(key, LexRange(min = LexMinimum.Open("London"), max = LexMaximum.Closed("Seoul")), None)
          } yield assert(result.toList)(
            equalTo(List("Paris"))
          )
        },
        testM("non-empty set with limit") {
          for {
            key    <- uuid
            _      <- zAdd()(
                   key,
                   None,
                   None,
                   None,
                   (
                     MemberScore(1d, "Delhi"),
                     List(
                       MemberScore(2d, "London"),
                       MemberScore(3d, "Paris"),
                       MemberScore(4d, "Tokyo"),
                       MemberScore(5d, "NewYork"),
                       MemberScore(6d, "Seoul")
                     )
                   )
                 )
            result <-
              zRangeByLex(key, LexRange(min = LexMinimum.Unbounded, max = LexMaximum.Unbounded), Some(Limit(2, 3)))
          } yield assert(result.toList)(
            equalTo(List("Paris", "Tokyo", "NewYork"))
          )
        },
        testM("empty set") {
          for {
            key    <- uuid
            result <- zRangeByLex(key, LexRange(min = LexMinimum.Open("A"), max = LexMaximum.Closed("Z")), None)
          } yield assert(result.toList)(
            isEmpty
          )
        }
      ),
      suite("zRangeByScore")(
        testM("non-empty set") {
          for {
            key    <- uuid
            _      <- zAdd()(
                   key,
                   None,
                   None,
                   None,
                   (
                     MemberScore(1556d, "Samsung"),
                     List(
                       MemberScore(2000d, "Nokia"),
                       MemberScore(1800d, "Micromax"),
                       MemberScore(2200d, "Sunsui"),
                       MemberScore(1800d, "MicroSoft"),
                       MemberScore(2500d, "LG")
                     )
                   )
                 )
            result <- zRangeByScore(
                        key,
                        ScoreRange(ScoreMinimum.Open(1500), ScoreMaximum.Closed(1900)),
                        None,
                        None
                      )
          } yield assert(result.toList)(
            equalTo(List("Samsung", "MicroSoft", "Micromax"))
          )
        },
        testM("non-empty set, with scores") {
          for {
            key    <- uuid
            _      <- zAdd()(
                   key,
                   None,
                   None,
                   None,
                   (
                     MemberScore(1556d, "Samsung"),
                     List(
                       MemberScore(2000d, "Nokia"),
                       MemberScore(1800d, "Micromax"),
                       MemberScore(2200d, "Sunsui"),
                       MemberScore(1800d, "MicroSoft"),
                       MemberScore(2500d, "LG")
                     )
                   )
                 )
            result <- zRangeByScore(
                        key,
                        ScoreRange(ScoreMinimum.Open(1500), ScoreMaximum.Closed(1900)),
                        Some(WithScores),
                        None
                      )
          } yield assert(result.toList)(
            equalTo(List("Samsung", "1556", "MicroSoft", "1800", "Micromax", "1800"))
          )
        },
        testM("non-empty set, with limit") {
          for {
            key    <- uuid
            _      <- zAdd()(
                   key,
                   None,
                   None,
                   None,
                   (
                     MemberScore(1556d, "Samsung"),
                     List(
                       MemberScore(2000d, "Nokia"),
                       MemberScore(1800d, "Micromax"),
                       MemberScore(2200d, "Sunsui"),
                       MemberScore(1800d, "MicroSoft"),
                       MemberScore(2500d, "LG")
                     )
                   )
                 )
            result <- zRangeByScore(
                        key,
                        ScoreRange(ScoreMinimum.Open(1500), ScoreMaximum.Closed(2500)),
                        None,
                        Some(Limit(offset = 1, count = 3))
                      )
          } yield assert(result.toList)(
            equalTo(List("MicroSoft", "Micromax", "Nokia"))
          )
        },
        testM("empty set") {
          for {
            key    <- uuid
            result <- zRangeByScore(
                        key,
                        ScoreRange(ScoreMinimum.Open(1500), ScoreMaximum.Closed(1900)),
                        None,
                        None
                      )
          } yield assert(result.toList)(isEmpty)
        }
      ),
      suite("zRank")(
        testM("existing elements from non-empty set") {
          for {
            key  <- uuid
            _    <- zAdd()(
                   key,
                   None,
                   None,
                   None,
                   (MemberScore(1d, "a"), List(MemberScore(2d, "b"), MemberScore(3d, "c")))
                 )
            rank <- zRank(key, "c")
          } yield assert(rank)(equalTo(Some(2L)))
        },
        testM("empty set") {
          for {
            key  <- uuid
            rank <- zRank(key, "c")
          } yield assert(rank)(isNone)
        }
      ),
      suite("zRem")(
        testM("existing elements from non-empty set") {
          for {
            key     <- uuid
            _       <- zAdd()(
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
            _       <- zAdd()(
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
            _       <- zAdd()(
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
      suite("zRemRangeByLex")(
        testM("non-empty set") {
          for {
            key         <- uuid
            _           <- zAdd()(
                   key,
                   None,
                   None,
                   None,
                   (
                     MemberScore(0d, "Delhi"),
                     List(
                       MemberScore(0d, "Mumbai"),
                       MemberScore(0d, "Hyderabad"),
                       MemberScore(0d, "Kolkata"),
                       MemberScore(0d, "Chennai")
                     )
                   )
                 )
            remResult   <- zRemRangeByLex(
                           key,
                           LexRange(min = LexMinimum.Open("Hyderabad"), max = LexMaximum.Closed("Mumbai"))
                         )
            rangeResult <- zRangeByLex(key, LexRange(min = LexMinimum.Unbounded, max = LexMaximum.Unbounded), None)
          } yield assert(rangeResult.toList)(
            equalTo(List("Chennai", "Delhi", "Hyderabad"))
          ) && assert(remResult)(equalTo(2L))
        },
        testM("empty set") {
          for {
            key       <- uuid
            remResult <- zRemRangeByLex(
                           key,
                           LexRange(min = LexMinimum.Open("Hyderabad"), max = LexMaximum.Closed("Mumbai"))
                         )
          } yield assert(remResult)(equalTo(0L))
        }
      ),
      suite("zRemRangeByRank")(
        testM("non-empty set") {
          for {
            key         <- uuid
            _           <- zAdd()(
                   key,
                   None,
                   None,
                   None,
                   (
                     MemberScore(1d, "Delhi"),
                     List(
                       MemberScore(2d, "Mumbai"),
                       MemberScore(3d, "Hyderabad"),
                       MemberScore(4d, "Kolkata"),
                       MemberScore(5d, "Chennai")
                     )
                   )
                 )
            remResult   <- zRemRangeByRank(
                           key,
                           Range(1, 2)
                         )
            rangeResult <- zRange(key, Range(0, -1), None)
          } yield assert(rangeResult.toList)(
            equalTo(List("Delhi", "Kolkata", "Chennai"))
          ) && assert(remResult)(equalTo(2L))
        },
        testM("empty set") {
          for {
            key       <- uuid
            remResult <- zRemRangeByRank(
                           key,
                           Range(1, 2)
                         )
          } yield assert(remResult)(equalTo(0L))
        }
      ),
      suite("zRemRangeByScore")(
        testM("non-empty set") {
          for {
            key         <- uuid
            _           <- zAdd()(
                   key,
                   None,
                   None,
                   None,
                   (
                     MemberScore(80d, "Delhi"),
                     List(
                       MemberScore(60d, "Mumbai"),
                       MemberScore(70d, "Hyderabad"),
                       MemberScore(50d, "Kolkata"),
                       MemberScore(65d, "Chennai")
                     )
                   )
                 )
            remResult   <- zRemRangeByScore(
                           key,
                           ScoreRange(min = ScoreMinimum.Infinity, max = ScoreMaximum.Open(70))
                         )
            rangeResult <-
              zRangeByScore(key, ScoreRange(min = ScoreMinimum.Infinity, max = ScoreMaximum.Infinity), None, None)
          } yield assert(rangeResult.toList)(
            equalTo(List("Hyderabad", "Delhi"))
          ) && assert(remResult)(equalTo(3L))
        },
        testM("empty set") {
          for {
            key       <- uuid
            remResult <- zRemRangeByScore(
                           key,
                           ScoreRange(min = ScoreMinimum.Infinity, max = ScoreMaximum.Open(70))
                         )
          } yield assert(remResult)(equalTo(0L))
        }
      ),
      suite("zRevRange")(
        testM("non-empty set") {
          for {
            key       <- uuid
            _         <- zAdd()(
                   key,
                   None,
                   None,
                   None,
                   (
                     MemberScore(80d, "Delhi"),
                     List(
                       MemberScore(60d, "Mumbai"),
                       MemberScore(70d, "Hyderabad"),
                       MemberScore(50d, "Kolkata"),
                       MemberScore(65d, "Chennai")
                     )
                   )
                 )
            revResult <- zRevRange(
                           key,
                           Range(0, 1),
                           None
                         )
          } yield assert(revResult.toList)(
            equalTo(List("Delhi", "Hyderabad"))
          )
        },
        testM("non-empty set with scores") {
          for {
            key       <- uuid
            _         <- zAdd()(
                   key,
                   None,
                   None,
                   None,
                   (
                     MemberScore(80d, "Delhi"),
                     List(
                       MemberScore(60d, "Mumbai"),
                       MemberScore(70d, "Hyderabad"),
                       MemberScore(50d, "Kolkata"),
                       MemberScore(65d, "Chennai")
                     )
                   )
                 )
            revResult <- zRevRange(
                           key,
                           Range(0, 1),
                           Some(WithScores)
                         )
          } yield assert(revResult.toList)(
            equalTo(List("Delhi", "80", "Hyderabad", "70"))
          )
        },
        testM("empty set") {
          for {
            key       <- uuid
            remResult <- zRevRange(
                           key,
                           Range(0, -1),
                           None
                         )
          } yield assert(remResult.toList)(isEmpty)
        }
      ),
      suite("zRevRangeByLex")(
        testM("non-empty set") {
          for {
            key         <- uuid
            _           <- zAdd()(
                   key,
                   None,
                   None,
                   None,
                   (
                     MemberScore(0d, "Delhi"),
                     List(
                       MemberScore(0d, "London"),
                       MemberScore(0d, "Paris"),
                       MemberScore(0d, "Tokyo"),
                       MemberScore(0d, "NewYork"),
                       MemberScore(0d, "Seoul")
                     )
                   )
                 )
            rangeResult <- zRevRangeByLex(
                             key,
                             LexRange(min = LexMinimum.Open("Seoul"), max = LexMaximum.Closed("Delhi")),
                             None
                           )
          } yield assert(rangeResult.toList)(
            equalTo(List("Paris", "NewYork", "London", "Delhi"))
          )
        },
        testM("non-empty set with limit") {
          for {
            key         <- uuid
            _           <- zAdd()(
                   key,
                   None,
                   None,
                   None,
                   (
                     MemberScore(0d, "Delhi"),
                     List(
                       MemberScore(0d, "London"),
                       MemberScore(0d, "Paris"),
                       MemberScore(0d, "Tokyo"),
                       MemberScore(0d, "NewYork"),
                       MemberScore(0d, "Seoul")
                     )
                   )
                 )
            rangeResult <- zRevRangeByLex(
                             key,
                             LexRange(min = LexMinimum.Open("Seoul"), max = LexMaximum.Closed("Delhi")),
                             Some(Limit(offset = 1, count = 2))
                           )
          } yield assert(rangeResult.toList)(
            equalTo(List("NewYork", "London"))
          )
        },
        testM("empty set") {
          for {
            key         <- uuid
            rangeResult <- zRevRangeByLex(
                             key,
                             LexRange(min = LexMinimum.Open("Hyderabad"), max = LexMaximum.Closed("Mumbai")),
                             None
                           )
          } yield assert(rangeResult)(isEmpty)
        }
      ),
      suite("zRevRangeByScore")(
        testM("non-empty set") {
          for {
            key         <- uuid
            _           <- zAdd()(
                   key,
                   None,
                   None,
                   None,
                   (
                     MemberScore(1556d, "Samsung"),
                     List(
                       MemberScore(2000d, "Nokia"),
                       MemberScore(1800d, "Micromax"),
                       MemberScore(2200d, "Sunsui"),
                       MemberScore(1800d, "MicroSoft"),
                       MemberScore(2500d, "LG")
                     )
                   )
                 )
            rangeResult <- zRevRangeByScore(
                             key,
                             //ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
                             ScoreRange(
                               min = ScoreMinimum.Open(2500),
                               max = ScoreMaximum.Closed(2000)
                             ), //TODO min <-> max
                             None,
                             None
                           )
          } yield assert(rangeResult.toList)(
            equalTo(List("Sunsui", "Nokia"))
          )
        },
        testM("non-empty set with scores") {
          for {
            key         <- uuid
            _           <- zAdd()(
                   key,
                   None,
                   None,
                   None,
                   (
                     MemberScore(1556d, "Samsung"),
                     List(
                       MemberScore(2000d, "Nokia"),
                       MemberScore(1800d, "Micromax"),
                       MemberScore(2200d, "Sunsui"),
                       MemberScore(1800d, "MicroSoft"),
                       MemberScore(2500d, "LG")
                     )
                   )
                 )
            rangeResult <- zRevRangeByScore(
                             key,
                             //ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
                             ScoreRange(
                               min = ScoreMinimum.Open(2500),
                               max = ScoreMaximum.Closed(2000)
                             ), //TODO min <-> max
                             Some(WithScores),
                             None
                           )
          } yield assert(rangeResult.toList)(
            equalTo(List("Sunsui", "2200", "Nokia", "2000"))
          )
        },
        testM("non-empty set with limit") {
          for {
            key         <- uuid
            _           <- zAdd()(
                   key,
                   None,
                   None,
                   None,
                   (
                     MemberScore(1556d, "Samsung"),
                     List(
                       MemberScore(2000d, "Nokia"),
                       MemberScore(1800d, "Micromax"),
                       MemberScore(2200d, "Sunsui"),
                       MemberScore(1800d, "MicroSoft"),
                       MemberScore(2500d, "LG")
                     )
                   )
                 )
            rangeResult <- zRevRangeByScore(
                             key,
                             //ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
                             ScoreRange(
                               min = ScoreMinimum.Open(2500),
                               max = ScoreMaximum.Closed(2000)
                             ), //TODO min <-> max
                             None,
                             Some(Limit(1, 2))
                           )
          } yield assert(rangeResult.toList)(
            equalTo(List("Nokia"))
          )
        },
        testM("empty set") {
          for {
            key         <- uuid
            rangeResult <- zRevRangeByScore(
                             key,
                             ScoreRange(
                               min = ScoreMinimum.Open(2500),
                               max = ScoreMaximum.Closed(2000)
                             ),
                             None,
                             None
                           )
          } yield assert(rangeResult)(isEmpty)
        }
      ),
      suite("zRevRank")(
        testM("non-empty set") {
          for {
            key    <- uuid
            _      <- zAdd()(
                   key,
                   None,
                   None,
                   None,
                   (
                     MemberScore(10d, "Delhi"),
                     List(
                       MemberScore(20d, "Mumbai"),
                       MemberScore(30d, "Hyderabad"),
                       MemberScore(40d, "Kolkata"),
                       MemberScore(50d, "Chennai")
                     )
                   )
                 )
            result <- zRevRank(
                        key,
                        "Hyderabad"
                      )
          } yield assert(result)(equalTo(Some(2L)))
        },
        testM("empty set") {
          for {
            key    <- uuid
            result <- zRevRank(
                        key,
                        "Hyderabad"
                      )
          } yield assert(result)(equalTo(None))
        }
      ),
      suite("zScan")(
        testM("non-empty set") {
          for {
            key              <- uuid
            _                <- zAdd()(
                   key,
                   None,
                   None,
                   None,
                   (MemberScore(1d, "atest"), List(MemberScore(2d, "btest"), MemberScore(3d, "ctest")))
                 )
            scan             <- zScan(key, 0L, None, None)
            (cursor, members) = scan
          } yield assert(cursor)(isNonEmptyString) &&
            assert(members)(isNonEmpty)
        },
        testM("empty set") {
          for {
            key              <- uuid
            scan             <- zScan(key, 0L, None, None)
            (cursor, members) = scan
          } yield assert(cursor)(equalTo("0")) &&
            assert(members)(isEmpty)
        },
        testM("with match over non-empty set") {
          for {
            key              <- uuid
            _                <- zAdd()(
                   key,
                   None,
                   None,
                   None,
                   (MemberScore(1d, "one"), List(MemberScore(2d, "two"), MemberScore(3d, "three")))
                 )
            scan             <- zScan(key, 0L, Some("t[a-z]*".r), None)
            (cursor, members) = scan
          } yield assert(cursor)(isNonEmptyString) &&
            assert(members)(isNonEmpty)
        },
        testM("with count over non-empty set") {
          for {
            key              <- uuid
            _                <- zAdd()(
                   key,
                   None,
                   None,
                   None,
                   (
                     MemberScore(1d, "a"),
                     List(MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"), MemberScore(5d, "e"))
                   )
                 )
            scan             <- zScan(key, 0L, None, Some(Count(3L)))
            (cursor, members) = scan
          } yield assert(cursor)(isNonEmptyString) &&
            assert(members)(isNonEmpty)
        },
        testM("match with count over non-empty set") {
          for {
            key              <- uuid
            _                <- zAdd()(
                   key,
                   None,
                   None,
                   None,
                   (
                     MemberScore(1d, "testa"),
                     List(
                       MemberScore(2d, "testb"),
                       MemberScore(3d, "testc"),
                       MemberScore(4d, "testd"),
                       MemberScore(5d, "teste")
                     )
                   )
                 )
            scan             <- zScan(key, 0L, Some("t[a-z]*".r), Some(Count(3L)))
            (cursor, members) = scan
          } yield assert(cursor)(isNonEmptyString) &&
            assert(members)(isNonEmpty)
        },
        testM("error when not set") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value, None, None, None)
            scan  <- zScan(key, 0L, None, None).either
          } yield assert(scan)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("zScore")(
        testM("non-empty set") {
          for {
            key    <- uuid
            _      <- zAdd()(
                   key,
                   None,
                   None,
                   None,
                   (
                     MemberScore(10d, "Delhi"),
                     List(
                       MemberScore(20d, "Mumbai"),
                       MemberScore(30d, "Hyderabad"),
                       MemberScore(40d, "Kolkata"),
                       MemberScore(50d, "Chennai")
                     )
                   )
                 )
            result <- zScore(
                        key,
                        "Delhi"
                      )
          } yield assert(result)(equalTo(Some("10")))
        },
        testM("empty set") {
          for {
            key    <- uuid
            result <- zScore(
                        key,
                        "Hyderabad"
                      )
          } yield assert(result)(isNone)
        }
      ),
      suite("zUnionStore")(
        testM("two non-empty sets") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- zAdd()(
                   first,
                   None,
                   None,
                   None,
                   (MemberScore(1d, "a"), List(MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d")))
                 )
            _      <- zAdd()(
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
            _        <- zAdd()(
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
            _      <- zAdd()(
                   first,
                   None,
                   None,
                   None,
                   (MemberScore(1d, "a"), List(MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d")))
                 )
            _      <- zAdd()(
                   second,
                   None,
                   None,
                   None,
                   (MemberScore(2, "b"), List(MemberScore(4d, "d")))
                 )
            _      <- zAdd()(
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
            _      <- zAdd()(
                   first,
                   None,
                   None,
                   None,
                   (MemberScore(1, "a"), Nil)
                 )
            _      <- set(second, value, None, None, None)
            card   <- zUnionStore(dest, (first, List(second)), None, None).either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("parameter weights provided") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- zAdd()(
                   first,
                   None,
                   None,
                   None,
                   (MemberScore(5d, "M"), List(MemberScore(6d, "N"), MemberScore(7d, "O")))
                 )
            _      <- zAdd()(
                   second,
                   None,
                   None,
                   None,
                   (MemberScore(3d, "N"), List(MemberScore(2d, "O"), MemberScore(4d, "P")))
                 )
            card   <- zUnionStore(dest, (first, List(second)), Some(Weights(Seq(2, 3))), None)
          } yield assert(card)(equalTo(4L))
        },
        testM("error when invalid weights provided ( less than sets number )") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- zAdd()(
                   first,
                   None,
                   None,
                   None,
                   (MemberScore(5d, "M"), List(MemberScore(6d, "N"), MemberScore(7d, "O")))
                 )
            _      <- zAdd()(
                   second,
                   None,
                   None,
                   None,
                   (MemberScore(3d, "N"), List(MemberScore(2d, "O"), MemberScore(4d, "P")))
                 )
            card   <- zUnionStore(dest, (first, List(second)), Some(Weights(Seq(2))), None).either
          } yield assert(card)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when invalid weights provided ( more than sets number )") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- zAdd()(
                   first,
                   None,
                   None,
                   None,
                   (MemberScore(5d, "M"), List(MemberScore(6d, "N"), MemberScore(7d, "O")))
                 )
            _      <- zAdd()(
                   second,
                   None,
                   None,
                   None,
                   (MemberScore(3d, "N"), List(MemberScore(2d, "O"), MemberScore(4d, "P")))
                 )
            card   <- zUnionStore(dest, (first, List(second)), Some(Weights(Seq(2, 3, 5))), None).either
          } yield assert(card)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("set aggregate parameter MAX") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- zAdd()(
                   first,
                   None,
                   None,
                   None,
                   (MemberScore(5d, "M"), List(MemberScore(6d, "N"), MemberScore(7d, "O")))
                 )
            _      <- zAdd()(
                   second,
                   None,
                   None,
                   None,
                   (MemberScore(3d, "N"), List(MemberScore(2d, "O"), MemberScore(4d, "P")))
                 )
            card   <- zUnionStore(dest, (first, List(second)), None, Some(Aggregate.Max))
          } yield assert(card)(equalTo(4L))
        },
        testM("set aggregate parameter MIN") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- zAdd()(
                   first,
                   None,
                   None,
                   None,
                   (MemberScore(5d, "M"), List(MemberScore(6d, "N"), MemberScore(7d, "O")))
                 )
            _      <- zAdd()(
                   second,
                   None,
                   None,
                   None,
                   (MemberScore(3d, "N"), List(MemberScore(2d, "O"), MemberScore(4d, "P")))
                 )
            card   <- zUnionStore(dest, (first, List(second)), None, Some(Aggregate.Min))
          } yield assert(card)(equalTo(4L))
        },
        testM("parameter weights provided along with aggregate") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- zAdd()(
                   first,
                   None,
                   None,
                   None,
                   (MemberScore(5d, "M"), List(MemberScore(6d, "N"), MemberScore(7d, "O")))
                 )
            _      <- zAdd()(
                   second,
                   None,
                   None,
                   None,
                   (MemberScore(3d, "N"), List(MemberScore(2d, "O"), MemberScore(4d, "P")))
                 )
            card   <- zUnionStore(dest, (first, List(second)), Some(Weights(Seq(2, 3))), Some(Aggregate.Max))
          } yield assert(card)(equalTo(4L))
        }
      )
    )
}
