package zio.redis

import scala.util.matching.Regex

import zio.Chunk
import zio.ZIO
import zio.redis.RedisError.{ ProtocolError, WrongType }
import zio.stream.ZStream
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
            added <- zAdd(key)(MemberScore(1d, value))
          } yield assert(added)(equalTo(1L))
        },
        testM("to the non-empty set") {
          for {
            key    <- uuid
            value  <- uuid
            _      <- zAdd(key)(MemberScore(1d, value))
            value2 <- uuid
            added  <- zAdd(key)(MemberScore(2d, value2))
          } yield assert(added)(equalTo(1L))
        },
        testM("existing element to set") {
          for {
            key   <- uuid
            value <- uuid
            _     <- zAdd(key)(MemberScore(1d, value))
            added <- zAdd(key)(MemberScore(2d, value))
          } yield assert(added)(equalTo(0L))
        },
        testM("multiple elements to set") {
          for {
            key   <- uuid
            added <- zAdd(key)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"))
          } yield assert(added)(equalTo(3L))
        },
        testM("error when not set") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value)
            added <- zAdd(key)(MemberScore(1d, value)).either
          } yield assert(added)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("NX - do not to update existing members, only add new") {
          for {
            key    <- uuid
            _      <- zAdd(key)(MemberScore(1d, "v1"))
            _      <- zAdd(key)(MemberScore(2d, "v2"))
            added  <- zAdd(key, Some(Update.SetNew))(MemberScore(3d, "v3"), MemberScore(22d, "v2"))
            result <- zRange(key, 0 to -1)
          } yield assert(added)(equalTo(1L)) &&
            assert(result.toList)(equalTo(List("v1", "v2", "v3")))
        },
        testM("XX - update existing members, not add new") {
          for {
            key    <- uuid
            _      <- zAdd(key)(MemberScore(1d, "v1"))
            _      <- zAdd(key)(MemberScore(2d, "v2"))
            added  <- zAdd(key, Some(Update.SetExisting))(MemberScore(3d, "v3"), MemberScore(11d, "v1"))
            result <- zRange(key, 0 to -1)
          } yield assert(added)(equalTo(0L)) &&
            assert(result.toList)(equalTo(List("v2", "v1")))
        },
        testM("CH - return number of new and updated members") {
          for {
            key    <- uuid
            _      <- zAdd(key)(MemberScore(1d, "v1"))
            _      <- zAdd(key)(MemberScore(2d, "v2"))
            added  <- zAdd(key, change = Some(Changed))(MemberScore(3d, "v3"), MemberScore(11d, "v1"))
            result <- zRange(key, 0 to -1)
          } yield assert(added)(equalTo(2L)) &&
            assert(result.toList)(equalTo(List("v2", "v3", "v1")))
        },
        testM("INCR - increment by score") {
          for {
            key      <- uuid
            _        <- zAdd(key)(MemberScore(1d, "v1"))
            _        <- zAdd(key)(MemberScore(2d, "v2"))
            newScore <- zAddWithIncr(key)(Increment, MemberScore(3d, "v1"))
            result   <- zRange(key, 0 to -1)
          } yield assert(newScore)(equalTo(Some(4.0))) &&
            assert(result.toList)(equalTo(List("v2", "v1")))
        }
      ),
      suite("zCard")(
        testM("non-empty set") {
          for {
            key  <- uuid
            _    <- zAdd(key)(MemberScore(1d, "hello"), MemberScore(2d, "world"))
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
            _     <- set(key, value)
            card  <- zCard(key).either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("zCount")(
        testM("non-empty set") {
          for {
            key   <- uuid
            _     <- zAdd(key)(
                   MemberScore(1d, "a"),
                   MemberScore(2d, "b"),
                   MemberScore(3d, "c"),
                   MemberScore(4d, "d"),
                   MemberScore(5d, "e")
                 )
            count <- zCount(key, 0 to 3)
          } yield assert(count)(equalTo(3L))
        },
        testM("empty set") {
          for {
            key   <- uuid
            count <- zCount(key, 0 to 3)
          } yield assert(count)(equalTo(0L))
        }
      ),
      suite("zIncrBy")(
        testM("non-empty set") {
          for {
            key     <- uuid
            _       <- zAdd(key)(
                   MemberScore(1d, "a"),
                   MemberScore(2d, "b"),
                   MemberScore(3d, "c"),
                   MemberScore(4d, "d"),
                   MemberScore(5d, "e")
                 )
            incrRes <- zIncrBy(key, 10, "a")
            count   <- zCount(key, 10 to 11)
          } yield assert(count)(equalTo(1L)) && assert(incrRes)(equalTo(11.0))
        },
        testM("empty set") {
          for {
            key     <- uuid
            incrRes <- zIncrBy(key, 10, "a")
            count   <- zCount(key, 0 to -1)
          } yield assert(count)(equalTo(0L)) && assert(incrRes)(equalTo(10.0))
        }
      ),
      suite("zInterStore")(
        testM("two non-empty sets") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            _      <- zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            _      <- zAdd(second)(MemberScore(1d, "a"), MemberScore(3d, "c"), MemberScore(5d, "e"))
            card   <- zInterStore(s"out_$dest", 2, first, second)()
          } yield assert(card)(equalTo(2L))
        },
        testM("empty when one of the sets is empty") {
          for {
            dest     <- uuid
            nonEmpty <- uuid
            empty    <- uuid
            _        <- zAdd(nonEmpty)(MemberScore(1d, "a"), MemberScore(2d, "b"))
            card     <- zInterStore(dest, 2, nonEmpty, empty)()
          } yield assert(card)(equalTo(0L))
        },
        testM("empty when both sets are empty") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            card   <- zInterStore(dest, 2, first, second)()
          } yield assert(card)(equalTo(0L))
        },
        testM("non-empty set with multiple non-empty sets") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            third  <- uuid
            _      <- zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            _      <- zAdd(second)(MemberScore(2d, "b"), MemberScore(2d, "b"), MemberScore(4d, "d"))
            _      <- zAdd(third)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"))
            card   <- zInterStore(dest, 3, first, second, third)()
          } yield assert(card)(equalTo(1L))
        },
        testM("error when first parameter is not set") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- set(first, value)
            card   <- zInterStore(dest, 2, first, second)().either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("error with empty first set and second parameter is not set") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- set(second, value)
            card   <- zInterStore(dest, 2, first, second)().either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("error with non-empty first set and second parameter is not set") {
          for {
            dest   <- uuid
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- zAdd(first)(MemberScore(1d, "a"))
            _      <- set(second, value)
            card   <- zInterStore(dest, 2, first, second)().either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("parameter weights provided") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _      <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            card   <- zInterStore(dest, 2, first, second)(weights = Some(::(2.0, 3.0 :: Nil)))
          } yield assert(card)(equalTo(2L))
        },
        testM("error when invalid weights provided ( less than sets number )") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _      <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            card   <- zInterStore(dest, 2, first, second)(weights = Some(::(2, Nil))).either
          } yield assert(card)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when invalid weights provided ( more than sets number )") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _      <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            card   <- zInterStore(dest, 2, first, second)(weights = Some(::(2.0, List(3.0, 5.0)))).either
          } yield assert(card)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("set aggregate parameter MAX") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _      <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            card   <- zInterStore(dest, 2, first, second)(Some(Aggregate.Max))
          } yield assert(card)(equalTo(2L))
        },
        testM("set aggregate parameter MIN") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _      <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            card   <- zInterStore(dest, 2, first, second)(Some(Aggregate.Min))
          } yield assert(card)(equalTo(2L))
        }
      ),
      suite("zLexCount")(
        testM("non-empty set") {
          for {
            key   <- uuid
            _     <- zAdd(key)(
                   MemberScore(1d, "Delhi"),
                   MemberScore(2d, "Mumbai"),
                   MemberScore(3d, "London"),
                   MemberScore(4d, "Paris"),
                   MemberScore(5d, "Tokyo")
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
            _      <- zAdd(key)(
                   MemberScore(1d, "Delhi"),
                   MemberScore(2d, "Mumbai"),
                   MemberScore(3d, "London"),
                   MemberScore(4d, "Paris"),
                   MemberScore(5d, "Tokyo")
                 )
            result <- zPopMax(key)
          } yield assert(result.toList)(equalTo(List("Tokyo", "5")))
        ),
        testM("non-empty set with count param")(
          for {
            key    <- uuid
            _      <- zAdd(key)(
                   MemberScore(1d, "Delhi"),
                   MemberScore(2d, "Mumbai"),
                   MemberScore(3d, "London"),
                   MemberScore(4d, "Paris"),
                   MemberScore(5d, "Tokyo")
                 )
            result <- zPopMax(key, Some(3))
          } yield assert(result.toList)(equalTo(List("Tokyo", "5", "Paris", "4", "London", "3")))
        ),
        testM("empty set")(for {
          key    <- uuid
          result <- zPopMax(key)
        } yield assert(result.toList)(equalTo(Nil)))
      ),
      suite("zPopMin")(
        testM("non-empty set")(
          for {
            key    <- uuid
            _      <- zAdd(key)(
                   MemberScore(1d, "Delhi"),
                   MemberScore(2d, "Mumbai"),
                   MemberScore(3d, "London"),
                   MemberScore(4d, "Paris"),
                   MemberScore(5d, "Tokyo")
                 )
            result <- zPopMin(key)
          } yield assert(result.toList)(equalTo(List("Delhi", "1")))
        ),
        testM("non-empty set with count param")(
          for {
            key    <- uuid
            _      <- zAdd(key)(
                   MemberScore(1d, "Delhi"),
                   MemberScore(2d, "Mumbai"),
                   MemberScore(3d, "London"),
                   MemberScore(4d, "Paris"),
                   MemberScore(5d, "Tokyo")
                 )
            result <- zPopMin(key, Some(3))
          } yield assert(result.toList)(equalTo(List("Delhi", "1", "Mumbai", "2", "London", "3")))
        ),
        testM("empty set")(for {
          key    <- uuid
          result <- zPopMin(key)
        } yield assert(result.toList)(equalTo(Nil)))
      ),
      suite("zRange")(
        testM("non-empty set") {
          for {
            key    <- uuid
            _      <- zAdd(key)(
                   MemberScore(1d, "Delhi"),
                   MemberScore(2d, "Mumbai"),
                   MemberScore(3d, "London"),
                   MemberScore(4d, "Paris"),
                   MemberScore(5d, "Tokyo")
                 )
            result <- zRange(key, 0 to -1)
          } yield assert(result.toList)(equalTo(List("Delhi", "Mumbai", "London", "Paris", "Tokyo")))
        },
        testM("non-empty set, with scores") {
          for {
            key    <- uuid
            _      <- zAdd(key)(
                   MemberScore(1d, "Delhi"),
                   MemberScore(2d, "Mumbai"),
                   MemberScore(3d, "London"),
                   MemberScore(4d, "Paris"),
                   MemberScore(5d, "Tokyo")
                 )
            result <- zRange(key, 0 to -1, Some(WithScores))
          } yield assert(result.toList)(
            equalTo(List("Delhi", "1", "Mumbai", "2", "London", "3", "Paris", "4", "Tokyo", "5"))
          )
        },
        testM("empty set") {
          for {
            key    <- uuid
            result <- zRange(key, 0 to -1)
          } yield assert(result.toList)(isEmpty)
        }
      ),
      suite("zRangeByLex")(
        testM("non-empty set") {
          for {
            key    <- uuid
            _      <- zAdd(key)(
                   MemberScore(1d, "Delhi"),
                   MemberScore(2d, "London"),
                   MemberScore(3d, "Paris"),
                   MemberScore(4d, "Tokyo"),
                   MemberScore(5d, "NewYork"),
                   MemberScore(6d, "Seoul")
                 )
            result <- zRangeByLex(key, LexRange(min = LexMinimum.Open("London"), max = LexMaximum.Closed("Seoul")))
          } yield assert(result.toList)(equalTo(List("Paris")))
        },
        testM("non-empty set with limit") {
          for {
            key    <- uuid
            _      <- zAdd(key)(
                   MemberScore(1d, "Delhi"),
                   MemberScore(2d, "London"),
                   MemberScore(3d, "Paris"),
                   MemberScore(4d, "Tokyo"),
                   MemberScore(5d, "NewYork"),
                   MemberScore(6d, "Seoul")
                 )
            result <-
              zRangeByLex(key, LexRange(min = LexMinimum.Unbounded, max = LexMaximum.Unbounded), Some(Limit(2, 3)))
          } yield assert(result.toList)(equalTo(List("Paris", "Tokyo", "NewYork")))
        },
        testM("empty set") {
          for {
            key    <- uuid
            result <- zRangeByLex(key, LexRange(min = LexMinimum.Open("A"), max = LexMaximum.Closed("Z")))
          } yield assert(result.toList)(isEmpty)
        }
      ),
      suite("zRangeByScore")(
        testM("non-empty set") {
          for {
            key    <- uuid
            _      <- zAdd(key)(
                   MemberScore(1556d, "Samsung"),
                   MemberScore(2000d, "Nokia"),
                   MemberScore(1800d, "Micromax"),
                   MemberScore(2200d, "Sunsui"),
                   MemberScore(1800d, "MicroSoft"),
                   MemberScore(2500d, "LG")
                 )
            result <- zRangeByScore(key, ScoreRange(ScoreMinimum.Open(1500), ScoreMaximum.Closed(1900)))
          } yield assert(result.toList)(equalTo(List("Samsung", "MicroSoft", "Micromax")))
        },
        testM("non-empty set, with scores") {
          for {
            key    <- uuid
            _      <- zAdd(key)(
                   MemberScore(1556d, "Samsung"),
                   MemberScore(2000d, "Nokia"),
                   MemberScore(1800d, "Micromax"),
                   MemberScore(2200d, "Sunsui"),
                   MemberScore(1800d, "MicroSoft"),
                   MemberScore(2500d, "LG")
                 )
            result <- zRangeByScore(
                        key,
                        ScoreRange(ScoreMinimum.Open(1500), ScoreMaximum.Closed(1900)),
                        Some(WithScores)
                      )
          } yield assert(result.toList)(equalTo(List("Samsung", "1556", "MicroSoft", "1800", "Micromax", "1800")))
        },
        testM("non-empty set, with limit") {
          for {
            key    <- uuid
            _      <- zAdd(key)(
                   MemberScore(1556d, "Samsung"),
                   MemberScore(2000d, "Nokia"),
                   MemberScore(1800d, "Micromax"),
                   MemberScore(2200d, "Sunsui"),
                   MemberScore(1800d, "MicroSoft"),
                   MemberScore(2500d, "LG")
                 )
            result <- zRangeByScore(
                        key,
                        ScoreRange(ScoreMinimum.Open(1500), ScoreMaximum.Closed(2500)),
                        limit = Some(Limit(offset = 1, count = 3))
                      )
          } yield assert(result.toList)(equalTo(List("MicroSoft", "Micromax", "Nokia")))
        },
        testM("empty set") {
          for {
            key    <- uuid
            result <- zRangeByScore(key, ScoreRange(ScoreMinimum.Open(1500), ScoreMaximum.Closed(1900)))
          } yield assert(result.toList)(isEmpty)
        }
      ),
      suite("zRank")(
        testM("existing elements from non-empty set") {
          for {
            key  <- uuid
            _    <- zAdd(key)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"))
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
            _       <- zAdd(key)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"))
            removed <- zRem(key, "b", "c")
          } yield assert(removed)(equalTo(2L))
        },
        testM("when just part of elements are present in the non-empty set") {
          for {
            key     <- uuid
            _       <- zAdd(key)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"))
            removed <- zRem(key, "b", "d")
          } yield assert(removed)(equalTo(1L))
        },
        testM("when none of the elements are present in the non-empty set") {
          for {
            key     <- uuid
            _       <- zAdd(key)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"))
            removed <- zRem(key, "d", "e")
          } yield assert(removed)(equalTo(0L))
        },
        testM("elements from an empty set") {
          for {
            key     <- uuid
            removed <- zRem(key, "a", "b")
          } yield assert(removed)(equalTo(0L))
        },
        testM("elements from not set") {
          for {
            key     <- uuid
            value   <- uuid
            _       <- set(key, value)
            removed <- zRem(key, "a", "b").either
          } yield assert(removed)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("zRemRangeByLex")(
        testM("non-empty set") {
          for {
            key         <- uuid
            _           <- zAdd(key)(
                   MemberScore(0d, "Delhi"),
                   MemberScore(0d, "Mumbai"),
                   MemberScore(0d, "Hyderabad"),
                   MemberScore(0d, "Kolkata"),
                   MemberScore(0d, "Chennai")
                 )
            remResult   <-
              zRemRangeByLex(key, LexRange(min = LexMinimum.Open("Hyderabad"), max = LexMaximum.Closed("Mumbai")))
            rangeResult <- zRangeByLex(key, LexRange(min = LexMinimum.Unbounded, max = LexMaximum.Unbounded))
          } yield assert(rangeResult.toList)(equalTo(List("Chennai", "Delhi", "Hyderabad"))) &&
            assert(remResult)(equalTo(2L))
        },
        testM("empty set") {
          for {
            key       <- uuid
            remResult <-
              zRemRangeByLex(key, LexRange(min = LexMinimum.Open("Hyderabad"), max = LexMaximum.Closed("Mumbai")))
          } yield assert(remResult)(equalTo(0L))
        }
      ),
      suite("zRemRangeByRank")(
        testM("non-empty set") {
          for {
            key         <- uuid
            _           <- zAdd(key)(
                   MemberScore(1d, "Delhi"),
                   MemberScore(2d, "Mumbai"),
                   MemberScore(3d, "Hyderabad"),
                   MemberScore(4d, "Kolkata"),
                   MemberScore(5d, "Chennai")
                 )
            remResult   <- zRemRangeByRank(key, 1 to 2)
            rangeResult <- zRange(key, 0 to -1)
          } yield assert(rangeResult.toList)(equalTo(List("Delhi", "Kolkata", "Chennai"))) &&
            assert(remResult)(equalTo(2L))
        },
        testM("empty set") {
          for {
            key       <- uuid
            remResult <- zRemRangeByRank(key, 1 to 2)
          } yield assert(remResult)(equalTo(0L))
        }
      ),
      suite("zRemRangeByScore")(
        testM("non-empty set") {
          for {
            key         <- uuid
            _           <- zAdd(key)(
                   MemberScore(80d, "Delhi"),
                   MemberScore(60d, "Mumbai"),
                   MemberScore(70d, "Hyderabad"),
                   MemberScore(50d, "Kolkata"),
                   MemberScore(65d, "Chennai")
                 )
            remResult   <- zRemRangeByScore(key, ScoreRange(min = ScoreMinimum.Infinity, max = ScoreMaximum.Open(70)))
            rangeResult <- zRangeByScore(key, ScoreRange(min = ScoreMinimum.Infinity, max = ScoreMaximum.Infinity))
          } yield assert(rangeResult.toList)(equalTo(List("Hyderabad", "Delhi"))) && assert(remResult)(equalTo(3L))
        },
        testM("empty set") {
          for {
            key       <- uuid
            remResult <- zRemRangeByScore(key, ScoreRange(min = ScoreMinimum.Infinity, max = ScoreMaximum.Open(70)))
          } yield assert(remResult)(equalTo(0L))
        }
      ),
      suite("zRevRange")(
        testM("non-empty set") {
          for {
            key       <- uuid
            _         <- zAdd(key)(
                   MemberScore(80d, "Delhi"),
                   MemberScore(60d, "Mumbai"),
                   MemberScore(70d, "Hyderabad"),
                   MemberScore(50d, "Kolkata"),
                   MemberScore(65d, "Chennai")
                 )
            revResult <- zRevRange(key, 0 to 1)
          } yield assert(revResult.toList)(equalTo(List("Delhi", "Hyderabad")))
        },
        testM("non-empty set with scores") {
          for {
            key       <- uuid
            _         <- zAdd(key)(
                   MemberScore(80d, "Delhi"),
                   MemberScore(60d, "Mumbai"),
                   MemberScore(70d, "Hyderabad"),
                   MemberScore(50d, "Kolkata"),
                   MemberScore(65d, "Chennai")
                 )
            revResult <- zRevRange(key, 0 to 1, Some(WithScores))
          } yield assert(revResult.toList)(equalTo(List("Delhi", "80", "Hyderabad", "70")))
        },
        testM("empty set") {
          for {
            key       <- uuid
            remResult <- zRevRange(key, 0 to -1)
          } yield assert(remResult.toList)(isEmpty)
        }
      ),
      suite("zRevRangeByLex")(
        testM("non-empty set") {
          for {
            key         <- uuid
            _           <- zAdd(key)(
                   MemberScore(0d, "Delhi"),
                   MemberScore(0d, "London"),
                   MemberScore(0d, "Paris"),
                   MemberScore(0d, "Tokyo"),
                   MemberScore(0d, "NewYork"),
                   MemberScore(0d, "Seoul")
                 )
            rangeResult <-
              zRevRangeByLex(key, LexRange(min = LexMinimum.Open("Seoul"), max = LexMaximum.Closed("Delhi")))
          } yield assert(rangeResult.toList)(equalTo(List("Paris", "NewYork", "London", "Delhi")))
        },
        testM("non-empty set with limit") {
          for {
            key         <- uuid
            _           <- zAdd(key)(
                   MemberScore(0d, "Delhi"),
                   MemberScore(0d, "London"),
                   MemberScore(0d, "Paris"),
                   MemberScore(0d, "Tokyo"),
                   MemberScore(0d, "NewYork"),
                   MemberScore(0d, "Seoul")
                 )
            rangeResult <- zRevRangeByLex(
                             key,
                             LexRange(min = LexMinimum.Open("Seoul"), max = LexMaximum.Closed("Delhi")),
                             Some(Limit(offset = 1, count = 2))
                           )
          } yield assert(rangeResult.toList)(equalTo(List("NewYork", "London")))
        },
        testM("empty set") {
          for {
            key         <- uuid
            rangeResult <-
              zRevRangeByLex(key, LexRange(min = LexMinimum.Open("Hyderabad"), max = LexMaximum.Closed("Mumbai")))
          } yield assert(rangeResult)(isEmpty)
        }
      ),
      suite("zRevRangeByScore")(
        testM("non-empty set") {
          for {
            key         <- uuid
            _           <- zAdd(key)(
                   MemberScore(1556d, "Samsung"),
                   MemberScore(2000d, "Nokia"),
                   MemberScore(1800d, "Micromax"),
                   MemberScore(2200d, "Sunsui"),
                   MemberScore(1800d, "MicroSoft"),
                   MemberScore(2500d, "LG")
                 )
            rangeResult <- zRevRangeByScore(
                             key,
                             //ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
                             ScoreRange(
                               min = ScoreMinimum.Open(2500),
                               max = ScoreMaximum.Closed(2000)
                             ) //TODO min <-> max
                           )
          } yield assert(rangeResult.toList)(equalTo(List("Sunsui", "Nokia")))
        },
        testM("non-empty set with scores") {
          for {
            key         <- uuid
            _           <- zAdd(key)(
                   MemberScore(1556d, "Samsung"),
                   MemberScore(2000d, "Nokia"),
                   MemberScore(1800d, "Micromax"),
                   MemberScore(2200d, "Sunsui"),
                   MemberScore(1800d, "MicroSoft"),
                   MemberScore(2500d, "LG")
                 )
            rangeResult <- zRevRangeByScore(
                             key,
                             //ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
                             ScoreRange(
                               min = ScoreMinimum.Open(2500),
                               max = ScoreMaximum.Closed(2000)
                             ), //TODO min <-> max
                             Some(WithScores)
                           )
          } yield assert(rangeResult.toList)(equalTo(List("Sunsui", "2200", "Nokia", "2000")))
        },
        testM("non-empty set with limit") {
          for {
            key         <- uuid
            _           <- zAdd(key)(
                   MemberScore(1556d, "Samsung"),
                   MemberScore(2000d, "Nokia"),
                   MemberScore(1800d, "Micromax"),
                   MemberScore(2200d, "Sunsui"),
                   MemberScore(1800d, "MicroSoft"),
                   MemberScore(2500d, "LG")
                 )
            rangeResult <- zRevRangeByScore(
                             key,
                             //ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
                             ScoreRange(
                               min = ScoreMinimum.Open(2500),
                               max = ScoreMaximum.Closed(2000)
                             ),
                             limit = Some(Limit(1, 2))
                           )
          } yield assert(rangeResult.toList)(equalTo(List("Nokia")))
        },
        testM("empty set") {
          for {
            key         <- uuid
            rangeResult <- zRevRangeByScore(
                             key,
                             ScoreRange(
                               min = ScoreMinimum.Open(2500),
                               max = ScoreMaximum.Closed(2000)
                             )
                           )
          } yield assert(rangeResult)(isEmpty)
        }
      ),
      suite("zRevRank")(
        testM("non-empty set") {
          for {
            key    <- uuid
            _      <- zAdd(key)(
                   MemberScore(10d, "Delhi"),
                   MemberScore(20d, "Mumbai"),
                   MemberScore(30d, "Hyderabad"),
                   MemberScore(40d, "Kolkata"),
                   MemberScore(50d, "Chennai")
                 )
            result <- zRevRank(key, "Hyderabad")
          } yield assert(result)(equalTo(Some(2L)))
        },
        testM("empty set") {
          for {
            key    <- uuid
            result <- zRevRank(key, "Hyderabad")
          } yield assert(result)(isNone)
        }
      ),
      suite("zScan")(
        testM("non-empty set") {
          for {
            key     <- uuid
            _       <- zAdd(key)(MemberScore(1d, "atest"), MemberScore(2d, "btest"), MemberScore(3d, "ctest"))
            members <- scanAll(key)
          } yield assert(members)(equalTo(Chunk("atest", "1", "btest", "2", "ctest", "3")))
        },
        testM("empty set") {
          for {
            key              <- uuid
            scan             <- zScan(key, 0L)
            (cursor, members) = scan
          } yield assert(cursor)(isZero) && assert(members)(isEmpty)
        },
        testM("with match over non-empty set") {
          for {
            key     <- uuid
            _       <- zAdd(key)(MemberScore(1d, "one"), MemberScore(2d, "two"), MemberScore(3d, "three"))
            members <- scanAll(key, Some("t[a-z]*".r))
          } yield assert(members)(equalTo((Chunk("two", "2", "three", "3"))))
        },
        testM("with count over non-empty set") {
          for {
            key     <- uuid
            _       <- zAdd(key)(
                   MemberScore(1d, "a"),
                   MemberScore(2d, "b"),
                   MemberScore(3d, "c"),
                   MemberScore(4d, "d"),
                   MemberScore(5d, "e")
                 )
            members <- scanAll(key, count = Some(Count(3L)))
          } yield assert(members)(isNonEmpty)
        },
        testM("match with count over non-empty set") {
          for {
            key     <- uuid
            _       <- zAdd(key)(
                   MemberScore(1d, "testa"),
                   MemberScore(2d, "testb"),
                   MemberScore(3d, "testc"),
                   MemberScore(4d, "testd"),
                   MemberScore(5d, "teste")
                 )
            members <- scanAll(key, Some("t[a-z]*".r), Some(Count(3L)))
          } yield assert(members)(isNonEmpty)
        },
        testM("error when not set") {
          for {
            key   <- uuid
            value <- uuid
            _     <- set(key, value)
            scan  <- zScan(key, 0L).either
          } yield assert(scan)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("zScore")(
        testM("non-empty set") {
          for {
            key    <- uuid
            _      <- zAdd(key)(
                   MemberScore(10d, "Delhi"),
                   MemberScore(20d, "Mumbai"),
                   MemberScore(30d, "Hyderabad"),
                   MemberScore(40d, "Kolkata"),
                   MemberScore(50d, "Chennai")
                 )
            result <- zScore(key, "Delhi")
          } yield assert(result)(equalTo(Some(10.0)))
        },
        testM("empty set") {
          for {
            key    <- uuid
            result <- zScore(key, "Hyderabad")
          } yield assert(result)(isNone)
        }
      ),
      suite("zUnionStore")(
        testM("two non-empty sets") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            _      <- zAdd(second)(MemberScore(1d, "a"), MemberScore(3d, "c"), MemberScore(5d, "e"))
            card   <- zUnionStore(dest, 2, first, second)()
          } yield assert(card)(equalTo(5L))
        },
        testM("equal to the non-empty set when the other one is empty") {
          for {
            nonEmpty <- uuid
            empty    <- uuid
            dest     <- uuid
            _        <- zAdd(nonEmpty)(MemberScore(1d, "a"), MemberScore(2d, "b"))
            card     <- zUnionStore(dest, 2, nonEmpty, empty)()
          } yield assert(card)(equalTo(2L))
        },
        testM("empty when both sets are empty") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            card   <- zUnionStore(dest, 2, first, second)()
          } yield assert(card)(equalTo(0L))
        },
        testM("non-empty set with multiple non-empty sets") {
          for {
            first  <- uuid
            second <- uuid
            third  <- uuid
            dest   <- uuid
            _      <- zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            _      <- zAdd(second)(MemberScore(2, "b"), MemberScore(4d, "d"))
            _      <- zAdd(third)(MemberScore(2, "b"), MemberScore(3d, "c"), MemberScore(5d, "e"))
            card   <- zUnionStore(dest, 3, first, second, third)()
          } yield assert(card)(equalTo(5L))
        },
        testM("error when the first parameter is not set") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            value  <- uuid
            _      <- set(first, value)
            card   <- zUnionStore(dest, 2, first, second)().either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("error when the first parameter is set and the second parameter is not set") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            value  <- uuid
            _      <- zAdd(first)(MemberScore(1, "a"))
            _      <- set(second, value)
            card   <- zUnionStore(dest, 2, first, second)().either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        },
        testM("parameter weights provided") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _      <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            card   <- zUnionStore(dest, 2, first, second)(Some(::(2, List(3))))
          } yield assert(card)(equalTo(4L))
        },
        testM("error when invalid weights provided ( less than sets number )") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _      <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            card   <- zUnionStore(dest, 2, first, second)(Some(::(2, Nil))).either
          } yield assert(card)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("error when invalid weights provided ( more than sets number )") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _      <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            card   <- zUnionStore(dest, 2, first, second)(Some(::(2, List(3, 5)))).either
          } yield assert(card)(isLeft(isSubtype[ProtocolError](anything)))
        },
        testM("set aggregate parameter MAX") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _      <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            card   <- zUnionStore(dest, 2, first, second)(aggregate = Some(Aggregate.Max))
          } yield assert(card)(equalTo(4L))
        },
        testM("set aggregate parameter MIN") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _      <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            card   <- zUnionStore(dest, 2, first, second)(aggregate = Some(Aggregate.Min))
          } yield assert(card)(equalTo(4L))
        },
        testM("parameter weights provided along with aggregate") {
          for {
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _      <- zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            card   <- zUnionStore(dest, 2, first, second)(Some(::(2, List(3))), Some(Aggregate.Max))
          } yield assert(card)(equalTo(4L))
        }
      )
    )
  private def scanAll(
    key: String,
    regex: Option[Regex] = None,
    count: Option[Count] = None
  ): ZIO[RedisExecutor, RedisError, Chunk[String]] =
    ZStream
      .paginateChunkM(0L) { cursor =>
        zScan(key, cursor, regex, count).map {
          case (nc, nm) if nc == 0 => (nm, None)
          case (nc, nm)            => (nm, Some(nc))
        }
      }
      .runCollect
}
