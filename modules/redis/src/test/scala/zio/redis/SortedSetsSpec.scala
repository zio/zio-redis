package zio.redis

import zio._
import zio.redis.RedisError.{ProtocolError, WrongType}
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test._

trait SortedSetsSpec extends BaseSpec {
  def sortedSetsSuite: Spec[Redis, RedisError] =
    suite("sorted sets")(
      suite("bzPopMax")(
        test("non-empty set")(
          for {
            redis   <- ZIO.service[Redis]
            key1    <- uuid
            key2    <- uuid
            key3    <- uuid
            duration = Duration.fromMillis(1000)
            delhi    = MemberScore(1d, "Delhi")
            mumbai   = MemberScore(2d, "Mumbai")
            london   = MemberScore(3d, "London")
            paris    = MemberScore(4d, "Paris")
            tokyo    = MemberScore(5d, "Tokyo")
            _       <- redis.zAdd(key1)(delhi, tokyo)
            _       <- redis.zAdd(key2)(mumbai, paris)
            _       <- redis.zAdd(key3)(london)
            result  <- redis.bzPopMax(duration, key1, key2, key3).returning[String]
          } yield assert(result)(isSome(equalTo((key1, tokyo))))
        ),
        test("empty set")(
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            duration = Duration.fromMillis(1000)
            result  <- redis.bzPopMax(duration, key).returning[String]
          } yield assert(result)(isNone)
        )
      ) @@ clusterExecutorUnsupported,
      suite("bzPopMin")(
        test("non-empty set")(
          for {
            redis   <- ZIO.service[Redis]
            key1    <- uuid
            key2    <- uuid
            key3    <- uuid
            duration = Duration.fromMillis(1000)
            delhi    = MemberScore(1d, "Delhi")
            london   = MemberScore(3d, "London")
            paris    = MemberScore(4d, "Paris")
            _       <- redis.zAdd(key2)(delhi, paris)
            _       <- redis.zAdd(key3)(london)
            result  <- redis.bzPopMin(duration, key1, key2, key3).returning[String]
          } yield assert(result)(isSome(equalTo((key2, delhi))))
        ),
        test("empty set")(
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            duration = Duration.fromMillis(1000)
            result  <- redis.bzPopMin(duration, key).returning[String]
          } yield assert(result)(isNone)
        )
      ) @@ clusterExecutorUnsupported,
      suite("zAdd")(
        test("to empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            value <- uuid
            added <- redis.zAdd(key)(MemberScore(1d, value))
          } yield assert(added)(equalTo(1L))
        },
        test("to the non-empty set") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            value  <- uuid
            _      <- redis.zAdd(key)(MemberScore(1d, value))
            value2 <- uuid
            added  <- redis.zAdd(key)(MemberScore(2d, value2))
          } yield assert(added)(equalTo(1L))
        },
        test("existing element to set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            value <- uuid
            _     <- redis.zAdd(key)(MemberScore(1d, value))
            added <- redis.zAdd(key)(MemberScore(2d, value))
          } yield assert(added)(equalTo(0L))
        },
        test("multiple elements to set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            added <- redis.zAdd(key)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"))
          } yield assert(added)(equalTo(3L))
        },
        test("error when not set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            value <- uuid
            _     <- redis.set(key, value)
            added <- redis.zAdd(key)(MemberScore(1d, value)).either
          } yield assert(added)(isLeft(isSubtype[WrongType](anything)))
        },
        test("NX - do not to update existing members, only add new") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.zAdd(key)(MemberScore(1d, "v1"))
            _      <- redis.zAdd(key)(MemberScore(2d, "v2"))
            added  <- redis.zAdd(key, Some(Update.SetNew))(MemberScore(3d, "v3"), MemberScore(22d, "v2"))
            result <- redis.zRange(key, 0 to -1).returning[String]
          } yield assert(added)(equalTo(1L)) && assert(result.toList)(equalTo(List("v1", "v2", "v3")))
        },
        test("XX - update existing members, not add new") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.zAdd(key)(MemberScore(1d, "v1"))
            _      <- redis.zAdd(key)(MemberScore(2d, "v2"))
            added  <- redis.zAdd(key, Some(Update.SetExisting))(MemberScore(3d, "v3"), MemberScore(11d, "v1"))
            result <- redis.zRange(key, 0 to -1).returning[String]
          } yield assert(added)(equalTo(0L)) && assert(result.toList)(equalTo(List("v2", "v1")))
        },
        test("CH - return number of new and updated members") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.zAdd(key)(MemberScore(1d, "v1"))
            _      <- redis.zAdd(key)(MemberScore(2d, "v2"))
            added  <- redis.zAdd(key, change = Some(Changed))(MemberScore(3d, "v3"), MemberScore(11d, "v1"))
            result <- redis.zRange(key, 0 to -1).returning[String]
          } yield assert(added)(equalTo(2L)) && assert(result.toList)(equalTo(List("v2", "v3", "v1")))
        },
        test("LT - return number of new members") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.zAdd(key)(MemberScore(3d, "v1"))
            _      <- redis.zAdd(key)(MemberScore(4d, "v2"))
            added  <- redis.zAdd(key, update = Some(Update.SetLessThan))(MemberScore(1d, "v3"), MemberScore(2d, "v1"))
            result <- redis.zRange(key, 0 to -1).returning[String]
          } yield assert(added)(equalTo(1L)) && assert(result.toList)(equalTo(List("v3", "v1", "v2")))
        },
        test("GT - return number of new members") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.zAdd(key)(MemberScore(1d, "v1"))
            _      <- redis.zAdd(key)(MemberScore(2d, "v2"))
            added  <- redis.zAdd(key, update = Some(Update.SetGreaterThan))(MemberScore(1d, "v3"), MemberScore(3d, "v1"))
            result <- redis.zRange(key, 0 to -1).returning[String]
          } yield assert(added)(equalTo(1L)) && assert(result.toList)(equalTo(List("v3", "v2", "v1")))
        },
        test("GT CH - return number of new and updated members") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.zAdd(key)(MemberScore(1d, "v1"))
            _     <- redis.zAdd(key)(MemberScore(2d, "v2"))
            added <- redis.zAdd(key, update = Some(Update.SetGreaterThan), change = Some(Changed))(
                       MemberScore(1d, "v3"),
                       MemberScore(3d, "v1")
                     )
            result <- redis.zRange(key, 0 to -1).returning[String]
          } yield assert(added)(equalTo(2L)) && assert(result.toList)(equalTo(List("v3", "v2", "v1")))
        },
        test("INCR - increment by score") {
          for {
            redis    <- ZIO.service[Redis]
            key      <- uuid
            _        <- redis.zAdd(key)(MemberScore(1d, "v1"))
            _        <- redis.zAdd(key)(MemberScore(2d, "v2"))
            newScore <- redis.zAddWithIncr(key)(Increment, MemberScore(3d, "v1"))
            result   <- redis.zRange(key, 0 to -1).returning[String]
          } yield assert(newScore)(isSome(equalTo(4.0))) && assert(result.toList)(equalTo(List("v2", "v1")))
        }
      ),
      suite("zCard")(
        test("non-empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.zAdd(key)(MemberScore(1d, "hello"), MemberScore(2d, "world"))
            card  <- redis.zCard(key)
          } yield assert(card)(equalTo(2L))
        },
        test("0 when key doesn't exist") {
          assertZIO(ZIO.serviceWithZIO[Redis](_.zCard("unknownSet")))(equalTo(0L))
        },
        test("error when not set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            value <- uuid
            _     <- redis.set(key, value)
            card  <- redis.zCard(key).either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("zCount")(
        test("non-empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _ <- redis.zAdd(key)(
                   MemberScore(1d, "a"),
                   MemberScore(2d, "b"),
                   MemberScore(3d, "c"),
                   MemberScore(4d, "d"),
                   MemberScore(5d, "e")
                 )
            count <- redis.zCount(key, 0 to 3)
          } yield assert(count)(equalTo(3L))
        },
        test("empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            count <- redis.zCount(key, 0 to 3)
          } yield assert(count)(equalTo(0L))
        }
      ),
      suite("zDiff")(
        test("empty sets") {
          for {
            redis <- ZIO.service[Redis]
            key1  <- uuid
            key2  <- uuid
            key3  <- uuid
            diff  <- redis.zDiff(key1, key2, key3).returning[String]
          } yield assert(diff)(isEmpty)
        },
        test("non-empty set with empty set") {
          for {
            redis <- ZIO.service[Redis]
            key1  <- uuid
            key2  <- uuid
            _     <- redis.zAdd(key1)(MemberScore(1d, "a"), MemberScore(2d, "b"))
            diff  <- redis.zDiff(key1, key2).returning[String]
          } yield assert(diff)(hasSameElements(Chunk("a", "b")))
        },
        test("non-empty sets") {
          for {
            redis <- ZIO.service[Redis]
            key1  <- uuid
            key2  <- uuid
            _     <- redis.zAdd(key1)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"))
            _     <- redis.zAdd(key2)(MemberScore(1d, "a"), MemberScore(2d, "b"))
            diff  <- redis.zDiff(key1, key2).returning[String]
          } yield assert(diff)(hasSameElements(Chunk("c")))
        }
      ) @@ clusterExecutorUnsupported,
      suite("zDiffWithScores")(
        test("empty sets") {
          for {
            redis <- ZIO.service[Redis]
            key1  <- uuid
            key2  <- uuid
            key3  <- uuid
            diff  <- redis.zDiffWithScores(key1, key2, key3).returning[String]
          } yield assert(diff)(isEmpty)
        },
        test("non-empty set with empty set") {
          for {
            redis <- ZIO.service[Redis]
            key1  <- uuid
            key2  <- uuid
            _     <- redis.zAdd(key1)(MemberScore(1d, "a"), MemberScore(2d, "b"))
            diff  <- redis.zDiffWithScores(key1, key2).returning[String]
          } yield assert(diff)(hasSameElements(Chunk(MemberScore(1d, "a"), MemberScore(2d, "b"))))
        },
        test("non-empty sets") {
          for {
            redis <- ZIO.service[Redis]
            key1  <- uuid
            key2  <- uuid
            _     <- redis.zAdd(key1)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"))
            _     <- redis.zAdd(key2)(MemberScore(1d, "a"), MemberScore(2d, "b"))
            diff  <- redis.zDiffWithScores(key1, key2).returning[String]
          } yield assert(diff)(hasSameElements(Chunk(MemberScore(3d, "c"))))
        }
      ) @@ clusterExecutorUnsupported,
      suite("zDiffStore")(
        test("empty sets") {
          for {
            redis <- ZIO.service[Redis]
            dest  <- uuid
            key1  <- uuid
            key2  <- uuid
            key3  <- uuid
            card  <- redis.zDiffStore(dest, key1, key2, key3)
          } yield assert(card)(equalTo(0L))
        },
        test("non-empty set with empty set") {
          for {
            redis <- ZIO.service[Redis]
            dest  <- uuid
            key1  <- uuid
            key2  <- uuid
            _ <- redis.zAdd(key1)(
                   MemberScore(1d, "a"),
                   MemberScore(2d, "b")
                 )
            card <- redis.zDiffStore(dest, key1, key2)
          } yield assert(card)(equalTo(2L))
        },
        test("non-empty sets") {
          for {
            redis <- ZIO.service[Redis]
            dest  <- uuid
            key1  <- uuid
            key2  <- uuid
            _ <- redis.zAdd(key1)(
                   MemberScore(1d, "a"),
                   MemberScore(2d, "b"),
                   MemberScore(3d, "c")
                 )
            _    <- redis.zAdd(key2)(MemberScore(1d, "a"), MemberScore(2d, "b"))
            card <- redis.zDiffStore(dest, key1, key2)
          } yield assert(card)(equalTo(1L))
        }
      ) @@ clusterExecutorUnsupported,
      suite("zIncrBy")(
        test("non-empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _ <- redis.zAdd(key)(
                   MemberScore(1d, "a"),
                   MemberScore(2d, "b"),
                   MemberScore(3d, "c"),
                   MemberScore(4d, "d"),
                   MemberScore(5d, "e")
                 )
            incrRes <- redis.zIncrBy(key, 10, "a")
            count   <- redis.zCount(key, 10 to 11)
          } yield assert(count)(equalTo(1L)) && assert(incrRes)(equalTo(11.0))
        },
        test("empty set") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            incrRes <- redis.zIncrBy(key, 10, "a")
            count   <- redis.zCount(key, 0 to -1)
          } yield assert(count)(equalTo(0L)) && assert(incrRes)(equalTo(10.0))
        }
      ),
      suite("zInter")(
        test("two non-empty sets") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            _ <-
              redis.zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            _       <- redis.zAdd(second)(MemberScore(1d, "a"), MemberScore(3d, "c"), MemberScore(5d, "e"))
            members <- redis.zInter(first, second)().returning[String]
          } yield assert(members)(equalTo(Chunk("a", "c")))
        },
        test("empty when one of the sets is empty") {
          for {
            redis    <- ZIO.service[Redis]
            nonEmpty <- uuid
            empty    <- uuid
            _        <- redis.zAdd(nonEmpty)(MemberScore(1d, "a"), MemberScore(2d, "b"))
            members  <- redis.zInter(nonEmpty, empty)().returning[String]
          } yield assert(members)(isEmpty)
        },
        test("empty when both sets are empty") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            members <- redis.zInter(first, second)().returning[String]
          } yield assert(members)(isEmpty)
        },
        test("non-empty set with multiple non-empty sets") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            third  <- uuid
            _ <-
              redis.zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            _       <- redis.zAdd(second)(MemberScore(2d, "b"), MemberScore(2d, "b"), MemberScore(4d, "d"))
            _       <- redis.zAdd(third)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"))
            members <- redis.zInter(first, second, third)().returning[String]
          } yield assert(members)(
            equalTo(Chunk("b"))
          )
        },
        test("error when first parameter is not set") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            value   <- uuid
            _       <- redis.set(first, value)
            members <- redis.zInter(first, second)().returning[String].either
          } yield assert(members)(isLeft(isSubtype[WrongType](anything)))
        },
        test("error with empty first set and second parameter is not set") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            value   <- uuid
            _       <- redis.set(second, value)
            members <- redis.zInter(first, second)().returning[String].either
          } yield assert(members)(isLeft(isSubtype[WrongType](anything)))
        },
        test("error with non-empty first set and second parameter is not set") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            value   <- uuid
            _       <- redis.zAdd(first)(MemberScore(1d, "a"))
            _       <- redis.set(second, value)
            members <- redis.zInter(first, second)().returning[String].either
          } yield assert(members)(isLeft(isSubtype[WrongType](anything)))
        },
        test("parameter weights provided") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- redis.zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- redis.zInter(first, second)(weights = Some(::(2.0, 3.0 :: Nil))).returning[String]
          } yield assert(members)(equalTo(Chunk("O", "N")))
        },
        test("error when invalid weights provided ( less than sets number )") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- redis.zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- redis.zInter(first, second)(weights = Some(::(2, Nil))).returning[String].either
          } yield assert(members)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when invalid weights provided ( more than sets number )") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- redis.zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- redis.zInter(first, second)(weights = Some(::(2.0, List(3.0, 5.0)))).returning[String].either
          } yield assert(members)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("set aggregate parameter MAX") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- redis.zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- redis.zInter(first, second)(Some(Aggregate.Max)).returning[String]
          } yield assert(members)(equalTo(Chunk("N", "O")))
        },
        test("set aggregate parameter MIN") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- redis.zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- redis.zInter(first, second)(Some(Aggregate.Min)).returning[String]
          } yield assert(members)(equalTo(Chunk("O", "N")))
        }
      ) @@ clusterExecutorUnsupported,
      suite("zInterWithScores")(
        test("two non-empty sets") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            _ <-
              redis.zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            _       <- redis.zAdd(second)(MemberScore(1d, "a"), MemberScore(3d, "c"), MemberScore(5d, "e"))
            members <- redis.zInterWithScores(first, second)().returning[String]
          } yield assert(members)(equalTo(Chunk(MemberScore(2d, "a"), MemberScore(6d, "c"))))
        },
        test("empty when one of the sets is empty") {
          for {
            redis    <- ZIO.service[Redis]
            nonEmpty <- uuid
            empty    <- uuid
            _        <- redis.zAdd(nonEmpty)(MemberScore(1d, "a"), MemberScore(2d, "b"))
            members  <- redis.zInterWithScores(nonEmpty, empty)().returning[String]
          } yield assert(members)(isEmpty)
        },
        test("empty when both sets are empty") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            members <- redis.zInterWithScores(first, second)().returning[String]
          } yield assert(members)(isEmpty)
        },
        test("non-empty set with multiple non-empty sets") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            third  <- uuid
            _ <-
              redis.zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            _       <- redis.zAdd(second)(MemberScore(2d, "b"), MemberScore(2d, "b"), MemberScore(4d, "d"))
            _       <- redis.zAdd(third)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"))
            members <- redis.zInterWithScores(first, second, third)().returning[String]
          } yield assert(members)(
            equalTo(Chunk(MemberScore(6d, "b")))
          )
        },
        test("error when first parameter is not set") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            value   <- uuid
            _       <- redis.set(first, value)
            members <- redis.zInterWithScores(first, second)().returning[String].either
          } yield assert(members)(isLeft(isSubtype[WrongType](anything)))
        },
        test("error with empty first set and second parameter is not set") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            value   <- uuid
            _       <- redis.set(second, value)
            members <- redis.zInterWithScores(first, second)().returning[String].either
          } yield assert(members)(isLeft(isSubtype[WrongType](anything)))
        },
        test("error with non-empty first set and second parameter is not set") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            value   <- uuid
            _       <- redis.zAdd(first)(MemberScore(1d, "a"))
            _       <- redis.set(second, value)
            members <- redis.zInterWithScores(first, second)().returning[String].either
          } yield assert(members)(isLeft(isSubtype[WrongType](anything)))
        },
        test("parameter weights provided") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- redis.zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- redis.zInterWithScores(first, second)(weights = Some(::(2.0, 3.0 :: Nil))).returning[String]
          } yield assert(members)(equalTo(Chunk(MemberScore(20d, "O"), MemberScore(21d, "N"))))
        },
        test("error when invalid weights provided ( less than sets number )") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- redis.zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- redis.zInterWithScores(first, second)(weights = Some(::(2, Nil))).returning[String].either
          } yield assert(members)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when invalid weights provided ( more than sets number )") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            _      <- redis.zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _      <- redis.zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- redis
                         .zInterWithScores(first, second)(weights = Some(::(2.0, List(3.0, 5.0))))
                         .returning[String]
                         .either
          } yield assert(members)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("set aggregate parameter MAX") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- redis.zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- redis.zInterWithScores(first, second)(Some(Aggregate.Max)).returning[String]
          } yield assert(members)(equalTo(Chunk(MemberScore(6d, "N"), MemberScore(7d, "O"))))
        },
        test("set aggregate parameter MIN") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- redis.zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- redis.zInterWithScores(first, second)(Some(Aggregate.Min)).returning[String]
          } yield assert(members)(equalTo(Chunk(MemberScore(2d, "O"), MemberScore(3d, "N"))))
        }
      ) @@ clusterExecutorUnsupported,
      suite("zInterStore")(
        test("two non-empty sets") {
          for {
            redis  <- ZIO.service[Redis]
            dest   <- uuid
            first  <- uuid
            second <- uuid
            _ <-
              redis.zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            _    <- redis.zAdd(second)(MemberScore(1d, "a"), MemberScore(3d, "c"), MemberScore(5d, "e"))
            card <- redis.zInterStore(s"out_$dest", first, second)()
          } yield assert(card)(equalTo(2L))
        },
        test("empty when one of the sets is empty") {
          for {
            redis    <- ZIO.service[Redis]
            dest     <- uuid
            nonEmpty <- uuid
            empty    <- uuid
            _        <- redis.zAdd(nonEmpty)(MemberScore(1d, "a"), MemberScore(2d, "b"))
            card     <- redis.zInterStore(dest, nonEmpty, empty)()
          } yield assert(card)(equalTo(0L))
        },
        test("empty when both sets are empty") {
          for {
            redis  <- ZIO.service[Redis]
            dest   <- uuid
            first  <- uuid
            second <- uuid
            card   <- redis.zInterStore(dest, first, second)()
          } yield assert(card)(equalTo(0L))
        },
        test("non-empty set with multiple non-empty sets") {
          for {
            redis  <- ZIO.service[Redis]
            dest   <- uuid
            first  <- uuid
            second <- uuid
            third  <- uuid
            _ <-
              redis.zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            _    <- redis.zAdd(second)(MemberScore(2d, "b"), MemberScore(2d, "b"), MemberScore(4d, "d"))
            _    <- redis.zAdd(third)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"))
            card <- redis.zInterStore(dest, first, second, third)()
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
            card   <- redis.zInterStore(dest, first, second)().either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        },
        test("error with empty first set and second parameter is not set") {
          for {
            redis  <- ZIO.service[Redis]
            dest   <- uuid
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- redis.set(second, value)
            card   <- redis.zInterStore(dest, first, second)().either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        },
        test("error with non-empty first set and second parameter is not set") {
          for {
            redis  <- ZIO.service[Redis]
            dest   <- uuid
            first  <- uuid
            second <- uuid
            value  <- uuid
            _      <- redis.zAdd(first)(MemberScore(1d, "a"))
            _      <- redis.set(second, value)
            card   <- redis.zInterStore(dest, first, second)().either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        },
        test("parameter weights provided") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- redis.zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _      <- redis.zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            card   <- redis.zInterStore(dest, first, second)(weights = Some(::(2.0, 3.0 :: Nil)))
          } yield assert(card)(equalTo(2L))
        },
        test("error when invalid weights provided ( less than sets number )") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- redis.zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _      <- redis.zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            card   <- redis.zInterStore(dest, first, second)(weights = Some(::(2, Nil))).either
          } yield assert(card)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when invalid weights provided ( more than sets number )") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- redis.zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _      <- redis.zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            card   <- redis.zInterStore(dest, first, second)(weights = Some(::(2.0, List(3.0, 5.0)))).either
          } yield assert(card)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("set aggregate parameter MAX") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- redis.zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _      <- redis.zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            card   <- redis.zInterStore(dest, first, second)(Some(Aggregate.Max))
          } yield assert(card)(equalTo(2L))
        },
        test("set aggregate parameter MIN") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- redis.zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _      <- redis.zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            card   <- redis.zInterStore(dest, first, second)(Some(Aggregate.Min))
          } yield assert(card)(equalTo(2L))
        }
      ) @@ clusterExecutorUnsupported,
      suite("zLexCount")(
        test("non-empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _ <- redis.zAdd(key)(
                   MemberScore(1d, "Delhi"),
                   MemberScore(2d, "Mumbai"),
                   MemberScore(3d, "London"),
                   MemberScore(4d, "Paris"),
                   MemberScore(5d, "Tokyo")
                 )
            count <- redis.zLexCount(key, LexRange(min = LexMinimum.Closed("London"), max = LexMaximum.Open("Paris")))
          } yield assert(count)(equalTo(2L))
        },
        test("empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            count <- redis.zLexCount(key, LexRange(min = LexMinimum.Closed("London"), max = LexMaximum.Open("Paris")))
          } yield assert(count)(equalTo(0L))
        }
      ),
      suite("zPopMax")(
        test("non-empty set")(
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            delhi   = MemberScore(1d, "Delhi")
            mumbai  = MemberScore(2d, "Mumbai")
            london  = MemberScore(3d, "London")
            paris   = MemberScore(4d, "Paris")
            tokyo   = MemberScore(5d, "Tokyo")
            _      <- redis.zAdd(key)(delhi, mumbai, london, paris, tokyo)
            result <- redis.zPopMax(key).returning[String]
          } yield assert(result.toList)(equalTo(List(tokyo)))
        ),
        test("non-empty set with count param")(
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            delhi   = MemberScore(1d, "Delhi")
            mumbai  = MemberScore(2d, "Mumbai")
            london  = MemberScore(3d, "London")
            paris   = MemberScore(4d, "Paris")
            tokyo   = MemberScore(5d, "Tokyo")
            _      <- redis.zAdd(key)(delhi, mumbai, london, paris, tokyo)
            result <- redis.zPopMax(key, Some(3)).returning[String]
          } yield assert(result.toList)(equalTo(List(tokyo, paris, london)))
        ),
        test("empty set")(for {
          redis  <- ZIO.service[Redis]
          key    <- uuid
          result <- redis.zPopMax(key).returning[String]
        } yield assert(result.toList)(isEmpty))
      ),
      suite("zPopMin")(
        test("non-empty set")(
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            delhi   = MemberScore(1d, "Delhi")
            mumbai  = MemberScore(2d, "Mumbai")
            london  = MemberScore(3d, "London")
            paris   = MemberScore(4d, "Paris")
            tokyo   = MemberScore(5d, "Tokyo")
            _      <- redis.zAdd(key)(delhi, mumbai, london, paris, tokyo)
            result <- redis.zPopMin(key).returning[String]
          } yield assert(result.toList)(equalTo(List(delhi)))
        ),
        test("non-empty set with count param")(
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            delhi   = MemberScore(1d, "Delhi")
            mumbai  = MemberScore(2d, "Mumbai")
            london  = MemberScore(3d, "London")
            paris   = MemberScore(4d, "Paris")
            tokyo   = MemberScore(5d, "Tokyo")
            _      <- redis.zAdd(key)(delhi, mumbai, london, paris, tokyo)
            result <- redis.zPopMin(key, Some(3)).returning[String]
          } yield assert(result.toList)(equalTo(List(delhi, mumbai, london)))
        ),
        test("empty set")(for {
          redis  <- ZIO.service[Redis]
          key    <- uuid
          result <- redis.zPopMin(key).returning[String]
        } yield assert(result.toList)(isEmpty))
      ),
      suite("zRange")(
        test("non-empty set") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            delhi   = MemberScore(1d, "Delhi")
            mumbai  = MemberScore(2d, "Mumbai")
            london  = MemberScore(3d, "London")
            paris   = MemberScore(4d, "Paris")
            tokyo   = MemberScore(5d, "Tokyo")
            _      <- redis.zAdd(key)(delhi, mumbai, london, tokyo, paris)
            result <- redis.zRange(key, 0 to -1).returning[String]
          } yield assert(result.toList)(equalTo(List("Delhi", "Mumbai", "London", "Paris", "Tokyo")))
        },
        test("empty set") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            result <- redis.zRange(key, 0 to -1).returning[String]
          } yield assert(result.toList)(isEmpty)
        }
      ),
      suite("zRangeWithScores")(
        test("non-empty set") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            delhi   = MemberScore(1d, "Delhi")
            mumbai  = MemberScore(2d, "Mumbai")
            london  = MemberScore(3d, "London")
            paris   = MemberScore(4d, "Paris")
            tokyo   = MemberScore(5d, "Tokyo")
            _      <- redis.zAdd(key)(delhi, mumbai, london, tokyo, paris)
            result <- redis.zRangeWithScores(key, 0 to -1).returning[String]
          } yield assert(result.toList)(
            equalTo(List(delhi, mumbai, london, paris, tokyo))
          )
        },
        test("empty set") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            result <- redis.zRangeWithScores(key, 0 to -1).returning[String]
          } yield assert(result.toList)(isEmpty)
        }
      ),
      suite("zRangeByLex")(
        test("non-empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _ <- redis.zAdd(key)(
                   MemberScore(1d, "Delhi"),
                   MemberScore(2d, "London"),
                   MemberScore(3d, "Paris"),
                   MemberScore(4d, "Tokyo"),
                   MemberScore(5d, "NewYork"),
                   MemberScore(6d, "Seoul")
                 )
            result <- redis
                        .zRangeByLex(
                          key,
                          LexRange(min = LexMinimum.Open("London"), max = LexMaximum.Closed("Seoul"))
                        )
                        .returning[String]
          } yield assert(result.toList)(equalTo(List("Paris")))
        },
        test("non-empty set with limit") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _ <- redis.zAdd(key)(
                   MemberScore(1d, "Delhi"),
                   MemberScore(2d, "London"),
                   MemberScore(3d, "Paris"),
                   MemberScore(4d, "Tokyo"),
                   MemberScore(5d, "NewYork"),
                   MemberScore(6d, "Seoul")
                 )
            result <- redis
                        .zRangeByLex(
                          key,
                          LexRange(min = LexMinimum.Unbounded, max = LexMaximum.Unbounded),
                          Some(Limit(2, 3))
                        )
                        .returning[String]
          } yield assert(result.toList)(equalTo(List("Paris", "Tokyo", "NewYork")))
        },
        test("empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            result <- redis
                        .zRangeByLex(key, LexRange(min = LexMinimum.Open("A"), max = LexMaximum.Closed("Z")))
                        .returning[String]
          } yield assert(result.toList)(isEmpty)
        }
      ),
      suite("zRangeByScore")(
        test("non-empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _ <- redis.zAdd(key)(
                   MemberScore(1556d, "Samsung"),
                   MemberScore(2000d, "Nokia"),
                   MemberScore(1801d, "Micromax"),
                   MemberScore(2200d, "Sunsui"),
                   MemberScore(1800d, "MicroSoft"),
                   MemberScore(2500d, "LG")
                 )
            result <- redis
                        .zRangeByScore(key, ScoreRange(ScoreMinimum.Open(1500), ScoreMaximum.Closed(1900)))
                        .returning[String]
          } yield assert(result.toList)(equalTo(List("Samsung", "MicroSoft", "Micromax")))
        },
        test("non-empty set, with limit") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _ <- redis.zAdd(key)(
                   MemberScore(1556d, "Samsung"),
                   MemberScore(2000d, "Nokia"),
                   MemberScore(1801d, "Micromax"),
                   MemberScore(2200d, "Sunsui"),
                   MemberScore(1800d, "MicroSoft"),
                   MemberScore(2500d, "LG")
                 )
            scoreRange = ScoreRange(ScoreMinimum.Open(1500), ScoreMaximum.Closed(2500))
            result    <- redis.zRangeByScore(key, scoreRange, Some(Limit(offset = 1, count = 3))).returning[String]
          } yield assert(result.toList)(equalTo(List("MicroSoft", "Micromax", "Nokia")))
        },
        test("empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            result <- redis
                        .zRangeByScore(key, ScoreRange(ScoreMinimum.Open(1500), ScoreMaximum.Closed(1900)))
                        .returning[String]
          } yield assert(result.toList)(isEmpty)
        }
      ),
      suite("zRangeByScoreWithScores")(
        test("non-empty set") {
          for {
            redis     <- ZIO.service[Redis]
            key       <- uuid
            samsung    = MemberScore(1556d, "Samsung")
            nokia      = MemberScore(2000d, "Nokia")
            micromax   = MemberScore(1801d, "Micromax")
            sunsui     = MemberScore(2200d, "Sunsui")
            microSoft  = MemberScore(1800d, "MicroSoft")
            lg         = MemberScore(2500d, "LG")
            _         <- redis.zAdd(key)(samsung, nokia, micromax, sunsui, microSoft, lg)
            scoreRange = ScoreRange(ScoreMinimum.Open(1500), ScoreMaximum.Closed(1900))
            result    <- redis.zRangeByScoreWithScores(key, scoreRange).returning[String]
          } yield assert(result.toList)(equalTo(List(samsung, microSoft, micromax)))
        },
        test("non-empty set, with limit") {
          for {
            redis     <- ZIO.service[Redis]
            key       <- uuid
            samsung    = MemberScore(1556d, "Samsung")
            nokia      = MemberScore(2000d, "Nokia")
            micromax   = MemberScore(1801d, "Micromax")
            sunsui     = MemberScore(2200d, "Sunsui")
            microSoft  = MemberScore(1800d, "MicroSoft")
            lg         = MemberScore(2500d, "LG")
            _         <- redis.zAdd(key)(samsung, nokia, micromax, sunsui, microSoft, lg)
            scoreRange = ScoreRange(ScoreMinimum.Open(1500), ScoreMaximum.Closed(2500))
            result <-
              redis.zRangeByScoreWithScores(key, scoreRange, Some(Limit(offset = 1, count = 3))).returning[String]
          } yield assert(result.toList)(equalTo(List(microSoft, micromax, nokia)))
        },
        test("empty set") {
          for {
            redis     <- ZIO.service[Redis]
            key       <- uuid
            scoreRange = ScoreRange(ScoreMinimum.Open(1500), ScoreMaximum.Closed(1900))
            result    <- redis.zRangeByScoreWithScores(key, scoreRange).returning[String]
          } yield assert(result.toList)(isEmpty)
        }
      ),
      suite("zRank")(
        test("existing elements from non-empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.zAdd(key)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"))
            rank  <- redis.zRank(key, "c")
          } yield assert(rank)(isSome(equalTo(2L)))
        },
        test("empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            rank  <- redis.zRank(key, "c")
          } yield assert(rank)(isNone)
        }
      ),
      suite("zRankWithScore")(
        test("existing elements from non-empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.zAdd(key)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"))
            rank  <- redis.zRankWithScore(key, "c")
          } yield assert(rank)(isSome(equalTo(RankScore(3d, 2L))))
        },
        test("empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            rank  <- redis.zRankWithScore(key, "c")
          } yield assert(rank)(isNone)
        }
      ),
      suite("zRem")(
        test("existing elements from non-empty set") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            _       <- redis.zAdd(key)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"))
            removed <- redis.zRem(key, "b", "c")
          } yield assert(removed)(equalTo(2L))
        },
        test("when just part of elements are present in the non-empty set") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            _       <- redis.zAdd(key)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"))
            removed <- redis.zRem(key, "b", "d")
          } yield assert(removed)(equalTo(1L))
        },
        test("when none of the elements are present in the non-empty set") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            _       <- redis.zAdd(key)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"))
            removed <- redis.zRem(key, "d", "e")
          } yield assert(removed)(equalTo(0L))
        },
        test("elements from an empty set") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            removed <- redis.zRem(key, "a", "b")
          } yield assert(removed)(equalTo(0L))
        },
        test("elements from not set") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            value   <- uuid
            _       <- redis.set(key, value)
            removed <- redis.zRem(key, "a", "b").either
          } yield assert(removed)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("zRemRangeByLex")(
        test("non-empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _ <- redis.zAdd(key)(
                   MemberScore(0d, "Delhi"),
                   MemberScore(0d, "Mumbai"),
                   MemberScore(0d, "Hyderabad"),
                   MemberScore(0d, "Kolkata"),
                   MemberScore(0d, "Chennai")
                 )
            remResult <-
              redis.zRemRangeByLex(key, LexRange(min = LexMinimum.Open("Hyderabad"), max = LexMaximum.Closed("Mumbai")))
            rangeResult <- redis
                             .zRangeByLex(key, LexRange(min = LexMinimum.Unbounded, max = LexMaximum.Unbounded))
                             .returning[String]
          } yield assert(rangeResult.toList)(equalTo(List("Chennai", "Delhi", "Hyderabad"))) &&
            assert(remResult)(equalTo(2L))
        },
        test("empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            remResult <-
              redis.zRemRangeByLex(key, LexRange(min = LexMinimum.Open("Hyderabad"), max = LexMaximum.Closed("Mumbai")))
          } yield assert(remResult)(equalTo(0L))
        }
      ),
      suite("zRemRangeByRank")(
        test("non-empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _ <- redis.zAdd(key)(
                   MemberScore(1d, "Delhi"),
                   MemberScore(2d, "Mumbai"),
                   MemberScore(3d, "Hyderabad"),
                   MemberScore(4d, "Kolkata"),
                   MemberScore(5d, "Chennai")
                 )
            remResult   <- redis.zRemRangeByRank(key, 1 to 2)
            rangeResult <- redis.zRange(key, 0 to -1).returning[String]
          } yield assert(rangeResult.toList)(equalTo(List("Delhi", "Kolkata", "Chennai"))) &&
            assert(remResult)(equalTo(2L))
        },
        test("empty set") {
          for {
            redis     <- ZIO.service[Redis]
            key       <- uuid
            remResult <- redis.zRemRangeByRank(key, 1 to 2)
          } yield assert(remResult)(equalTo(0L))
        }
      ),
      suite("zRemRangeByScore")(
        test("non-empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _ <- redis.zAdd(key)(
                   MemberScore(80d, "Delhi"),
                   MemberScore(60d, "Mumbai"),
                   MemberScore(70d, "Hyderabad"),
                   MemberScore(50d, "Kolkata"),
                   MemberScore(65d, "Chennai")
                 )
            remResult <-
              redis.zRemRangeByScore(key, ScoreRange(min = ScoreMinimum.Infinity, max = ScoreMaximum.Open(70)))
            rangeResult <- redis
                             .zRangeByScore(key, ScoreRange(min = ScoreMinimum.Infinity, max = ScoreMaximum.Infinity))
                             .returning[String]
          } yield assert(rangeResult.toList)(equalTo(List("Hyderabad", "Delhi"))) && assert(remResult)(equalTo(3L))
        },
        test("empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            remResult <-
              redis.zRemRangeByScore(key, ScoreRange(min = ScoreMinimum.Infinity, max = ScoreMaximum.Open(70)))
          } yield assert(remResult)(equalTo(0L))
        }
      ),
      suite("zRevRange")(
        test("non-empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _ <- redis.zAdd(key)(
                   MemberScore(80d, "Delhi"),
                   MemberScore(60d, "Mumbai"),
                   MemberScore(70d, "Hyderabad"),
                   MemberScore(50d, "Kolkata"),
                   MemberScore(65d, "Chennai")
                 )
            revResult <- redis.zRevRange(key, 0 to 1).returning[String]
          } yield assert(revResult.toList)(equalTo(List("Delhi", "Hyderabad")))
        },
        test("empty set") {
          for {
            redis     <- ZIO.service[Redis]
            key       <- uuid
            remResult <- redis.zRevRange(key, 0 to -1).returning[String]
          } yield assert(remResult.toList)(isEmpty)
        }
      ),
      suite("zRevRangeWithScores")(
        test("non-empty set") {
          for {
            redis     <- ZIO.service[Redis]
            key       <- uuid
            delhi      = MemberScore(80d, "Delhi")
            mumbai     = MemberScore(60d, "Mumbai")
            hyderabad  = MemberScore(70d, "Hyderabad")
            kolkata    = MemberScore(50d, "Kolkata")
            chennai    = MemberScore(65d, "Chennai")
            _         <- redis.zAdd(key)(delhi, mumbai, hyderabad, kolkata, chennai)
            revResult <- redis.zRevRangeWithScores(key, 0 to 1).returning[String]
          } yield assert(revResult.toList)(equalTo(List(delhi, hyderabad)))
        },
        test("empty set") {
          for {
            redis     <- ZIO.service[Redis]
            key       <- uuid
            remResult <- redis.zRevRange(key, 0 to -1).returning[String]
          } yield assert(remResult.toList)(isEmpty)
        }
      ),
      suite("zRevRangeByLex")(
        test("non-empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _ <- redis.zAdd(key)(
                   MemberScore(0d, "Delhi"),
                   MemberScore(0d, "London"),
                   MemberScore(0d, "Paris"),
                   MemberScore(0d, "Tokyo"),
                   MemberScore(0d, "NewYork"),
                   MemberScore(0d, "Seoul")
                 )
            lexRange     = LexRange(min = LexMinimum.Closed("Delhi"), max = LexMaximum.Open("Seoul"))
            rangeResult <- redis.zRevRangeByLex(key, lexRange).returning[String]
          } yield assert(rangeResult.toList)(equalTo(List("Paris", "NewYork", "London", "Delhi")))
        },
        test("non-empty set with limit") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _ <- redis.zAdd(key)(
                   MemberScore(0d, "Delhi"),
                   MemberScore(0d, "London"),
                   MemberScore(0d, "Paris"),
                   MemberScore(0d, "Tokyo"),
                   MemberScore(0d, "NewYork"),
                   MemberScore(0d, "Seoul")
                 )
            lexRange     = LexRange(min = LexMinimum.Closed("Delhi"), max = LexMaximum.Open("Seoul"))
            rangeResult <- redis.zRevRangeByLex(key, lexRange, Some(Limit(offset = 1, count = 2))).returning[String]
          } yield assert(rangeResult.toList)(equalTo(List("NewYork", "London")))
        },
        test("empty set") {
          for {
            redis       <- ZIO.service[Redis]
            key         <- uuid
            lexRange     = LexRange(min = LexMinimum.Closed("Mumbai"), max = LexMaximum.Open("Hyderabad"))
            rangeResult <- redis.zRevRangeByLex(key, lexRange).returning[String]
          } yield assert(rangeResult)(isEmpty)
        }
      ),
      suite("zRevRangeByScore")(
        test("non-empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _ <- redis.zAdd(key)(
                   MemberScore(1556d, "Samsung"),
                   MemberScore(2000d, "Nokia"),
                   MemberScore(1800d, "Micromax"),
                   MemberScore(2200d, "Sunsui"),
                   MemberScore(1800d, "MicroSoft"),
                   MemberScore(2500d, "LG")
                 )
            scoreRange   = ScoreRange(ScoreMinimum.Closed(2000), ScoreMaximum.Open(2500))
            rangeResult <- redis.zRevRangeByScore(key, scoreRange).returning[String]
          } yield assert(rangeResult.toList)(equalTo(List("Sunsui", "Nokia")))
        },
        test("non-empty set with limit") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _ <- redis.zAdd(key)(
                   MemberScore(1556d, "Samsung"),
                   MemberScore(2000d, "Nokia"),
                   MemberScore(1800d, "Micromax"),
                   MemberScore(2200d, "Sunsui"),
                   MemberScore(1800d, "MicroSoft"),
                   MemberScore(2500d, "LG")
                 )
            scoreRange   = ScoreRange(ScoreMinimum.Closed(2000), ScoreMaximum.Open(2500))
            rangeResult <- redis.zRevRangeByScore(key, scoreRange, Some(Limit(1, 2))).returning[String]
          } yield assert(rangeResult.toList)(equalTo(List("Nokia")))
        },
        test("empty set") {
          for {
            redis       <- ZIO.service[Redis]
            key         <- uuid
            scoreRange   = ScoreRange(ScoreMinimum.Closed(2000), ScoreMaximum.Open(2500))
            rangeResult <- redis.zRevRangeByScore(key, scoreRange).returning[String]
          } yield assert(rangeResult)(isEmpty)
        }
      ),
      suite("zRevRangeByScoreWithScores")(
        test("non-empty set") {
          for {
            redis       <- ZIO.service[Redis]
            key         <- uuid
            samsung      = MemberScore(1556d, "Samsung")
            nokia        = MemberScore(2000d, "Nokia")
            micromax     = MemberScore(1800d, "Micromax")
            sunsui       = MemberScore(2200d, "Sunsui")
            nicroSoft    = MemberScore(1800d, "MicroSoft")
            lg           = MemberScore(2500d, "LG")
            _           <- redis.zAdd(key)(samsung, nokia, micromax, sunsui, nicroSoft, lg)
            scoreRange   = ScoreRange(ScoreMinimum.Closed(2000), ScoreMaximum.Open(2500))
            rangeResult <- redis.zRevRangeByScoreWithScores(key, scoreRange).returning[String]
          } yield assert(rangeResult.toList)(equalTo(List(sunsui, nokia)))
        },
        test("non-empty set with limit") {
          for {
            redis       <- ZIO.service[Redis]
            key         <- uuid
            samsung      = MemberScore(1556d, "Samsung")
            nokia        = MemberScore(2000d, "Nokia")
            micromax     = MemberScore(1800d, "Micromax")
            sunsui       = MemberScore(2200d, "Sunsui")
            nicroSoft    = MemberScore(1800d, "MicroSoft")
            lg           = MemberScore(2500d, "LG")
            _           <- redis.zAdd(key)(samsung, nokia, micromax, sunsui, nicroSoft, lg)
            scoreRange   = ScoreRange(ScoreMinimum.Closed(2000), ScoreMaximum.Open(2500))
            rangeResult <- redis.zRevRangeByScoreWithScores(key, scoreRange, Some(Limit(1, 2))).returning[String]
          } yield assert(rangeResult.toList)(equalTo(List(nokia)))
        },
        test("empty set") {
          for {
            redis       <- ZIO.service[Redis]
            key         <- uuid
            scoreRange   = ScoreRange(ScoreMinimum.Closed(2000), ScoreMaximum.Open(2500))
            rangeResult <- redis.zRevRangeByScoreWithScores(key, scoreRange).returning[String]
          } yield assert(rangeResult)(isEmpty)
        }
      ),
      suite("zRevRank")(
        test("non-empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _ <- redis.zAdd(key)(
                   MemberScore(10d, "Delhi"),
                   MemberScore(20d, "Mumbai"),
                   MemberScore(30d, "Hyderabad"),
                   MemberScore(40d, "Kolkata"),
                   MemberScore(50d, "Chennai")
                 )
            result <- redis.zRevRank(key, "Hyderabad")
          } yield assert(result)(isSome(equalTo(2L)))
        },
        test("empty set") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            result <- redis.zRevRank(key, "Hyderabad")
          } yield assert(result)(isNone)
        }
      ),
      suite("zRevRankWithScore")(
        test("non-empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _ <- redis.zAdd(key)(
                   MemberScore(10d, "Delhi"),
                   MemberScore(20d, "Mumbai"),
                   MemberScore(30d, "Hyderabad"),
                   MemberScore(40d, "Kolkata"),
                   MemberScore(50d, "Chennai")
                 )
            result <- redis.zRevRankWithScore(key, "Kolkata")
          } yield assert(result)(isSome(equalTo(RankScore(40d, 1L))))
        },
        test("empty set") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            result <- redis.zRevRankWithScore(key, "Hyderabad")
          } yield assert(result)(isNone)
        }
      ),
      suite("zScan")(
        test("non-empty set") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            a        = MemberScore(1d, "atest")
            b        = MemberScore(2d, "btest")
            c        = MemberScore(3d, "ctest")
            _       <- redis.zAdd(key)(a, b, c)
            members <- scanAll(key)
          } yield assert(members)(equalTo(Chunk(a, b, c)))
        },
        test("empty set") {
          for {
            redis            <- ZIO.service[Redis]
            key              <- uuid
            scan             <- redis.zScan(key, 0L).returning[String]
            (cursor, members) = scan
          } yield assert(cursor)(isZero) && assert(members)(isEmpty)
        },
        test("with match over non-empty set") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            one      = MemberScore(1d, "one")
            two      = MemberScore(2d, "two")
            three    = MemberScore(3d, "three")
            _       <- redis.zAdd(key)(one, two, three)
            members <- scanAll(key, Some("t[a-z]*"))
          } yield assert(members)(equalTo(Chunk(two, three)))
        },
        test("with count over non-empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _ <- redis.zAdd(key)(
                   MemberScore(1d, "a"),
                   MemberScore(2d, "b"),
                   MemberScore(3d, "c"),
                   MemberScore(4d, "d"),
                   MemberScore(5d, "e")
                 )
            members <- scanAll(key, count = Some(Count(3L)))
          } yield assert(members)(isNonEmpty)
        },
        test("match with count over non-empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _ <- redis.zAdd(key)(
                   MemberScore(1d, "testa"),
                   MemberScore(2d, "testb"),
                   MemberScore(3d, "testc"),
                   MemberScore(4d, "testd"),
                   MemberScore(5d, "teste")
                 )
            members <- scanAll(key, pattern = Some("t[a-z]*"), count = Some(Count(3L)))
          } yield assert(members)(isNonEmpty)
        },
        test("error when not set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            value <- uuid
            _     <- redis.set(key, value)
            scan  <- redis.zScan(key, 0L).returning[String].either
          } yield assert(scan)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("zScore")(
        test("non-empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _ <- redis.zAdd(key)(
                   MemberScore(10d, "Delhi"),
                   MemberScore(20d, "Mumbai"),
                   MemberScore(30d, "Hyderabad"),
                   MemberScore(40d, "Kolkata"),
                   MemberScore(50d, "Chennai")
                 )
            result <- redis.zScore(key, "Delhi")
          } yield assert(result)(isSome(equalTo(10.0)))
        },
        test("empty set") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            result <- redis.zScore(key, "Hyderabad")
          } yield assert(result)(isNone)
        }
      ),
      suite("zMScore")(
        test("non-empty set") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _ <- redis.zAdd(key)(
                   MemberScore(10d, "Delhi"),
                   MemberScore(20d, "Mumbai"),
                   MemberScore(30d, "Hyderabad"),
                   MemberScore(40d, "Kolkata"),
                   MemberScore(50d, "Chennai")
                 )
            result <- redis.zMScore(key, "Delhi", "Mumbai", "notFound")
          } yield assert(result)(equalTo(Chunk(Some(10d), Some(20d), None)))
        },
        test("empty set") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            result <- redis.zMScore(key, "Hyderabad")
          } yield assert(result)(equalTo(Chunk(None)))
        }
      ),
      suite("zUnion")(
        test("two non-empty sets") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            _ <-
              redis.zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            _       <- redis.zAdd(second)(MemberScore(1d, "a"), MemberScore(3d, "c"), MemberScore(5d, "e"))
            members <- redis.zUnion(first, second)().returning[String]
          } yield assert(members)(equalTo(Chunk("a", "b", "d", "e", "c")))
        },
        test("equal to the non-empty set when the other one is empty") {
          for {
            redis    <- ZIO.service[Redis]
            nonEmpty <- uuid
            empty    <- uuid
            _        <- redis.zAdd(nonEmpty)(MemberScore(1d, "a"), MemberScore(2d, "b"))
            members  <- redis.zUnion(nonEmpty, empty)().returning[String]
          } yield assert(members)(equalTo(Chunk("a", "b")))
        },
        test("empty when both sets are empty") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            members <- redis.zUnion(first, second)().returning[String]
          } yield assert(members)(isEmpty)
        },
        test("non-empty set with multiple non-empty sets") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            third  <- uuid
            _ <-
              redis.zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            _       <- redis.zAdd(second)(MemberScore(2, "b"), MemberScore(4d, "d"))
            _       <- redis.zAdd(third)(MemberScore(2, "b"), MemberScore(3d, "c"), MemberScore(5d, "e"))
            members <- redis.zUnion(first, second, third)().returning[String]
          } yield assert(members)(equalTo(Chunk("a", "e", "b", "c", "d")))
        },
        test("error when the first parameter is not set") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            value   <- uuid
            _       <- redis.set(first, value)
            members <- redis.zUnion(first, second)().returning[String].either
          } yield assert(members)(isLeft(isSubtype[WrongType](anything)))
        },
        test("error when the first parameter is set and the second parameter is not set") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            value   <- uuid
            _       <- redis.zAdd(first)(MemberScore(1, "a"))
            _       <- redis.set(second, value)
            members <- redis.zUnion(first, second)().returning[String].either
          } yield assert(members)(isLeft(isSubtype[WrongType](anything)))
        },
        test("parameter weights provided") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- redis.zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- redis.zUnion(first, second)(Some(::(2, List(3)))).returning[String]
          } yield assert(members)(equalTo(Chunk("M", "P", "O", "N")))
        },
        test("error when invalid weights provided ( less than sets number )") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- redis.zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- redis.zUnion(first, second)(Some(::(2, Nil))).returning[String].either
          } yield assert(members)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when invalid weights provided ( more than sets number )") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- redis.zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- redis.zUnion(first, second)(Some(::(2, List(3, 5)))).returning[String].either
          } yield assert(members)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("set aggregate parameter MAX") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- redis.zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- redis.zUnion(first, second)(aggregate = Some(Aggregate.Max)).returning[String]
          } yield assert(members)(equalTo(Chunk("P", "M", "N", "O")))
        },
        test("set aggregate parameter MIN") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- redis.zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- redis.zUnion(first, second)(aggregate = Some(Aggregate.Min)).returning[String]
          } yield assert(members)(equalTo(Chunk("O", "N", "P", "M")))
        },
        test("parameter weights provided along with aggregate") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- redis.zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- redis.zUnion(first, second)(Some(::(2, List(3))), Some(Aggregate.Max)).returning[String]
          } yield assert(members)(equalTo(Chunk("M", "N", "P", "O")))
        }
      ) @@ clusterExecutorUnsupported,
      suite("zUnionWithScores")(
        test("two non-empty sets") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            _ <-
              redis.zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            _       <- redis.zAdd(second)(MemberScore(1d, "a"), MemberScore(3d, "c"), MemberScore(5d, "e"))
            members <- redis.zUnionWithScores(first, second)().returning[String]
          } yield assert(members)(
            equalTo(
              Chunk(
                MemberScore(2d, "a"),
                MemberScore(2d, "b"),
                MemberScore(4d, "d"),
                MemberScore(5d, "e"),
                MemberScore(6d, "c")
              )
            )
          )
        },
        test("equal to the non-empty set when the other one is empty") {
          for {
            redis    <- ZIO.service[Redis]
            nonEmpty <- uuid
            empty    <- uuid
            _        <- redis.zAdd(nonEmpty)(MemberScore(1d, "a"), MemberScore(2d, "b"))
            members  <- redis.zUnionWithScores(nonEmpty, empty)().returning[String]
          } yield assert(members)(equalTo(Chunk(MemberScore(1d, "a"), MemberScore(2d, "b"))))
        },
        test("empty when both sets are empty") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            members <- redis.zUnionWithScores(first, second)().returning[String]
          } yield assert(members)(isEmpty)
        },
        test("non-empty set with multiple non-empty sets") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            third  <- uuid
            _ <-
              redis.zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            _       <- redis.zAdd(second)(MemberScore(2, "b"), MemberScore(4d, "d"))
            _       <- redis.zAdd(third)(MemberScore(2, "b"), MemberScore(3d, "c"), MemberScore(5d, "e"))
            members <- redis.zUnionWithScores(first, second, third)().returning[String]
          } yield assert(members)(
            equalTo(
              Chunk(
                MemberScore(1d, "a"),
                MemberScore(5d, "e"),
                MemberScore(6d, "b"),
                MemberScore(6d, "c"),
                MemberScore(8d, "d")
              )
            )
          )
        },
        test("error when the first parameter is not set") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            value   <- uuid
            _       <- redis.set(first, value)
            members <- redis.zUnionWithScores(first, second)().returning[String].either
          } yield assert(members)(isLeft(isSubtype[WrongType](anything)))
        },
        test("error when the first parameter is set and the second parameter is not set") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            value   <- uuid
            _       <- redis.zAdd(first)(MemberScore(1, "a"))
            _       <- redis.set(second, value)
            members <- redis.zUnionWithScores(first, second)().returning[String].either
          } yield assert(members)(isLeft(isSubtype[WrongType](anything)))
        },
        test("parameter weights provided") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- redis.zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- redis.zUnionWithScores(first, second)(Some(::(2, List(3)))).returning[String]
          } yield assert(members)(
            equalTo(
              Chunk(
                MemberScore(10d, "M"),
                MemberScore(12d, "P"),
                MemberScore(20d, "O"),
                MemberScore(21d, "N")
              )
            )
          )
        },
        test("error when invalid weights provided ( less than sets number )") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- redis.zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- redis.zUnionWithScores(first, second)(Some(::(2, Nil))).returning[String].either
          } yield assert(members)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when invalid weights provided ( more than sets number )") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- redis.zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- redis.zUnionWithScores(first, second)(Some(::(2, List(3, 5)))).returning[String].either
          } yield assert(members)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("set aggregate parameter MAX") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- redis.zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- redis.zUnionWithScores(first, second)(aggregate = Some(Aggregate.Max)).returning[String]
          } yield assert(members)(
            equalTo(
              Chunk(
                MemberScore(4d, "P"),
                MemberScore(5d, "M"),
                MemberScore(6d, "N"),
                MemberScore(7d, "O")
              )
            )
          )
        },
        test("set aggregate parameter MIN") {
          for {
            redis   <- ZIO.service[Redis]
            first   <- uuid
            second  <- uuid
            _       <- redis.zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _       <- redis.zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <- redis.zUnionWithScores(first, second)(aggregate = Some(Aggregate.Min)).returning[String]
          } yield assert(members)(
            equalTo(
              Chunk(
                MemberScore(2d, "O"),
                MemberScore(3d, "N"),
                MemberScore(4d, "P"),
                MemberScore(5d, "M")
              )
            )
          )
        },
        test("parameter weights provided along with aggregate") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            _      <- redis.zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _      <- redis.zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            members <-
              redis.zUnionWithScores(first, second)(Some(::(2, List(3))), Some(Aggregate.Max)).returning[String]
          } yield assert(members)(
            equalTo(
              Chunk(
                MemberScore(10d, "M"),
                MemberScore(12d, "N"),
                MemberScore(12d, "P"),
                MemberScore(14d, "O")
              )
            )
          )
        }
      ) @@ clusterExecutorUnsupported,
      suite("zUnionStore")(
        test("two non-empty sets") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _ <-
              redis.zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            _    <- redis.zAdd(second)(MemberScore(1d, "a"), MemberScore(3d, "c"), MemberScore(5d, "e"))
            card <- redis.zUnionStore(dest, first, second)()
          } yield assert(card)(equalTo(5L))
        },
        test("equal to the non-empty set when the other one is empty") {
          for {
            redis    <- ZIO.service[Redis]
            nonEmpty <- uuid
            empty    <- uuid
            dest     <- uuid
            _        <- redis.zAdd(nonEmpty)(MemberScore(1d, "a"), MemberScore(2d, "b"))
            card     <- redis.zUnionStore(dest, nonEmpty, empty)()
          } yield assert(card)(equalTo(2L))
        },
        test("empty when both sets are empty") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            dest   <- uuid
            card   <- redis.zUnionStore(dest, first, second)()
          } yield assert(card)(equalTo(0L))
        },
        test("non-empty set with multiple non-empty sets") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            third  <- uuid
            dest   <- uuid
            _ <-
              redis.zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            _    <- redis.zAdd(second)(MemberScore(2, "b"), MemberScore(4d, "d"))
            _    <- redis.zAdd(third)(MemberScore(2, "b"), MemberScore(3d, "c"), MemberScore(5d, "e"))
            card <- redis.zUnionStore(dest, first, second, third)()
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
            card   <- redis.zUnionStore(dest, first, second)().either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        },
        test("error when the first parameter is set and the second parameter is not set") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            dest   <- uuid
            value  <- uuid
            _      <- redis.zAdd(first)(MemberScore(1, "a"))
            _      <- redis.set(second, value)
            card   <- redis.zUnionStore(dest, first, second)().either
          } yield assert(card)(isLeft(isSubtype[WrongType](anything)))
        },
        test("parameter weights provided") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- redis.zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _      <- redis.zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            card   <- redis.zUnionStore(dest, first, second)(Some(::(2, List(3))))
          } yield assert(card)(equalTo(4L))
        },
        test("error when invalid weights provided ( less than sets number )") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- redis.zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _      <- redis.zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            card   <- redis.zUnionStore(dest, first, second)(Some(::(2, Nil))).either
          } yield assert(card)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("error when invalid weights provided ( more than sets number )") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- redis.zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _      <- redis.zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            card   <- redis.zUnionStore(dest, first, second)(Some(::(2, List(3, 5)))).either
          } yield assert(card)(isLeft(isSubtype[ProtocolError](anything)))
        },
        test("set aggregate parameter MAX") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- redis.zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _      <- redis.zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            card   <- redis.zUnionStore(dest, first, second)(aggregate = Some(Aggregate.Max))
          } yield assert(card)(equalTo(4L))
        },
        test("set aggregate parameter MIN") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- redis.zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _      <- redis.zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            card   <- redis.zUnionStore(dest, first, second)(aggregate = Some(Aggregate.Min))
          } yield assert(card)(equalTo(4L))
        },
        test("parameter weights provided along with aggregate") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            dest   <- uuid
            _      <- redis.zAdd(first)(MemberScore(5d, "M"), MemberScore(6d, "N"), MemberScore(7d, "O"))
            _      <- redis.zAdd(second)(MemberScore(3d, "N"), MemberScore(2d, "O"), MemberScore(4d, "P"))
            card   <- redis.zUnionStore(dest, first, second)(Some(::(2, List(3))), Some(Aggregate.Max))
          } yield assert(card)(equalTo(4L))
        }
      ) @@ clusterExecutorUnsupported,
      suite("zRandMember")(
        test("key does not exist") {
          for {
            redis     <- ZIO.service[Redis]
            first     <- uuid
            notExists <- uuid
            _ <-
              redis.zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            ret <- redis.zRandMember(notExists).returning[String]
          } yield assert(ret)(isNone)
        },
        test("key does not exist with count") {
          for {
            redis     <- ZIO.service[Redis]
            first     <- uuid
            notExists <- uuid
            _ <-
              redis.zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            ret <- redis.zRandMember(notExists, 1).returning[String]
          } yield assert(ret)(isEmpty)
        },
        test("get an element") {
          for {
            redis <- ZIO.service[Redis]
            first <- uuid
            _ <-
              redis.zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            ret <- redis.zRandMember(first).returning[String]
          } yield assert(ret)(isSome)
        },
        test("get elements with count") {
          for {
            redis <- ZIO.service[Redis]
            first <- uuid
            _ <-
              redis.zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            ret <- redis.zRandMember(first, 2).returning[String]
          } yield assert(ret)(hasSize(equalTo(2)))
        }
      ),
      suite("zRandMemberWithScores")(
        test("key does not exist") {
          for {
            redis     <- ZIO.service[Redis]
            first     <- uuid
            notExists <- uuid
            _ <-
              redis.zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            ret <- redis.zRandMemberWithScores(notExists, 1).returning[String]
          } yield assert(ret)(isEmpty)
        },
        test("get elements with count") {
          for {
            redis <- ZIO.service[Redis]
            first <- uuid
            _ <-
              redis.zAdd(first)(MemberScore(1d, "a"), MemberScore(2d, "b"), MemberScore(3d, "c"), MemberScore(4d, "d"))
            ret <- redis.zRandMemberWithScores(first, 2).returning[String]
          } yield assert(ret)(hasSize(equalTo(2)))
        }
      )
    )

  private def scanAll(
    key: String,
    pattern: Option[String] = None,
    count: Option[Count] = None
  ): ZIO[Redis, RedisError, Chunk[MemberScore[String]]] =
    ZStream
      .paginateChunkZIO(0L) { cursor =>
        ZIO.serviceWithZIO[Redis](_.zScan(key, cursor, pattern, count).returning[String]).map {
          case (nc, nm) if nc == 0 => (nm, None)
          case (nc, nm)            => (nm, Some(nc))
        }
      }
      .runCollect

}
