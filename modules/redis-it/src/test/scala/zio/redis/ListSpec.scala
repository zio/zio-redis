package zio.redis

import zio.Clock.currentTime
import zio._
import zio.redis.RedisError.WrongType
import zio.test.Assertion._
import zio.test._

import java.util.concurrent.TimeUnit

trait ListSpec extends IntegrationSpec {
  def listSuite: Spec[Redis, Any] =
    suite("lists")(
      suite("pop")(
        test("lPop non-empty list") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            _      <- redis.lPush(key, "world", "hello")
            popped <- redis.lPop(key).returning[String]
          } yield assert(popped)(isSome(equalTo("hello")))
        },
        test("lPop empty list") {
          for {
            redis  <- ZIO.service[Redis]
            popped <- redis.lPop("unknown").returning[String]
          } yield assert(popped)(isNone)
        },
        test("lPop error not list") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            value <- uuid
            _     <- redis.set(key, value)
            pop   <- redis.lPop(key).returning[String].either
          } yield assert(pop)(isLeft)
        },
        test("rPop non-empty list") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.rPush(key, "world", "hello")
            pop   <- redis.rPop(key).returning[String]
          } yield assert(pop)(isSome(equalTo("hello")))
        },
        test("rPop empty list") {
          for {
            redis <- ZIO.service[Redis]
            pop   <- redis.rPop("unknown").returning[String]
          } yield assert(pop)(isNone)
        },
        test("rPop error not list") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            value <- uuid
            _     <- redis.set(key, value)
            pop   <- redis.rPop(key).returning[String].either
          } yield assert(pop)(isLeft)
        }
      ),
      suite("push")(
        test("lPush onto empty list") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            push  <- redis.lPush(key, "hello")
            range <- redis.lRange(key, 0 to -1).returning[String]
          } yield assert(push)(equalTo(1L)) && assert(range)(equalTo(Chunk("hello")))
        },
        test("lPush multiple elements onto empty list") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            push  <- redis.lPush(key, "hello", "world")
            range <- redis.lRange(key, 0 to -1).returning[String]
          } yield assert(push)(equalTo(2L)) && assert(range)(equalTo(Chunk("world", "hello")))
        },
        test("lPush error when not list") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            value <- uuid
            _     <- redis.set(key, value)
            push  <- redis.lPush(key, "hello").either
          } yield assert(push)(isLeft)
        },
        test("lPushX onto non-empty list") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.lPush(key, "world")
            px    <- redis.lPushX(key, "hello")
            range <- redis.lRange(key, 0 to -1).returning[String]
          } yield assert(px)(equalTo(2L)) && assert(range)(equalTo(Chunk("hello", "world")))
        },
        test("lPushX nothing when key doesn't exist") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            px    <- redis.lPushX(key, "world")
          } yield assert(px)(equalTo(0L))
        },
        test("lPushX error when not list") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            value <- uuid
            _     <- redis.set(key, value)
            push  <- redis.lPushX(key, "hello").either
          } yield assert(push)(isLeft(isSubtype[RedisError.WrongType](anything)))
        },
        test("rPush onto empty list") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            push  <- redis.rPush(key, "hello")
            range <- redis.lRange(key, 0 to -1).returning[String]
          } yield assert(push)(equalTo(1L)) && assert(range)(equalTo(Chunk("hello")))
        },
        test("rPush multiple elements onto empty list") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            push  <- redis.rPush(key, "hello", "world")
            range <- redis.lRange(key, 0 to -1).returning[String]
          } yield assert(push)(equalTo(2L)) && assert(range)(equalTo(Chunk("hello", "world")))
        },
        test("rPush error when not list") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            value <- uuid
            _     <- redis.set(key, value)
            push  <- redis.rPush(key, "hello").either
          } yield assert(push)(isLeft)
        },
        test("rPushX onto non-empty list") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.rPush(key, "world")
            px    <- redis.rPushX(key, "hello")
            range <- redis.lRange(key, 0 to -1).returning[String]
          } yield assert(px)(equalTo(2L)) && assert(range)(equalTo(Chunk("world", "hello")))
        },
        test("rPushX nothing when key doesn't exist") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            px    <- redis.rPushX(key, "world")
          } yield assert(px)(equalTo(0L))
        },
        test("rPushX error when not list") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            value <- uuid
            _     <- redis.set(key, value)
            push  <- redis.rPushX(key, "hello").either
          } yield assert(push)(isLeft(isSubtype[RedisError.WrongType](anything)))
        }
      ),
      suite("remove")(
        test("lRem 2 elements moving from head") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            _       <- redis.lPush(key, "world", "hello", "hello", "hello")
            removed <- redis.lRem(key, 2, "hello")
            range   <- redis.lRange(key, 0 to 1).returning[String]
          } yield assert(removed)(equalTo(2L)) && assert(range)(equalTo(Chunk("hello", "world")))
        },
        test("lRem 2 elements moving from tail") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            _       <- redis.lPush(key, "hello", "hello", "world", "hello")
            removed <- redis.lRem(key, -2, "hello")
            range   <- redis.lRange(key, 0 to 1).returning[String]
          } yield assert(removed)(equalTo(2L)) && assert(range)(equalTo(Chunk("hello", "world")))
        },
        test("lRem all 3 'hello' elements") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            _       <- redis.lPush(key, "hello", "hello", "world", "hello")
            removed <- redis.lRem(key, 0, "hello")
            range   <- redis.lRange(key, 0 to 1).returning[String]
          } yield assert(removed)(equalTo(3L)) && assert(range)(equalTo(Chunk("world")))
        },
        test("lRem nothing when key does not exist") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            _       <- redis.lPush(key, "world", "hello")
            removed <- redis.lRem(key, 0, "goodbye")
            range   <- redis.lRange(key, 0 to 1).returning[String]
          } yield assert(removed)(equalTo(0L)) && assert(range)(equalTo(Chunk("hello", "world")))
        },
        test("lRem error when not list") {
          for {
            redis   <- ZIO.service[Redis]
            key     <- uuid
            _       <- redis.set(key, "hello")
            removed <- redis.lRem(key, 0, "hello").either
          } yield assert(removed)(isLeft)
        }
      ),
      suite("set")(
        test("lSet element") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.lPush(key, "world", "hello")
            _     <- redis.lSet(key, 1, "goodbye")
            range <- redis.lRange(key, 0 to 1).returning[String]
          } yield assert(range)(equalTo(Chunk("hello", "goodbye")))
        },
        test("lSet error when index out of bounds") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.lPush(key, "world", "hello")
            set   <- redis.lSet(key, 2, "goodbye").either
          } yield assert(set)(isLeft)
        },
        test("lSet error when not list") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.set(key, "hello")
            set   <- redis.lSet(key, 0, "goodbye").either
          } yield assert(set)(isLeft)
        }
      ),
      suite("length")(
        test("lLen non-empty list") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.lPush(key, "world", "hello")
            len   <- redis.lLen(key)
          } yield assert(len)(equalTo(2L))
        },
        test("lLen 0 when no key") {
          for {
            redis <- ZIO.service[Redis]
            len   <- redis.lLen("unknown")
          } yield assert(len)(equalTo(0L))
        },
        test("lLen error when not list") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            value <- uuid
            _     <- redis.set(key, value)
            index <- redis.lLen(key).either
          } yield assert(index)(isLeft)
        }
      ),
      suite("range")(
        test("lRange two elements") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.lPush(key, "world", "hello")
            range <- redis.lRange(key, 0 to 1).returning[String]
          } yield assert(range)(equalTo(Chunk("hello", "world")))
        },
        test("lRange elements by exclusive range") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.lPush(key, "c", "b", "a")
            range <- redis.lRange(key, 0 until 2).returning[String]
          } yield assert(range)(equalTo(Chunk("a", "b")))
        },
        test("lRange two elements negative indices") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.lPush(key, "world", "hello")
            range <- redis.lRange(key, -2 to -1).returning[String]
          } yield assert(range)(equalTo(Chunk("hello", "world")))
        },
        test("lRange elements by exclusive range with negative indices") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.lPush(key, "d", "c", "b", "a")
            range <- redis.lRange(key, -3 until -1).returning[String]
          } yield assert(range)(equalTo(Chunk("b", "c")))
        },
        test("lRange start out of bounds") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.lPush(key, "world", "hello")
            range <- redis.lRange(key, 2 to 3).returning[String]
          } yield assert(range)(equalTo(Chunk()))
        },
        test("lRange end out of bounds") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.lPush(key, "world", "hello")
            range <- redis.lRange(key, 1 to 2).returning[String]
          } yield assert(range)(equalTo(Chunk("world")))
        },
        test("lRange error when not list") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.set(key, "hello")
            range <- redis.lRange(key, 1 to 2).returning[String].either
          } yield assert(range)(isLeft)
        }
      ),
      suite("index element")(
        test("lIndex first element") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.lPush(key, "world", "hello")
            index <- redis.lIndex(key, 0L).returning[String]
          } yield assert(index)(isSome(equalTo("hello")))
        },
        test("lIndex last element") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.lPush(key, "world", "hello")
            index <- redis.lIndex(key, -1L).returning[String]
          } yield assert(index)(isSome(equalTo("world")))
        },
        test("lIndex no existing element") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.lPush(key, "world", "hello")
            index <- redis.lIndex(key, 3).returning[String]
          } yield assert(index)(isNone)
        },
        test("lIndex error when not list") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            value <- uuid
            _     <- redis.set(key, value)
            index <- redis.lIndex(key, -1L).returning[String].either
          } yield assert(index)(isLeft)
        }
      ),
      suite("trim element")(
        test("lTrim element") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.lPush(key, "world", "hello")
            _     <- redis.lTrim(key, 0 to 0)
            range <- redis.lRange(key, 0 to -1).returning[String]
          } yield assert(range)(equalTo(Chunk("hello")))
        },
        test("lTrim start index out of bounds") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.lPush(key, "world", "hello")
            _     <- redis.lTrim(key, 2 to 5)
            range <- redis.lRange(key, 0 to 1).returning[String]
          } yield assert(range)(equalTo(Chunk()))
        },
        test("lTrim end index out of bounds") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.lPush(key, "world", "hello")
            _     <- redis.lTrim(key, 0 to 3)
            range <- redis.lRange(key, 0 to 1).returning[String]
          } yield assert(range)(equalTo(Chunk("hello", "world")))
        },
        test("lTrim error when not list") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.set(key, "hello")
            trim  <- redis.lTrim(key, 0 to 3).either
          } yield assert(trim)(isLeft)
        }
      ),
      suite("blPop")(
        test("from single list") {
          for {
            redis      <- ZIO.service[Redis]
            key        <- uuid
            _          <- redis.lPush(key, "a", "b", "c")
            fiber      <- redis.blPop(key)(1.second).returning[String].fork
            _          <- TestClock.adjust(1.second)
            popped     <- fiber.join.some
            (src, elem) = popped
          } yield assert(src)(equalTo(key)) && assert(elem)(equalTo("c"))
        },
        test("from one empty and one non-empty list") {
          for {
            redis      <- ZIO.service[Redis]
            empty      <- uuid
            nonEmpty   <- uuid
            _          <- redis.lPush(nonEmpty, "a", "b", "c")
            fiber      <- redis.blPop(empty, nonEmpty)(1.second).returning[String].fork
            _          <- TestClock.adjust(1.second)
            popped     <- fiber.join.some
            (src, elem) = popped
          } yield assert(src)(equalTo(nonEmpty)) && assert(elem)(equalTo("c"))
        },
        test("from one empty list") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            fiber  <- redis.blPop(key)(1.second).returning[String].fork
            _      <- TestClock.adjust(1.second)
            popped <- fiber.join
          } yield assert(popped)(isNone)
        },
        test("from multiple empty lists") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            fiber  <- redis.blPop(first, second)(1.second).returning[String].fork
            _      <- TestClock.adjust(1.second)
            popped <- fiber.join
          } yield assert(popped)(isNone)
        },
        test("from non-empty list with timeout 0s") {
          for {
            redis      <- ZIO.service[Redis]
            key        <- uuid
            _          <- redis.lPush(key, "a", "b", "c")
            popped     <- redis.blPop(key)(0.seconds).returning[String].some
            (src, elem) = popped
          } yield assert(src)(equalTo(key)) && assert(elem)(equalTo("c"))
        },
        test("from not list") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            value  <- uuid
            _      <- redis.set(key, value)
            popped <- redis.blPop(key)(1.second).returning[String].either
          } yield assert(popped)(isLeft(isSubtype[WrongType](anything)))
        }
      ) @@ clusterExecutorUnsupported,
      suite("brPop")(
        test("from single list") {
          for {
            redis      <- ZIO.service[Redis]
            key        <- uuid
            _          <- redis.lPush(key, "a", "b", "c")
            fiber      <- redis.brPop(key)(1.second).returning[String].fork
            _          <- TestClock.adjust(1.second)
            popped     <- fiber.join.some
            (src, elem) = popped
          } yield assert(src)(equalTo(key)) && assert(elem)(equalTo("a"))
        },
        test("from one empty and one non-empty list") {
          for {
            redis      <- ZIO.service[Redis]
            empty      <- uuid
            nonEmpty   <- uuid
            _          <- redis.lPush(nonEmpty, "a", "b", "c")
            popped     <- redis.brPop(empty, nonEmpty)(1.second).returning[String].some
            (src, elem) = popped
          } yield assert(src)(equalTo(nonEmpty)) && assert(elem)(equalTo("a"))
        },
        test("from one empty list") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            fiber  <- redis.brPop(key)(1.second).returning[String].fork
            _      <- TestClock.adjust(1.second)
            popped <- fiber.join
          } yield assert(popped)(isNone)
        },
        test("from multiple empty lists") {
          for {
            redis  <- ZIO.service[Redis]
            first  <- uuid
            second <- uuid
            fiber  <- redis.brPop(first, second)(1.second).returning[String].fork
            _      <- TestClock.adjust(1.second)
            popped <- fiber.join
          } yield assert(popped)(isNone)
        },
        test("from non-empty list with timeout 0s") {
          for {
            redis      <- ZIO.service[Redis]
            key        <- uuid
            _          <- redis.lPush(key, "a", "b", "c")
            popped     <- redis.brPop(key)(0.seconds).returning[String].some
            (src, elem) = popped
          } yield assert(src)(equalTo(key)) && assert(elem)(equalTo("a"))
        },
        test("from not list") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            value  <- uuid
            _      <- redis.set(key, value)
            popped <- redis.brPop(key)(1.second).returning[String].either
          } yield assert(popped)(isLeft(isSubtype[WrongType](anything)))
        }
      ) @@ clusterExecutorUnsupported,
      suite("lInsert")(
        test("before pivot into non-empty list") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.lPush(key, "a", "b", "c")
            len   <- redis.lInsert(key, Position.Before, "b", "d")
          } yield assert(len)(equalTo(4L))
        },
        test("after pivot into non-empty list") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.lPush(key, "a", "b", "c")
            len   <- redis.lInsert(key, Position.After, "b", "d")
          } yield assert(len)(equalTo(4L))
        },
        test("before pivot into empty list") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            len   <- redis.lInsert(key, Position.Before, "a", "b")
          } yield assert(len)(equalTo(0L))
        },
        test("after pivot into empty list") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            len   <- redis.lInsert(key, Position.After, "a", "b")
          } yield assert(len)(equalTo(0L))
        },
        test("before pivot that doesn't exist") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.lPush(key, "a", "b", "c")
            len   <- redis.lInsert(key, Position.Before, "unknown", "d")
          } yield assert(len)(equalTo(-1L))
        },
        test("after pivot that doesn't exist") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.lPush(key, "a", "b", "c")
            len   <- redis.lInsert(key, Position.After, "unknown", "d")
          } yield assert(len)(equalTo(-1L))
        },
        test("error before pivot into not list") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            value <- uuid
            _     <- redis.set(key, value)
            len   <- redis.lInsert(key, Position.Before, "a", "b").either
          } yield assert(len)(isLeft(isSubtype[WrongType](anything)))
        },
        test("error after pivot into not list") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            value <- uuid
            _     <- redis.set(key, value)
            len   <- redis.lInsert(key, Position.After, "a", "b").either
          } yield assert(len)(isLeft(isSubtype[WrongType](anything)))
        }
      ),
      suite("lMove")(
        test("move from source to destination left right") {
          for {
            redis            <- ZIO.service[Redis]
            source           <- uuid
            destination      <- uuid
            _                <- redis.rPush(source, "a", "b", "c")
            _                <- redis.rPush(destination, "d")
            moved            <- redis.lMove(source, destination, Side.Left, Side.Right).returning[String]
            sourceRange      <- redis.lRange(source, 0 to -1).returning[String]
            destinationRange <- redis.lRange(destination, 0 to -1).returning[String]
          } yield assert(moved)(isSome(equalTo("a"))) &&
            assert(sourceRange)(equalTo(Chunk("b", "c"))) && assert(destinationRange)(equalTo(Chunk("d", "a")))
        },
        test("move from source to destination right left") {
          for {
            redis            <- ZIO.service[Redis]
            source           <- uuid
            destination      <- uuid
            _                <- redis.rPush(source, "a", "b", "c")
            _                <- redis.rPush(destination, "d")
            moved            <- redis.lMove(source, destination, Side.Right, Side.Left).returning[String]
            sourceRange      <- redis.lRange(source, 0 to -1).returning[String]
            destinationRange <- redis.lRange(destination, 0 to -1).returning[String]
          } yield assert(moved)(isSome(equalTo("c"))) &&
            assert(sourceRange)(equalTo(Chunk("a", "b"))) &&
            assert(destinationRange)(equalTo(Chunk("c", "d")))
        },
        test("move from source to destination left left") {
          for {
            redis            <- ZIO.service[Redis]
            source           <- uuid
            destination      <- uuid
            _                <- redis.rPush(source, "a", "b", "c")
            _                <- redis.rPush(destination, "d")
            moved            <- redis.lMove(source, destination, Side.Left, Side.Left).returning[String]
            sourceRange      <- redis.lRange(source, 0 to -1).returning[String]
            destinationRange <- redis.lRange(destination, 0 to -1).returning[String]
          } yield assert(moved)(isSome(equalTo("a"))) &&
            assert(sourceRange)(equalTo(Chunk("b", "c"))) &&
            assert(destinationRange)(equalTo(Chunk("a", "d")))
        },
        test("move from source to destination right right") {
          for {
            redis            <- ZIO.service[Redis]
            source           <- uuid
            destination      <- uuid
            _                <- redis.rPush(source, "a", "b", "c")
            _                <- redis.rPush(destination, "d")
            moved            <- redis.lMove(source, destination, Side.Right, Side.Right).returning[String]
            sourceRange      <- redis.lRange(source, 0 to -1).returning[String]
            destinationRange <- redis.lRange(destination, 0 to -1).returning[String]
          } yield assert(moved)(isSome(equalTo("c"))) &&
            assert(sourceRange)(equalTo(Chunk("a", "b"))) &&
            assert(destinationRange)(equalTo(Chunk("d", "c")))
        },
        test("move from source to source left right") {
          for {
            redis       <- ZIO.service[Redis]
            source      <- uuid
            _           <- redis.rPush(source, "a", "b", "c")
            moved       <- redis.lMove(source, source, Side.Left, Side.Right).returning[String]
            sourceRange <- redis.lRange(source, 0 to -1).returning[String]
          } yield assert(moved)(isSome(equalTo("a"))) && assert(sourceRange)(equalTo(Chunk("b", "c", "a")))
        },
        test("move from source to source right left") {
          for {
            redis       <- ZIO.service[Redis]
            source      <- uuid
            _           <- redis.rPush(source, "a", "b", "c")
            moved       <- redis.lMove(source, source, Side.Right, Side.Left).returning[String]
            sourceRange <- redis.lRange(source, 0 to -1).returning[String]
          } yield assert(moved)(isSome(equalTo("c"))) &&
            assert(sourceRange)(equalTo(Chunk("c", "a", "b")))
        },
        test("move from source to source left left") {
          for {
            redis       <- ZIO.service[Redis]
            source      <- uuid
            _           <- redis.rPush(source, "a", "b", "c")
            moved       <- redis.lMove(source, source, Side.Left, Side.Left).returning[String]
            sourceRange <- redis.lRange(source, 0 to -1).returning[String]
          } yield assert(moved)(isSome(equalTo("a"))) &&
            assert(sourceRange)(equalTo(Chunk("a", "b", "c")))
        },
        test("move from source to source right right") {
          for {
            redis       <- ZIO.service[Redis]
            source      <- uuid
            _           <- redis.rPush(source, "a", "b", "c")
            moved       <- redis.lMove(source, source, Side.Right, Side.Right).returning[String]
            sourceRange <- redis.lRange(source, 0 to -1).returning[String]
          } yield assert(moved)(isSome(equalTo("c"))) && assert(sourceRange)(equalTo(Chunk("a", "b", "c")))
        },
        test("return nil when source dose not exist") {
          for {
            redis       <- ZIO.service[Redis]
            source      <- uuid
            destination <- uuid
            _           <- redis.rPush(destination, "d")
            moved       <- redis.lMove(source, destination, Side.Left, Side.Right).returning[String]
          } yield assert(moved)(isNone)
        }
      ) @@ clusterExecutorUnsupported,
      suite("blMove")(
        test("move from source to destination left right") {
          for {
            redis            <- ZIO.service[Redis]
            source           <- uuid
            destination      <- uuid
            _                <- redis.rPush(source, "a", "b", "c")
            _                <- redis.rPush(destination, "d")
            moved            <- redis.blMove(source, destination, Side.Left, Side.Right, 1.second).returning[String]
            sourceRange      <- redis.lRange(source, 0 to -1).returning[String]
            destinationRange <- redis.lRange(destination, 0 to -1).returning[String]
          } yield assert(moved)(isSome(equalTo("a"))) &&
            assert(sourceRange)(equalTo(Chunk("b", "c"))) &&
            assert(destinationRange)(equalTo(Chunk("d", "a")))
        },
        test("move from source to destination right left") {
          for {
            redis            <- ZIO.service[Redis]
            source           <- uuid
            destination      <- uuid
            _                <- redis.rPush(source, "a", "b", "c")
            _                <- redis.rPush(destination, "d")
            moved            <- redis.blMove(source, destination, Side.Right, Side.Left, 1.second).returning[String]
            sourceRange      <- redis.lRange(source, 0 to -1).returning[String]
            destinationRange <- redis.lRange(destination, 0 to -1).returning[String]
          } yield assert(moved)(isSome(equalTo("c"))) &&
            assert(sourceRange)(equalTo(Chunk("a", "b"))) &&
            assert(destinationRange)(equalTo(Chunk("c", "d")))
        },
        test("move from source to destination left left") {
          for {
            redis            <- ZIO.service[Redis]
            source           <- uuid
            destination      <- uuid
            _                <- redis.rPush(source, "a", "b", "c")
            _                <- redis.rPush(destination, "d")
            moved            <- redis.blMove(source, destination, Side.Left, Side.Left, 1.second).returning[String]
            sourceRange      <- redis.lRange(source, 0 to -1).returning[String]
            destinationRange <- redis.lRange(destination, 0 to -1).returning[String]
          } yield assert(moved)(isSome(equalTo("a"))) &&
            assert(sourceRange)(equalTo(Chunk("b", "c"))) &&
            assert(destinationRange)(equalTo(Chunk("a", "d")))
        },
        test("move from source to destination right right") {
          for {
            redis            <- ZIO.service[Redis]
            source           <- uuid
            destination      <- uuid
            _                <- redis.rPush(source, "a", "b", "c")
            _                <- redis.rPush(destination, "d")
            moved            <- redis.blMove(source, destination, Side.Right, Side.Right, 1.second).returning[String]
            sourceRange      <- redis.lRange(source, 0 to -1).returning[String]
            destinationRange <- redis.lRange(destination, 0 to -1).returning[String]
          } yield assert(moved)(isSome(equalTo("c"))) &&
            assert(sourceRange)(equalTo(Chunk("a", "b"))) &&
            assert(destinationRange)(equalTo(Chunk("d", "c")))
        },
        test("move from source to source left right") {
          for {
            redis       <- ZIO.service[Redis]
            source      <- uuid
            _           <- redis.rPush(source, "a", "b", "c")
            moved       <- redis.blMove(source, source, Side.Left, Side.Right, 1.second).returning[String]
            sourceRange <- redis.lRange(source, 0 to -1).returning[String]
          } yield assert(moved)(isSome(equalTo("a"))) && assert(sourceRange)(equalTo(Chunk("b", "c", "a")))
        },
        test("move from source to source right left") {
          for {
            redis       <- ZIO.service[Redis]
            source      <- uuid
            _           <- redis.rPush(source, "a", "b", "c")
            moved       <- redis.blMove(source, source, Side.Right, Side.Left, 1.second).returning[String]
            sourceRange <- redis.lRange(source, 0 to -1).returning[String]
          } yield assert(moved)(isSome(equalTo("c"))) && assert(sourceRange)(equalTo(Chunk("c", "a", "b")))
        },
        test("move from source to source left left") {
          for {
            redis       <- ZIO.service[Redis]
            source      <- uuid
            _           <- redis.rPush(source, "a", "b", "c")
            moved       <- redis.blMove(source, source, Side.Left, Side.Left, 1.second).returning[String]
            sourceRange <- redis.lRange(source, 0 to -1).returning[String]
          } yield assert(moved)(isSome(equalTo("a"))) && assert(sourceRange)(equalTo(Chunk("a", "b", "c")))
        },
        test("move from source to source right right") {
          for {
            redis       <- ZIO.service[Redis]
            source      <- uuid
            _           <- redis.rPush(source, "a", "b", "c")
            moved       <- redis.blMove(source, source, Side.Right, Side.Right, 1.second).returning[String]
            sourceRange <- redis.lRange(source, 0 to -1).returning[String]
          } yield assert(moved)(isSome(equalTo("c"))) && assert(sourceRange)(equalTo(Chunk("a", "b", "c")))
        },
        test("block until timeout reached and return nil") {
          for {
            redis       <- ZIO.service[Redis]
            source      <- uuid
            destination <- uuid
            _           <- redis.rPush(destination, "d")
            startTime   <- currentTime(TimeUnit.SECONDS)
            fiber       <- redis.blMove(source, destination, Side.Left, Side.Right, 1.second).returning[String].fork
            _           <- TestClock.adjust(1.second)
            moved       <- fiber.join
            endTime     <- currentTime(TimeUnit.SECONDS)
          } yield assert(moved)(isNone) && assert(endTime - startTime)(isGreaterThanEqualTo(1L))
        }
      ) @@ clusterExecutorUnsupported,
      suite("lPos")(
        test("find index of element") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.rPush(key, "a", "b", "c", "d", "1", "2", "3", "4", "3", "3", "3")
            idx   <- redis.lPos(key, "3")
          } yield assert(idx)(isSome(equalTo(6L)))
        },
        test("don't find index of element") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.rPush(key, "a", "b", "c", "d", "1", "2", "3", "4", "3", "3", "3")
            idx   <- redis.lPos(key, "unknown")
          } yield assert(idx)(isNone)
        },
        test("find index of element with positive rank") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.rPush(key, "a", "b", "c", "d", "1", "2", "3", "4", "3", "3", "3")
            idx   <- redis.lPos(key, "3", rank = Some(Rank(2)))
          } yield assert(idx)(isSome(equalTo(8L)))
        },
        test("find index of element with negative rank") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.rPush(key, "a", "b", "c", "d", "1", "2", "3", "4", "3", "3", "3")
            idx   <- redis.lPos(key, "3", rank = Some(Rank(-1)))
          } yield assert(idx)(isSome(equalTo(10L)))
        },
        test("find index of element with maxLen") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.rPush(key, "a", "b", "c", "d", "1", "2", "3", "4", "3", "3", "3")
            idx   <- redis.lPos(key, "3", maxLen = Some(ListMaxLen(8)))
          } yield assert(idx)(isSome(equalTo(6L)))
        },
        test("don't find index of element with maxLen") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.rPush(key, "a", "b", "c", "d", "1", "2", "3", "4", "3", "3", "3")
            idx   <- redis.lPos(key, "3", maxLen = Some(ListMaxLen(5)))
          } yield assert(idx)(isNone)
        }
      ),
      suite("lPosCount")(
        test("find index of element with rank and count") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.rPush(key, "a", "b", "c", "d", "1", "2", "3", "4", "3", "3", "3")
            idx   <- redis.lPosCount(key, "3", Count(2), rank = Some(Rank(2)))
          } yield assert(idx)(equalTo(Chunk(8L, 9L)))
        },
        test("find index of element with negative rank and count") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            _     <- redis.rPush(key, "a", "b", "c", "d", "1", "2", "3", "4", "3", "3", "3")
            idx   <- redis.lPosCount(key, "3", Count(2), rank = Some(Rank(-3)))
          } yield assert(idx)(equalTo(Chunk(8L, 6L)))
        }
      )
    )
}
