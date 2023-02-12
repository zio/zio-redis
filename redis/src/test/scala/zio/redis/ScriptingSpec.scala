package zio.redis

import zio._
import zio.redis.Input.{BoolInput, ByteInput, LongInput, StringInput}
import zio.redis.Output._
import zio.redis.RedisError._
import zio.redis.ScriptingSpec._
import zio.test.Assertion._
import zio.test._

import scala.util.Random

trait ScriptingSpec extends BaseSpec {
  def scriptingSpec: Spec[Redis, RedisError] =
    suite("scripting")(
      suite("eval")(
        test("put boolean and return existence of key") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            arg    = true
            lua =
              """
                |redis.call('set',KEYS[1],ARGV[1])
                |return redis.call('exists',KEYS[1])
              """.stripMargin
            res <- redis.eval(lua, Chunk(key), Chunk(arg)).returning[Boolean]
          } yield assertTrue(res)
        },
        test("take strings return strings") {
          for {
            redis   <- ZIO.service[Redis]
            keyHash <- uuid
            key1    <- uuid.map(_ + s"{$keyHash}")
            key2    <- uuid.map(_ + s"{$keyHash}")
            arg1    <- uuid
            arg2    <- uuid
            lua      = """return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}"""
            res     <- redis.eval(lua, Chunk(key1, key2), Chunk(arg1, arg2)).returning[Chunk[String]]
          } yield assertTrue(res == Chunk(key1, key2, arg1, arg2))
        },
        test("put custom input value return custom input value") {
          for {
            redis   <- ZIO.service[Redis]
            keyHash <- uuid
            key1    <- uuid.map(_ + s"{$keyHash}")
            key2    <- uuid.map(_ + s"{$keyHash}")
            arg1    <- uuid
            arg2    <- ZIO.succeed(Random.nextLong())
            arg      = CustomInputValue(arg1, arg2)
            lua      = """return {ARGV[1],ARGV[2]}"""
            res     <- redis.eval(lua, Chunk(key1, key2), Chunk(arg)).returning[Map[String, String]]
          } yield assertTrue(res == Map(arg1 -> arg2.toString))
        },
        test("return custom data type") {
          val lua                     = """return {1,2,{3,'Hello World!'}}"""
          val expected                = CustomData(1, 2, (3, "Hello World!"))
          val emptyInput: Chunk[Long] = Chunk.empty
          for {
            redis <- ZIO.service[Redis]
            res   <- redis.eval(lua, emptyInput, emptyInput).returning[CustomData]
          } yield assertTrue(res == expected)
        },
        test("throw an error when incorrect script's sent") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            arg   <- uuid
            lua    = ";"
            error  = "Error compiling script (new function): user_script:1: unexpected symbol near ';'"
            res   <- redis.eval(lua, Chunk(key), Chunk(arg)).returning[String].either
          } yield assert(res)(isLeft(isSubtype[ProtocolError](hasField("message", _.message, equalTo(error)))))
        },
        test("throw an error if couldn't decode resp value") {
          val customError                      = "custom error"
          implicit val decoder: Output[String] = errorOutput(customError)
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            arg   <- uuid
            lua    = ""
            res   <- redis.eval(lua, Chunk(key), Chunk(arg)).returning[String](decoder).either
          } yield assert(res)(isLeft(isSubtype[ProtocolError](hasField("message", _.message, equalTo(customError)))))
        },
        test("throw custom error from script") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            arg    <- uuid
            myError = "My Error"
            lua     = s"""return redis.error_reply("${myError}")"""
            res    <- redis.eval(lua, Chunk(key), Chunk(arg)).returning[String].either
          } yield assert(res)(isLeft(isSubtype[ProtocolError](hasField("message", _.message, equalTo(myError)))))
        }
      ),
      suite("evalSHA")(
        test("put boolean and return existence of key") {
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            arg    = true
            lua =
              """
                |redis.call('set',KEYS[1],ARGV[1])
                |return redis.call('exists',KEYS[1])
              """.stripMargin
            sha <- redis.scriptLoad(lua)
            res <- redis.evalSha(sha, Chunk(key), Chunk(arg)).returning[Boolean]
          } yield assertTrue(res)
        },
        test("take strings return strings") {
          for {
            redis   <- ZIO.service[Redis]
            keyHash <- uuid
            key1    <- uuid.map(_ + s"{$keyHash}")
            key2    <- uuid.map(_ + s"{$keyHash}")
            arg1    <- uuid
            arg2    <- uuid
            lua      = """return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}"""
            sha     <- redis.scriptLoad(lua)
            res     <- redis.evalSha(sha, Chunk(key1, key2), Chunk(arg1, arg2)).returning[Chunk[String]]
          } yield assertTrue(res == Chunk(key1, key2, arg1, arg2))
        },
        test("return custom data type") {
          val lua                       = """return {1,2,{3,'Hello World!'}}"""
          val expected                  = CustomData(1, 2, (3, "Hello World!"))
          val emptyInput: Chunk[String] = Chunk.empty
          for {
            redis <- ZIO.service[Redis]
            res   <- redis.eval(lua, emptyInput, emptyInput).returning[CustomData]
          } yield assertTrue(res == expected)
        },
        test("throw an error if couldn't decode resp value") {
          val customError                      = "custom error"
          implicit val decoder: Output[String] = errorOutput(customError)
          for {
            redis <- ZIO.service[Redis]
            key   <- uuid
            arg   <- uuid
            lua    = ""
            sha   <- redis.scriptLoad(lua)
            res   <- redis.evalSha(sha, Chunk(key), Chunk(arg)).returning[String](decoder).either
          } yield assert(res)(isLeft(isSubtype[ProtocolError](hasField("message", _.message, equalTo(customError)))))
        },
        test("throw custom error from script") {
          for {
            redis  <- ZIO.service[Redis]
            key    <- uuid
            arg    <- uuid
            myError = "My Error"
            lua     = s"""return redis.error_reply("${myError}")"""
            sha    <- redis.scriptLoad(lua)
            res    <- redis.evalSha(sha, Chunk(key), Chunk(arg)).returning[String].either
          } yield assert(res)(isLeft(isSubtype[ProtocolError](hasField("message", _.message, equalTo(myError)))))
        },
        test("throw NoScript error if script isn't found in cache") {
          val lua                       = """return "1""""
          val error                     = "No matching script. Please use EVAL."
          val emptyInput: Chunk[String] = Chunk.empty
          for {
            redis <- ZIO.service[Redis]
            res   <- redis.evalSha(lua, emptyInput, emptyInput).returning[String].either
          } yield assert(res)(isLeft(isSubtype[NoScript](hasField("message", _.message, equalTo(error)))))
        }
      ),
      suite("scriptDebug")(
        test("enable non-blocking asynchronous debugging") {
          for {
            redis <- ZIO.service[Redis]
            res   <- redis.scriptDebug(DebugMode.Yes)
          } yield assert(res)(isUnit)
        },
        test("enable blocking synchronous debugging") {
          for {
            redis <- ZIO.service[Redis]
            res   <- redis.scriptDebug(DebugMode.Sync)
          } yield assert(res)(isUnit)
        },
        test("disable debug mode") {
          for {
            redis <- ZIO.service[Redis]
            res   <- redis.scriptDebug(DebugMode.No)
          } yield assert(res)(isUnit)
        }
      ),
      suite("scriptExists")(
        test("return true if scripts are found in the cache") {
          val lua1 = """return "1""""
          val lua2 = """return "2""""
          for {
            redis <- ZIO.service[Redis]
            sha1  <- redis.scriptLoad(lua1)
            sha2  <- redis.scriptLoad(lua2)
            res   <- redis.scriptExists(sha1, sha2)
          } yield assertTrue(res == Chunk(true, true))
        },
        test("return false if scripts aren't found in the cache") {
          val lua1 = """return "1""""
          val lua2 = """return "2""""
          for {
            redis <- ZIO.service[Redis]
            res   <- redis.scriptExists(lua1, lua2)
          } yield assertTrue(res == Chunk(false, false))
        }
      ),
      suite("scriptFlush")(
        test("flush scripts in default mode") {
          val lua1 = """return "1""""
          val lua2 = """return "2""""
          for {
            redis <- ZIO.service[Redis]
            sha1  <- redis.scriptLoad(lua1)
            sha2  <- redis.scriptLoad(lua2)
            _     <- redis.scriptFlush()
            res   <- redis.scriptExists(sha1, sha2)
          } yield assertTrue(res == Chunk(false, false))
        },
        test("flush scripts in SYNC mode") {
          val lua1 = """return "1""""
          val lua2 = """return "2""""
          for {
            redis <- ZIO.service[Redis]
            sha1  <- redis.scriptLoad(lua1)
            sha2  <- redis.scriptLoad(lua2)
            _     <- redis.scriptFlush(mode = Some(FlushMode.Sync))
            res   <- redis.scriptExists(sha1, sha2)
          } yield assertTrue(res == Chunk(false, false))
        },
        test("flush scripts in ASYNC mode") {
          val lua1 = """return "1""""
          val lua2 = """return "2""""
          for {
            redis <- ZIO.service[Redis]
            sha1  <- redis.scriptLoad(lua1)
            sha2  <- redis.scriptLoad(lua2)
            _     <- redis.scriptFlush(mode = Some(FlushMode.Async))
            res   <- redis.scriptExists(sha1, sha2)
          } yield assertTrue(res == Chunk(false, false))
        }
      ),
      suite("scriptKill")(
        test("return NOTBUSY when there is no scripts in execution") {
          for {
            redis <- ZIO.service[Redis]
            res   <- redis.scriptKill.either
          } yield assert(res)(isLeft(isSubtype[RedisError.NotBusy](anything)))
        }
      ),
      suite("scriptLoad")(
        test("return OK") {
          val lua = """return "1""""
          for {
            redis <- ZIO.service[Redis]
            sha   <- redis.scriptLoad(lua)
          } yield assert(sha)(isSubtype[String](anything))
        },
        test("throw an error when incorrect script was sent") {
          val lua   = ";"
          val error = "Error compiling script (new function): user_script:1: unexpected symbol near ';'"
          for {
            redis <- ZIO.service[Redis]
            sha   <- redis.scriptLoad(lua).either
          } yield assert(sha)(isLeft(isSubtype[ProtocolError](hasField("message", _.message, equalTo(error)))))
        }
      )
    )
}

object ScriptingSpec {

  final case class CustomInputValue(name: String, age: Long)

  object CustomInputValue {
    implicit val encoder: Input[CustomInputValue] = Input.Tuple2(StringInput, LongInput).contramap { civ =>
      (civ.name, civ.age)
    }
  }

  final case class CustomData(count: Long, avg: Long, pair: (Int, String))

  object CustomData {
    implicit val decoder: Output[CustomData] = RespValueOutput.map {
      case RespValue.Array(elements) =>
        val count = RespToLong(elements(0))
        val avg   = RespToLong(elements(1))
        val pair = elements(2) match {
          case RespValue.Array(elements) => (RespToLong(elements(0)).toInt, RespToString(elements(1)))
          case other                     => throw ProtocolError(s"$other isn't an array type")
        }
        CustomData(count, avg, pair)
      case other => throw ProtocolError(s"$other isn't an array type")
    }
  }

  private val RespToLong: RespValue => Long = {
    case RespValue.Integer(value) => value
    case other                    => throw ProtocolError(s"$other isn't a integer type")
  }
  private val RespToString: RespValue => String = {
    case s @ RespValue.BulkString(_) => s.asString
    case other                       => throw ProtocolError(s"$other isn't a string type")
  }

  implicit val bytesEncoder: Input[Chunk[Byte]] = ByteInput
  implicit val booleanInput: Input[Boolean]     = BoolInput
  implicit val stringInput: Input[String]       = StringInput
  implicit val longInput: Input[Long]           = LongInput

  implicit val keyValueOutput: KeyValueOutput[String, String] = KeyValueOutput(MultiStringOutput, MultiStringOutput)
  implicit val booleanOutput: Output[Boolean] = RespValueOutput.map {
    case RespValue.Integer(0) => false
    case RespValue.Integer(1) => true
    case other                => throw ProtocolError(s"$other isn't a string nor an array")
  }
  implicit val simpleStringOutput: Output[String] = RespValueOutput.map {
    case RespValue.SimpleString(value) => value
    case other                         => throw ProtocolError(s"$other isn't a string nor an array")
  }
  implicit val chunkStringOutput: Output[Chunk[String]] = RespValueOutput.map {
    case RespValue.Array(elements) =>
      elements.map {
        case s @ RespValue.BulkString(_) => s.asString
        case other                       => throw ProtocolError(s"$other isn't a bulk string")
      }
    case other => throw ProtocolError(s"$other isn't a string nor an array")
  }

  def errorOutput(error: String): Output[String] = RespValueOutput.map { _ =>
    throw ProtocolError(error)
  }
}
