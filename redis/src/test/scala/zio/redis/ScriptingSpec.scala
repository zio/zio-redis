package zio.redis

import zio._
import zio.redis.RedisError._
import zio.test.Assertion._
import zio.test.TestAspect.ignore
import zio.test._

trait ScriptingSpec extends BaseSpec {
  val scriptingSpec: Spec[Annotations with RedisExecutor, TestFailure[Any], TestSuccess] =
    suite("scripting")(
      suite("eval")(
        testM("put boolean and return existence of key") {
          import ScriptingSpec.booleanDecoder
          for {
            key <- uuid
            arg  = true
            lua =
              """
                |redis.call('set',KEYS[1],ARGV[1])
                |return redis.call('exists',KEYS[1])
              """.stripMargin
            res <- eval(lua, Chunk(key), Chunk(arg))
          } yield assert(res)(equalTo(true))
        },
        testM("take strings return strings") {
          import ScriptingSpec.chunkStringDecoder
          for {
            key1 <- uuid
            key2 <- uuid
            arg1 <- uuid
            arg2 <- uuid
            lua   = """return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}"""
            res  <- eval(lua, Chunk(key1, key2), Chunk(arg1, arg2))
          } yield assert(res)(equalTo(Chunk(key1, key2, arg1, arg2)))
        },
        testM("return custom data type") {
          import ScriptingSpec.CustomData
          val lua      = """return {1,2,{3,'Hello World!'}}"""
          val expected = CustomData(1, 2, (3, "Hello World!"))
          for {
            res <- eval[Long, Long, CustomData](lua, Chunk.empty, Chunk.empty)
          } yield assert(res)(equalTo(expected))
        },
        testM("throw an error when incorrect script's sent") {
          import ScriptingSpec.simpleStringDecoder
          for {
            key  <- uuid
            arg  <- uuid
            lua   = ";"
            error = "Error compiling script (new function): user_script:1: unexpected symbol near ';'"
            res  <- eval(lua, Chunk(key), Chunk(arg)).either
          } yield assert(res)(isLeft(isSubtype[ProtocolError](hasField("message", _.message, equalTo(error)))))
        },
        testM("throw an error if couldn't decode resp value") {
          import ScriptingSpec.decoderWithError
          val customError                       = "custom error"
          implicit val decoder: Decoder[String] = decoderWithError(customError)
          for {
            key <- uuid
            arg <- uuid
            lua  = ""
            res <- eval(lua, Chunk(key), Chunk(arg)).either
          } yield assert(res)(isLeft(isSubtype[ProtocolError](hasField("message", _.message, equalTo(customError)))))
        },
        testM("throw custom error from script") {
          import ScriptingSpec.simpleStringDecoder
          for {
            key    <- uuid
            arg    <- uuid
            myError = "My Error"
            lua     = s"""return redis.error_reply("${myError}")"""
            res    <- eval(lua, Chunk(key), Chunk(arg)).either
          } yield assert(res)(isLeft(isSubtype[ProtocolError](hasField("message", _.message, equalTo(myError)))))
        }
      ),
      suite("evalSHA")(
        testM("put boolean and return existence of key") {
          import ScriptingSpec.booleanDecoder
          for {
            key <- uuid
            arg  = true
            lua =
              """
                |redis.call('set',KEYS[1],ARGV[1])
                |return redis.call('exists',KEYS[1])
              """.stripMargin
            sha <- scriptLoad(lua)
            res <- evalSHA(sha, Chunk(key), Chunk(arg))
          } yield assert(res)(equalTo(true))
        },
        testM("take strings return strings") {
          import ScriptingSpec.chunkStringDecoder
          for {
            key1 <- uuid
            key2 <- uuid
            arg1 <- uuid
            arg2 <- uuid
            lua   = """return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}"""
            sha  <- scriptLoad(lua)
            res  <- evalSHA(sha, Chunk(key1, key2), Chunk(arg1, arg2))
          } yield assert(res)(equalTo(Chunk(key1, key2, arg1, arg2)))
        },
        testM("return custom data type") {
          import ScriptingSpec.CustomData
          val lua      = """return {1,2,{3,'Hello World!'}}"""
          val expected = CustomData(1, 2, (3, "Hello World!"))
          for {
            res <- eval[Long, Long, CustomData](lua, Chunk.empty, Chunk.empty)
          } yield assert(res)(equalTo(expected))
        },
        testM("throw an error if couldn't decode resp value") {
          import ScriptingSpec.decoderWithError
          val customError                       = "custom error"
          implicit val decoder: Decoder[String] = decoderWithError(customError)
          for {
            key <- uuid
            arg <- uuid
            lua  = ""
            sha <- scriptLoad(lua)
            res <- evalSHA(sha, Chunk(key), Chunk(arg)).either
          } yield assert(res)(isLeft(isSubtype[ProtocolError](hasField("message", _.message, equalTo(customError)))))
        },
        testM("throw custom error from script") {
          import ScriptingSpec.simpleStringDecoder
          for {
            key    <- uuid
            arg    <- uuid
            myError = "My Error"
            lua     = s"""return redis.error_reply("${myError}")"""
            sha    <- scriptLoad(lua)
            res    <- evalSHA(sha, Chunk(key), Chunk(arg)).either
          } yield assert(res)(isLeft(isSubtype[ProtocolError](hasField("message", _.message, equalTo(myError)))))
        },
        testM("throw NoScript error if script isn't found in cache") {
          import ScriptingSpec.simpleStringDecoder
          val lua   = """return "1""""
          val error = "No matching script. Please use EVAL."
          for {
            res <- evalSHA[String, String, String](lua, Chunk.empty, Chunk.empty).either
          } yield assert(res)(isLeft(isSubtype[NoScript](hasField("message", _.message, equalTo(error)))))
        }
      ),
      suite("scriptDebug")(
        testM("turn on sync mode and turn it off") {
          for {
            resSync <- scriptDebug(DebugMode.Sync).either
            resNo   <- scriptDebug(DebugMode.No).either
          } yield assert(resSync)(isRight) && assert(resNo)(isRight)
        } @@ ignore,
        testM("turn on async mode and turn it off") {
          for {
            resYes <- scriptDebug(DebugMode.Yes).either
            resNo  <- scriptDebug(DebugMode.No).either
          } yield assert(resYes)(isRight) && assert(resNo)(isRight)
        } @@ ignore
      ),
      suite("scriptExists")(
        testM("return true if scripts are found in the cache") {
          val lua1 = """return "1""""
          val lua2 = """return "2""""
          for {
            sha1 <- scriptLoad(lua1)
            sha2 <- scriptLoad(lua2)
            res  <- scriptExists(sha1, sha2)
          } yield assert(res)(equalTo(Chunk(true, true)))
        },
        testM("return false if scripts aren't found in the cache") {
          val lua1 = """return "1""""
          val lua2 = """return "2""""
          for {
            res <- scriptExists(lua1, lua2)
          } yield assert(res)(equalTo(Chunk(false, false)))
        }
      ),
      suite("scriptFlush")(
        testM("correct flushes the scripts cache") {
          val lua1 = """return "1""""
          for {
            sha1       <- scriptLoad(lua1)
            existence1 <- scriptExists(sha1)
            _          <- scriptFlush()
            existence2 <- scriptExists(sha1)
          } yield assert(existence1)(equalTo(Chunk(true))) && assert(existence2)(equalTo(Chunk(false)))
        } @@ ignore
      ),
      suite("scriptKill")(
        testM("correctly kills the scripts that in execution right now") {
          import ScriptingSpec.simpleStringDecoder
          val lua =
            """
              |while true do
              |end
            """.stripMargin
          for {
            fiber   <- eval[String, String, String](lua, Chunk.empty, Chunk.empty).run.fork
            killRes <- scriptKill().either
            evalRes <- fiber.join
          } yield assert(killRes)(isRight) && assert(evalRes)(dies(anything))
        } @@ ignore,
        testM("throw NotBusy error if no scripts in execution right now") {
          val error = "No scripts in execution right now."
          for {
            res <- scriptKill().either
          } yield assert(res)(isLeft(isSubtype[NotBusy](hasField("message", _.message, equalTo(error)))))
        }
      ),
      suite("scriptLoad")(
        testM("return OK") {
          val lua = """return "1""""
          for {
            sha <- scriptLoad(lua)
          } yield assert(sha)(isSubtype[String](anything))
        },
        testM("throw an error when incorrect script was sent") {
          val lua   = ";"
          val error = "Error compiling script (new function): user_script:1: unexpected symbol near ';'"
          for {
            sha <- scriptLoad(lua).either
          } yield assert(sha)(isLeft(isSubtype[ProtocolError](hasField("message", _.message, equalTo(error)))))
        }
      )
    )
}

object ScriptingSpec {
  final case class CustomData(count: Long, avg: Long, pair: (Int, String))

  object CustomData {
    implicit val decoder: Decoder[CustomData] = new Decoder[CustomData] {
      val tryDecodeLong: RespValue => Long = {
        case RespValue.Integer(value) => value
        case other                    => throw ProtocolError(s"$other isn't a integer type")
      }
      val tryDecodeString: RespValue => String = {
        case s @ RespValue.BulkString(_) => s.asString
        case other                       => throw ProtocolError(s"$other isn't a integer type")
      }

      override def decode(respValue: RespValue): IO[RedisError, CustomData] = IO.effect {
        respValue match {
          case RespValue.Array(elements) =>
            val count = tryDecodeLong(elements(0))
            val avg   = tryDecodeLong(elements(1))
            val pair = elements(2) match {
              case RespValue.Array(elements) => (tryDecodeLong(elements(0)).toInt, tryDecodeString(elements(1)))
              case other                     => throw ProtocolError(s"$other isn't an array type")
            }
            CustomData(count, avg, pair)
          case other => throw ProtocolError(s"$other isn't an array type")
        }
      }.refineToOrDie[RedisError]
    }
  }

  implicit val booleanDecoder: Decoder[Boolean] = {
    case RespValue.Integer(0) => IO.succeedNow(false)
    case RespValue.Integer(1) => IO.succeedNow(true)
    case other                => IO.fail(ProtocolError(s"$other isn't a string nor an array"))
  }

  implicit val simpleStringDecoder: Decoder[String] = {
    case RespValue.SimpleString(value) => IO.succeedNow(value)
    case other                         => IO.fail(ProtocolError(s"$other isn't a string nor an array"))
  }

  implicit val chunkStringDecoder: Decoder[Chunk[String]] = {
    case RespValue.Array(elements) =>
      ZIO.foreach(elements) {
        case s @ RespValue.BulkString(_) => IO.succeed(s.asString)
        case other                       => IO.fail(ProtocolError(s"$other isn't a bulk string"))
      }
    case other => IO.fail(ProtocolError(s"$other isn't a string nor an array"))
  }

  def decoderWithError(error: String): Decoder[String] = { _ =>
    IO.fail(ProtocolError(error))
  }
}
