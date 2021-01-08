package zio.redis

import zio._
import zio.redis.RedisEncoder._
import zio.redis.RedisError.ProtocolError
import zio.test.Assertion._
import zio.test._

trait ScriptingSpec extends BaseSpec {
  val scriptingSpec =
    suite("scripting")(
      suite("eval")(
        testM("put boolean and return existence of key") {
          implicit val decoder: RedisDecoder[Boolean] = {
            case RespValue.Integer(0) => IO.succeedNow(false)
            case RespValue.Integer(1) => IO.succeedNow(true)
            case other                => IO.fail(ProtocolError(s"$other isn't a string nor an array"))
          }

          for {
            key <- uuid
            arg  = true
            lua =
              """
                |redis.call('set',KEYS[1],ARGV[1])
                |return redis.call('exists',KEYS[1])
              """.stripMargin
            script = Script(lua, Seq(key), Seq(arg))
            res   <- eval(script)
          } yield assert(res)(equalTo(true))
        },
        testM("take strings return strings") {
          implicit val decoder: RedisDecoder[Chunk[String]] = {
            case RespValue.Array(elements) =>
              ZIO.foreach(elements) {
                case s @ RespValue.BulkString(_) => IO.succeed(s.asString)
                case other                       => IO.fail(ProtocolError(s"$other isn't a bulk string"))
              }
            case other => IO.fail(ProtocolError(s"$other isn't a string nor an array"))
          }

          for {
            key1  <- uuid
            key2  <- uuid
            arg1  <- uuid
            arg2  <- uuid
            lua    = """return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}"""
            script = Script(lua, Seq(key1, key2), Seq(arg1, arg2))
            res   <- eval(script)
          } yield assert(res)(equalTo(Chunk(key1, key2, arg1, arg2)))
        },
        testM("return custom data type") {

          final case class CustomData(count: Long, avg: Long, pair: (Int, String))

          implicit val decoder: RedisDecoder[CustomData] = new RedisDecoder[CustomData] {
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

          val lua      = """return {1,2,{3,'Hello World!'}}"""
          val script   = Script[Long, Long](lua, Seq.empty, Seq.empty)
          val expected = CustomData(1, 2, (3, "Hello World!"))

          for {
            res <- eval(script)
          } yield assert(res)(equalTo(expected))
        },
        testM("throw an error when incorrect script's sent") {
          implicit val decoder: RedisDecoder[String] = {
            case RespValue.SimpleString(value) => IO.succeedNow(value)
            case other                         => IO.fail(ProtocolError(s"$other isn't a string nor an array"))
          }

          for {
            key   <- uuid
            arg   <- uuid
            lua    = ";"
            error  = "Error compiling script (new function): user_script:1: unexpected symbol near ';'"
            script = Script(lua, Seq(key), Seq(arg))
            res   <- eval(script).either
          } yield assert(res)(isLeft(isSubtype[ProtocolError](hasField("message", _.message, equalTo(error)))))
        },
        testM("throw an error if couldn't decode resp value") {
          val customError = "custom error"
          implicit val decoder: RedisDecoder[String] = { _ =>
            IO.fail(ProtocolError(customError))
          }

          for {
            key   <- uuid
            arg   <- uuid
            lua    = ""
            script = Script(lua, Seq(key), Seq(arg))
            res   <- eval(script).either
          } yield assert(res)(isLeft(isSubtype[ProtocolError](hasField("message", _.message, equalTo(customError)))))
        },
        testM("throw custom error from script") {
          implicit val decoder: RedisDecoder[String] = {
            case RespValue.SimpleString(value) => IO.succeedNow(value)
            case other                         => IO.fail(ProtocolError(s"$other isn't a string nor an array"))
          }

          for {
            key    <- uuid
            arg    <- uuid
            myError = "My Error"
            lua     = s"""return redis.error_reply("${myError}")"""
            script  = Script(lua, Seq(key), Seq(arg))
            res    <- eval(script).either
          } yield assert(res)(isLeft(isSubtype[ProtocolError](hasField("message", _.message, equalTo(myError)))))
        }
      )
    )
}
