package zio.redis

import zio.Chunk
import zio.clock.Clock
import zio.logging.Logging
import zio.redis.Input.{ LongInput, StringInput }
import zio.redis.Output.MultiStringChunkOutput
import zio.redis.RedisError.ProtocolError
import zio.test.Assertion._
import zio.test.{ testM, _ }

object ScriptingSpec extends BaseSpec {
  def spec =
    suite("scripting")(
      suite("eval")(
        testM("take strings return strings") {
          for {
            key1  <- uuid
            key2  <- uuid
            arg1  <- uuid
            arg2  <- uuid
            lua    = """return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}"""
            script = Script(lua, Seq(key1, key2), Seq(arg1, arg2))
            res   <- eval(script)(StringInput, StringInput, MultiStringChunkOutput)
          } yield assert(res)(equalTo(Chunk(key1, key2, arg1, arg2)))
        },
        testM("return custom data type") {

          case class CustomData(count: Long, avg: Long, pair: (Int, String))

          implicit val longInput = LongInput
          implicit val customDataOutput = new Output[CustomData] {
            val tryDecodeLong: RespValue => Long = {
              case RespValue.Integer(value) => value
              case other                    => throw ProtocolError(s"$other isn't a integer type")
            }
            val tryDecodeString: RespValue => String = {
              case s @ RespValue.BulkString(_) => s.asString
              case other                       => throw ProtocolError(s"$other isn't a integer type")
            }

            override protected def tryDecode(respValue: RespValue): CustomData =
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
          }

          val lua      = """return {1,2,{3,'Hello World!'}}"""
          val script   = Script[Long, Long](lua, Seq.empty, Seq.empty)
          val expected = CustomData(1, 2, (3, "Hello World!"))

          for {
            res <- eval(script)
          } yield assert(res)(equalTo(expected))
        }
      )
    ).provideCustomLayerShared((Logging.ignore >>> RedisExecutor.local.orDie) ++ Clock.live)
}
