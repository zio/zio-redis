package zio.redis.internal

import zio.redis._
import zio.redis.internal.RespCommandArgument
import zio.test._

object RespCommandArgumentSpec extends BaseSpec {
  def spec: Spec[Any, RedisError.ProtocolError] =
    suite("RespArgument")(
      suite("BulkString.asCRC16")(
        test("key without braces") {
          val key = RespCommandArgument.Key("hello world")
          assertTrue(15332 == key.asCRC16)
        },
        test("key between braces") {
          val key = RespCommandArgument.Key("hello{key1}wor}ld")
          assertTrue(41957 == key.asCRC16)
        },
        test("empty key between braces") {
          val key = RespCommandArgument.Key("hello{}world")
          assertTrue(40253 == key.asCRC16)
        }
      )
    )
}
