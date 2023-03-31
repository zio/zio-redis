package zio.redis.internal

import zio.Chunk
import zio.redis._
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test._

object RespValueSpec extends BaseSpec {
  def spec: Spec[Any, RedisError.ProtocolError] =
    suite("RespValue")(
      test("serializes and deserializes messages") {
        val values = Chunk(
          RespValue.SimpleString("OK"),
          RespValue.bulkString("test1"),
          RespValue.array(
            RespValue.bulkString("test1"),
            RespValue.Integer(42L),
            RespValue.NullBulkString,
            RespValue.array(RespValue.SimpleString("a"), RespValue.Integer(0L)),
            RespValue.bulkString("in array"),
            RespValue.SimpleString("test2")
          ),
          RespValue.NullBulkString
        )

        ZStream
          .fromChunk(values)
          .mapConcat(_.serialize)
          .via(RespValue.Decoder)
          .collectSome
          .runCollect
          .map(assert(_)(equalTo(values)))
      }
    )
}
