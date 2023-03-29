package zio.redis.internal

import zio.Chunk
import zio.redis._
import zio.redis.internal.RespValue
import zio.test.Assertion._
import zio.test._

import java.nio.charset.StandardCharsets

object RespValueSpec extends BaseSpec {
  def spec: Spec[Any, RedisError.ProtocolError] =
    suite("RespValue")(
      suite("serialization")(
        test("array") {
          val expected = Chunk.fromArray("*3\r\n$3\r\nabc\r\n:123\r\n$-1\r\n".getBytes(StandardCharsets.UTF_8))
          val v        = RespValue.array(RespValue.bulkString("abc"), RespValue.Integer(123), RespValue.NullBulkString)
          assert(v.serialize)(equalTo(expected))
        }
      ),
      suite("deserialization")(
        test("array") {
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

          zio.stream.ZStream
            .fromChunk(values)
            .mapConcat(_.serialize)
            .via(RespValue.Decoder)
            .collect { case Some(value) =>
              value
            }
            .runCollect
            .map(assert(_)(equalTo(values)))
        }
      )
    )
}
