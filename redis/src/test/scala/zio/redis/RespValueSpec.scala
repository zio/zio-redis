package zio.redis
import java.nio.charset.StandardCharsets

import zio.Chunk
import zio.stream.Stream
import zio.test.Assertion._
import zio.test._

object RespValueSpec extends BaseSpec {

  def spec: Spec[Any, TestFailure[RedisError.ProtocolError], TestSuccess] =
    suite("RespValue")(
      suite("serialization")(
        test("array") {
          val expected = Chunk.fromArray("*3\r\n$3\r\nabc\r\n:123\r\n$-1\r\n".getBytes(StandardCharsets.UTF_8))
          val v        = RespValue.array(RespValue.bulkString("abc"), RespValue.Integer(123), RespValue.NullValue)
          assert(v.serialize)(equalTo(expected))
        }
      ),
      testM("deserialization") {
        val values = Chunk(
          RespValue.SimpleString("OK"),
          RespValue.bulkString("test1"),
          RespValue.array(RespValue.Integer(42L), RespValue.bulkString("in array"))
        )

        val bytes = values.flatMap(_.serialize)

        Stream
          .succeed(bytes)
          .transduce(RespValue.Deserializer)
          .runCollect
          .map(assert(_)(equalTo(values)))
      }
    )
}
