package zio.redis
import java.nio.charset.StandardCharsets

import zio.Chunk
import zio.stream.Stream
import zio.test.Assertion._
import zio.test._

object RespValueSpec extends BaseSpec {

  private def encode(s: String) = Chunk.fromArray(s.getBytes(StandardCharsets.UTF_8))

  override def spec: Spec[Any, TestFailure[RedisError.ProtocolError], TestSuccess] =
    suite("RespValue")(
      suite("serialization")(
        test("array") {
          val expected = encode("*3\r\n$3\r\nabc\r\n:123\r\n$-1\r\n")
          val v        = RespValue.array(RespValue.bulkString("abc"), RespValue.Integer(123), RespValue.NullValue)
          assert(v.serialize)(equalTo(expected))
        }
      ),
      suite("deserialization")(
        testM("simple string") {
          val s     = "Hello, world."
          val bytes = Chunk.fromArray((s + "\r\n").getBytes(StandardCharsets.UTF_8))
          Stream.fromChunk(bytes).run(RespValue.SimpleStringDeserializer).map(assert(_)(equalTo(s)))
        },
        testM("an array") {
          val value = RespValue.array(
            RespValue.bulkString("hi there"),
            RespValue.Integer(42L),
            RespValue.NullValue,
            RespValue.bulkString("last")
          )
          Stream.fromChunk(value.serialize).run(RespValue.Deserializer).map(assert(_)(equalTo(value)))
        },
        testM("a bulk string") {
          val bytes = encode("$6\r\nfoobar\r\n$4\r\ntest\r\n")
          Stream.fromChunk(bytes).run(RespValue.Deserializer).map(assert(_)(equalTo(RespValue.bulkString("foobar"))))
        },
        testM("as transducer") {
          val values = Chunk(
            RespValue.SimpleString("OK"),
            RespValue.bulkString("test1"),
            RespValue.array(RespValue.Integer(42L), RespValue.bulkString("in array"))
          )
          val bytes  = values.flatMap(_.serialize)
          Stream
            .fromChunk(bytes)
            .transduce(RespValue.Deserializer.toTransducer)
            .runCollect
            .map(assert(_)(equalTo(values)))
        }
      )
    )
}
