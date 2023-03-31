package zio.redis.internal

import zio.Chunk
import zio.redis._
import zio.test.Assertion._
import zio.test._

import java.nio.charset.StandardCharsets

object CRC16Spec extends BaseSpec {
  override def spec: Spec[Environment, Any] =
    suite("CRC16")(
      test("crc16") {
        val value = "123456789"
        val bytes = Chunk.fromArray(value.getBytes(StandardCharsets.UTF_8))
        assert(CRC16.get(bytes))(equalTo(0x31c3))
      }
    )
}
