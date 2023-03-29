package zio.redis

import zio.test.Assertion.{equalTo, isSome}
import zio.test.{Spec, assert}
import zio.{Chunk, ZIO}

import java.nio.charset.StandardCharsets

object RedisConnectionSpec extends BaseSpec {
  override def spec: Spec[Environment, Any] =
    suite("Redis Connection Byte stream")(
      test("can write and read") {
        for {
          stream <- ZIO.service[RedisConnection]
          data    = Chunk.fromArray("*2\r\n$7\r\nCOMMAND\r\n$4\r\nINFO\r\n$3\r\nGET\r\n".getBytes(StandardCharsets.UTF_8))
          _      <- stream.write(data)
          res    <- stream.read.runHead
        } yield assert(res)(isSome(equalTo('*'.toByte)))
      }
    ).provideLayer(RedisConnection.local)
}
