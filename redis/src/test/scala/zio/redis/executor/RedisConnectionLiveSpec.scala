package zio.redis.executor

import zio.logging.Logging
import zio.redis.BaseSpec
import zio.test.Assertion.{equalTo, isSome}
import zio.test.{ZSpec, assert}
import zio.{Chunk, ZIO}

import java.nio.charset.StandardCharsets

object RedisConnectionLiveSpec extends BaseSpec {
  override def spec: ZSpec[Environment, Failure] =
    suite("Byte stream")(
      test("can write and read") {
        for {
          stream <- ZIO.service[RedisConnection]
          data    = Chunk.fromArray("*2\r\n$7\r\nCOMMAND\r\n$4\r\nINFO\r\n$3\r\nGET\r\n".getBytes(StandardCharsets.UTF_8))
          _      <- stream.write(data)
          res    <- stream.read.runHead
        } yield assert(res)(isSome(equalTo('*'.toByte)))
      }
    ).provideCustomLayer(Logging.ignore >>> RedisConnectionLive.default.orDie)
}
