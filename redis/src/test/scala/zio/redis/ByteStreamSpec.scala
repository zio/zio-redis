package zio.redis

import zio.logging.Logging
import zio.test.Assertion._
import zio.test._
import zio.{Chunk, ZIO}

import java.nio.charset.StandardCharsets

object ByteStreamSpec extends BaseSpec {
  override def spec: ZSpec[Environment, Failure] =
    suite("Byte stream")(
      testM("can write and read") {
        for {
          stream <- ZIO.service[ByteStream.Service]
          data    = Chunk.fromArray("*2\r\n$7\r\nCOMMAND\r\n$4\r\nINFO\r\n$3\r\nGET\r\n".getBytes(StandardCharsets.UTF_8))
          _      <- stream.write(data)
          res    <- stream.read.runHead
        } yield assert(res)(isSome(equalTo('*'.toByte)))
      }
    ).provideCustomLayer(Logging.ignore >>> ByteStream.default.orDie)
}
