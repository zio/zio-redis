package zio.redis

import zio._
import zio.test.Assertion._
import zio.test._

import java.nio.charset.StandardCharsets

object ByteStreamSpec extends BaseSpec {
  override def spec: Spec[TestEnvironment, Throwable] =
    suite("Byte stream")(
      test("can write and read") {
        for {
          stream <- ZIO.service[ByteStream]
          data    = Chunk.fromArray("*2\r\n$7\r\nCOMMAND\r\n$4\r\nINFO\r\n$3\r\nGET\r\n".getBytes(StandardCharsets.UTF_8))
          _      <- stream.write(data)
          res    <- stream.read.runHead
        } yield assert(res)(isSome(equalTo('*'.toByte)))
      }
    ).provideCustomLayer(ByteStream.default)
}
