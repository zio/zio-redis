package zio.redis

import java.nio.charset.StandardCharsets

import zio.Chunk
import zio.logging.Logging
import zio.test.Assertion._
import zio.test._

object ByteStreamSpec extends BaseSpec {
  override def spec: ZSpec[Environment, Failure] =
    suite("Byte stream")(
      testM("can write and read") {
        ByteStream.connect.use { rw =>
          rw.write(
            Chunk.fromArray("*2\r\n$7\r\nCOMMAND\r\n$4\r\nINFO\r\n$3\r\nGET\r\n".getBytes(StandardCharsets.UTF_8))
          ) *>
            rw.read.runHead
              .map(result => assert(result)(isSome(equalTo('*'.toByte))))

        }
      }
    ).provideCustomLayerShared(Logging.ignore >>> ByteStream.socketLoopback(RedisExecutor.DefaultPort).orDie)
}
