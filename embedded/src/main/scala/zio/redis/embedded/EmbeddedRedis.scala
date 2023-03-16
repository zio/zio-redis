package zio.redis.embedded

import redis.embedded.RedisServer
import zio._
import zio.redis.RedisConfig

import java.io.IOException
import java.net.ServerSocket

object EmbeddedRedis {

  private def findFreePort: Task[Int] =
    (for {
      socket <- ZIO.attemptBlockingIO(new ServerSocket(0))
      port    = socket.getLocalPort
      _      <- ZIO.attemptBlockingIO(socket.close)
    } yield port).catchSome { case ex: IOException =>
      findFreePort
    }

  val layer: ZLayer[Any, Throwable, RedisConfig] = ZLayer.scoped(
    for {
      port <- findFreePort
      redisServer <- ZIO.acquireRelease(ZIO.attemptBlockingIO(new RedisServer(port)))(redisServer =>
                       ZIO.attemptBlockingIO(redisServer.stop).ignoreLogged
                     )
      _ <- ZIO.attemptBlockingIO(redisServer.start)
    } yield RedisConfig("localhost", port)
  )

}
