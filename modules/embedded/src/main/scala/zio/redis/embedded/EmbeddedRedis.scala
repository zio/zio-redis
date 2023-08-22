/*
 * Copyright 2021 John A. De Goes and the ZIO contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
    } yield port).catchSome { case _: IOException =>
      findFreePort
    }

  lazy val layer: ZLayer[Any, Throwable, RedisConfig] =
    ZLayer.scoped {
      for {
        port        <- findFreePort
        redisServer <- ZIO.acquireRelease(ZIO.attemptBlockingIO(new RedisServer(port)))(redisServer =>
                         ZIO.attemptBlockingIO(redisServer.stop).ignoreLogged
                       )
        _           <- ZIO.attemptBlockingIO(redisServer.start)
      } yield RedisConfig("localhost", port)
    }

}
