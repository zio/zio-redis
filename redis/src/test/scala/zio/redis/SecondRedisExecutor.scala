package zio.redis

import zio.Has
//import zio.ZLayer
import zio.Chunk
//import zio.logging.Logging

object SecondRedisExecutorLayer {
      // Need a RedisExecutor of a different type so we can have two executors at the same time
    // in the same environment, for testing things like migrate...
    type SecondRedisExecutor = Has[SecondRedisExecutor.Service]

    object SecondRedisExecutor {
      trait Service extends RedisExecutor.Service

      private final val DefaultPort = 6380

      lazy val live =
        RedisExecutor.live.project {
          srv =>
            (command: Chunk[RespValue.BulkString]) => srv.execute(command)
        }
    }
}
