package zio.redis

import java.io.IOException

import zio.{ Chunk, Has, IO, Layer, ULayer }

trait Interpreter {
  type RedisExecutor = Has[RedisExecutor.Service]

  object RedisExecutor {
    trait Service {
      def execute(command: Chunk[String]): IO[RedisError, String]
    }

    def live(port: Int, host: String): Layer[IOException, RedisExecutor] = ???

    def test: ULayer[RedisExecutor] = ???
  }
}
