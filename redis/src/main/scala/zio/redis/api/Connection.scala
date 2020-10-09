package zio.redis.api

import zio.ZIO
import zio.redis.Input._
import zio.redis.Output._
import zio.redis._

trait Connection {
  final val auth =
    new RedisCommand("AUTH", StringInput, UnitOutput) { self =>
      def apply(a: String): ZIO[RedisExecutor, RedisError, Unit] = self.run(a)
    }

  final val echo =
    new RedisCommand("ECHO", StringInput, MultiStringOutput) { self =>
      def apply(a: String): ZIO[RedisExecutor, RedisError, String] = self.run(a)
    }

  final val ping =
    new RedisCommand("PING", Varargs(StringInput), MultiStringOutput) { self =>
      def apply(as: String*): ZIO[RedisExecutor, RedisError, String] = self.run(as)
    }

  final val select =
    new RedisCommand("SELECT", LongInput, UnitOutput) { self =>
      def apply(a: Long): ZIO[RedisExecutor, RedisError, Unit] = self.run(a)
    }
}
