package zio.redis.api

import zio.ZIO
import zio.redis.Input._
import zio.redis.Output._
import zio.redis._

trait HyperLogLog {
  final val pfAdd =
    new RedisCommand("PFADD", Tuple2(StringInput, NonEmptyList(StringInput)), BoolOutput) { self =>
      def apply(a: String, b: String, bs: String*): ZIO[RedisExecutor, RedisError, Boolean] =
        self.run((a, (b, bs.toList)))
    }

  final val pfCount = 
    new RedisCommand("PFCOUNT", NonEmptyList(StringInput), LongOutput) { self =>
      def apply(a: String, as: String*): ZIO[RedisExecutor, RedisError, Long] = self.run((a, as.toList))
    }

  final val pfMerge = 
    new RedisCommand("PFMERGE", Tuple2(StringInput, NonEmptyList(StringInput)), UnitOutput) { self =>
      def apply(a: String, b: String, bs: String*): ZIO[RedisExecutor, RedisError, Unit] = self.run((a, (b, bs.toList)))
    }
}
