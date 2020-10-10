package zio.redis.api

import zio.{ Chunk, ZIO }
import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import scala.util.matching.Regex

trait Hashes {
  final val hDel =
    new RedisCommand("HDEL", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput) { self =>
      def apply(a: String, b: String, bs: String*): ZIO[RedisExecutor, RedisError, Long] = self.run((a, (b, bs.toList)))
    }

  final val hExists =
    new RedisCommand("HEXISTS", Tuple2(StringInput, StringInput), BoolOutput) { self =>
      def apply(a: String, b: String): ZIO[RedisExecutor, RedisError, Boolean] = self.run((a, b))
    }

  final val hGet =
    new RedisCommand("HGET", Tuple2(StringInput, StringInput), OptionalOutput(MultiStringOutput)) { self =>
      def apply(a: String, b: String): ZIO[RedisExecutor, RedisError, Option[String]] = self.run((a, b))
    }

  final val hGetAll =
    new RedisCommand("HGETALL", StringInput, KeyValueOutput) { self =>
      def apply(a: String): ZIO[RedisExecutor, RedisError, Map[String, String]] = self.run(a)
    }

  final val hIncrBy =
    new RedisCommand("HINCRBY", Tuple3(StringInput, StringInput, LongInput), LongOutput) { self =>
      def apply(a: String, b: String, c: Long): ZIO[RedisExecutor, RedisError, Long] = self.run((a, b, c))
    }

  final val hIncrByFloat =
    new RedisCommand("HINCRBYFLOAT", Tuple3(StringInput, StringInput, DoubleInput), IncrementOutput) { self =>
      def apply(a: String, b: String, c: Double): ZIO[RedisExecutor, RedisError, Double] = self.run((a, b, c))
    }

  final val hKeys =
    new RedisCommand("HKEYS", StringInput, ChunkOutput) { self =>
      def apply(a: String): ZIO[RedisExecutor, RedisError, Chunk[String]] = self.run(a)
    }

  final val hLen =
    new RedisCommand("HLEN", StringInput, LongOutput) { self =>
      def apply(a: String): ZIO[RedisExecutor, RedisError, Long] = self.run(a)
    }

  final val hmGet =
    new RedisCommand("HMGET", Tuple2(StringInput, NonEmptyList(StringInput)), ChunkOutput) { self =>
      def apply(a: String, b: String, bs: String*): ZIO[RedisExecutor, RedisError, Chunk[String]] =
        self.run((a, (b, bs.toList)))
    }

  final val hScan =
    new RedisCommand(
      "HSCAN",
      Tuple4(LongInput, OptionalInput(RegexInput), OptionalInput(LongInput), OptionalInput(StringInput)),
      ScanOutput
    ) { self =>
      def apply(
        a: Long,
        b: Option[Regex] = None,
        c: Option[Long] = None,
        d: Option[String] = None
      ): ZIO[RedisExecutor, RedisError, (String, Chunk[String])] = self.run((a, b, c, d))
    }

  final val hSet =
    new RedisCommand(
      "HSET",
      Tuple2(StringInput, NonEmptyList(Tuple2(StringInput, StringInput))),
      LongOutput
    ) { self =>
      def apply(a: String, b: (String, String), bs: (String, String)*): ZIO[RedisExecutor, RedisError, Long] =
        self.run((a, (b, bs.toList)))
    }

  final val hSetNx =
    new RedisCommand("HSETNX", Tuple3(StringInput, StringInput, StringInput), BoolOutput) { self =>
      def apply(a: String, b: String, c: String): ZIO[RedisExecutor, RedisError, Boolean] = self.run((a, b, c))
    }

  final val hStrLen =
    new RedisCommand("HSTRLEN", Tuple2(StringInput, StringInput), LongOutput) { self =>
      def apply(a: String, b: String): ZIO[RedisExecutor, RedisError, Long] = self.run((a, b))
    }

  final val hVals =
    new RedisCommand("HVALS", StringInput, ChunkOutput) { self =>
      def apply(a: String): ZIO[RedisExecutor, RedisError, Chunk[String]] = self.run(a)
    }
}
