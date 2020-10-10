package zio.redis.api

import zio.{ Chunk, ZIO }
import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import java.time.Duration

trait Lists {
  final val brPopLPush =
    new RedisCommand(
      "BRPOPLPUSH",
      Tuple3(StringInput, StringInput, DurationSecondsInput),
      OptionalOutput(MultiStringOutput)
    ) { self =>
      def apply(a: String, b: String, c: Duration): ZIO[RedisExecutor, RedisError, Option[String]] = self.run((a, b, c))
    }

  final val lIndex =
    new RedisCommand("LINDEX", Tuple2(StringInput, LongInput), OptionalOutput(MultiStringOutput)) { self =>
      def apply(a: String, b: Long): ZIO[RedisExecutor, RedisError, Option[String]] = self.run((a, b))
    }

  final val lLen =
    new RedisCommand("LLEN", StringInput, LongOutput) { self =>
      def apply(a: String): ZIO[RedisExecutor, RedisError, Long] = self.run(a)
    }

  final val lPop =
    new RedisCommand("LPOP", StringInput, OptionalOutput(MultiStringOutput)) { self =>
      def apply(a: String): ZIO[RedisExecutor, RedisError, Option[String]] = self.run(a)
    }

  final val lPush =
    new RedisCommand("LPUSH", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput) { self =>
      def apply(a: String, b: String, bs: String*): ZIO[RedisExecutor, RedisError, Long] = self.run((a, (b, bs.toList)))
    }

  final val lPushX =
    new RedisCommand("LPUSHX", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput) { self =>
      def apply(a: String, b: String, bs: String*): ZIO[RedisExecutor, RedisError, Long] = self.run((a, (b, bs.toList)))
    }

  final val lRange =
    new RedisCommand("LRANGE", Tuple2(StringInput, RangeInput), ChunkOutput) { self =>
      def apply(a: String, b: Range): ZIO[RedisExecutor, RedisError, Chunk[String]] = self.run((a, b))
    }

  final val lRem =
    new RedisCommand("LREM", Tuple3(StringInput, LongInput, StringInput), LongOutput) { self =>
      def apply(a: String, b: Long, c: String): ZIO[RedisExecutor, RedisError, Long] = self.run((a, b, c))
    }

  final val lSet =
    new RedisCommand("LSET", Tuple3(StringInput, LongInput, StringInput), UnitOutput) { self =>
      def apply(a: String, b: Long, c: String): ZIO[RedisExecutor, RedisError, Unit] = self.run((a, b, c))
    }

  final val lTrim =
    new RedisCommand("LTRIM", Tuple2(StringInput, RangeInput), UnitOutput) { self =>
      def apply(a: String, b: Range): ZIO[RedisExecutor, RedisError, Unit] = self.run((a, b))
    }

  final val rPop =
    new RedisCommand("RPOP", StringInput, OptionalOutput(MultiStringOutput)) { self =>
      def apply(a: String): ZIO[RedisExecutor, RedisError, Option[String]] = self.run(a)
    }

  final val rPopLPush =
    new RedisCommand("RPOPLPUSH", Tuple2(StringInput, StringInput), OptionalOutput(MultiStringOutput)) { self =>
      def apply(a: String, b: String): ZIO[RedisExecutor, RedisError, Option[String]] = self.run((a, b))
    }

  final val rPush =
    new RedisCommand("RPUSH", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput) { self =>
      def apply(a: String, b: String, bs: String*): ZIO[RedisExecutor, RedisError, Long] = self.run((a, (b, bs.toList)))
    }

  final val rPushX =
    new RedisCommand("RPUSHX", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput) { self =>
      def apply(a: String, b: String, bs: String*): ZIO[RedisExecutor, RedisError, Long] = self.run((a, (b, bs.toList)))
    }

  final val blPop =
    new RedisCommand("BLPOP", Tuple2(NonEmptyList(StringInput), DurationSecondsInput), KeyElemOutput) { self =>
      def apply(a: String, as: String*)(b: Duration): ZIO[RedisExecutor, RedisError, Option[(String, String)]] =
        self.run(((a, as.toList), b))
    }

  final val brPop =
    new RedisCommand("BRPOP", Tuple2(NonEmptyList(StringInput), DurationSecondsInput), KeyElemOutput) { self =>
      def apply(a: String, as: String*)(b: Duration): ZIO[RedisExecutor, RedisError, Option[(String, String)]] =
        self.run(((a, as.toList), b))
    }

  final val lInsert =
    new RedisCommand("LINSERT", Tuple4(StringInput, PositionInput, StringInput, StringInput), LongOutput) { self =>
      def apply(a: String, b: Position, c: String, d: String): ZIO[RedisExecutor, RedisError, Long] =
        self.run((a, b, c, d))
    }
}
