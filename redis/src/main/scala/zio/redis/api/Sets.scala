package zio.redis.api

import zio.{ Chunk, ZIO }
import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import scala.util.matching.Regex

trait Sets {
  final val sAdd =
    new RedisCommand("SADD", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput) { self =>
      def apply(a: String, b: String, bs: String*): ZIO[RedisExecutor, RedisError, Long] = self.run((a, (b, bs.toList)))
    }

  final val sCard =
    new RedisCommand("SCARD", StringInput, LongOutput) { self =>
      def apply(a: String): ZIO[RedisExecutor, RedisError, Long] = self.run(a)
    }

  final val sDiff =
    new RedisCommand("SDIFF", NonEmptyList(StringInput), ChunkOutput) { self =>
      def apply(a: String, as: String*): ZIO[RedisExecutor, RedisError, Chunk[String]] = self.run((a, as.toList))
    }

  final val sDiffStore =
    new RedisCommand("SDIFFSTORE", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput) { self =>
      def apply(a: String, b: String, bs: String*): ZIO[RedisExecutor, RedisError, Long] = self.run((a, (b, bs.toList)))
    }

  final val sInter =
    new RedisCommand("SINTER", NonEmptyList(StringInput), ChunkOutput) { self =>
      def apply(a: String, as: String*): ZIO[RedisExecutor, RedisError, Chunk[String]] = self.run((a, as.toList))
    }

  final val sInterStore =
    new RedisCommand("SINTERSTORE", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput) { self =>
      def apply(a: String, b: String, bs: String*): ZIO[RedisExecutor, RedisError, Long] = self.run((a, (b, bs.toList)))
    }

  final val sIsMember =
    new RedisCommand("SISMEMBER", Tuple2(StringInput, StringInput), BoolOutput) { self =>
      def apply(a: String, b: String): ZIO[RedisExecutor, RedisError, Boolean] = self.run((a, b))
    }

  final val sMembers =
    new RedisCommand("SMEMBERS", StringInput, ChunkOutput) { self =>
      def apply(a: String): ZIO[RedisExecutor, RedisError, Chunk[String]] = self.run(a)
    }

  final val sMove =
    new RedisCommand("SMOVE", Tuple3(StringInput, StringInput, StringInput), BoolOutput) { self =>
      def apply(a: String, b: String, c: String): ZIO[RedisExecutor, RedisError, Boolean] = self.run((a, b, c))
    }

  final val sPop =
    new RedisCommand("SPOP", Tuple2(StringInput, OptionalInput(LongInput)), MultiStringChunkOutput) { self =>
      def apply(a: String, b: Option[Long] = None): ZIO[RedisExecutor, RedisError, Chunk[String]] = self.run((a, b))
    }

  final val sRandMember =
    new RedisCommand("SRANDMEMBER", Tuple2(StringInput, OptionalInput(LongInput)), MultiStringChunkOutput) { self =>
      def apply(a: String, b: Option[Long] = None): ZIO[RedisExecutor, RedisError, Chunk[String]] = self.run((a, b))
    }

  final val sRem =
    new RedisCommand("SREM", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput) { self =>
      def apply(a: String, b: String, bs: String*): ZIO[RedisExecutor, RedisError, Long] = self.run((a, (b, bs.toList)))
    }

  final val sScan =
    new RedisCommand(
      "SSCAN",
      Tuple4(StringInput, LongInput, OptionalInput(RegexInput), OptionalInput(CountInput)),
      ScanOutput
    ) { self =>
      def apply(
        a: String,
        b: Long,
        c: Option[Regex] = None,
        d: Option[Count] = None
      ): ZIO[RedisExecutor, RedisError, (String, Chunk[String])] = self.run((a, b, c, d))
    }

  final val sUnion =
    new RedisCommand("SUNION", NonEmptyList(StringInput), ChunkOutput) { self =>
      def apply(a: String, as: String*): ZIO[RedisExecutor, RedisError, Chunk[String]] = self.run((a, as.toList))
    }

  final val sUnionStore =
    new RedisCommand("SUNIONSTORE", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput) { self =>
      def apply(a: String, b: String, bs: String*): ZIO[RedisExecutor, RedisError, Long] = self.run((a, (b, bs.toList)))
    }
}
