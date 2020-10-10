package zio.redis.api

import zio.{ Chunk, ZIO }
import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import java.time.Duration
import java.time.Instant
import scala.util.matching.Regex

trait Keys {
  final val del =
    new RedisCommand("DEL", NonEmptyList(StringInput), LongOutput) { self =>
      def apply(a: String, as: String*): ZIO[RedisExecutor, RedisError, Long] = self.run((a, as.toList))
    }

  final val dump =
    new RedisCommand("DUMP", StringInput, MultiStringOutput) { self =>
      def apply(a: String): ZIO[RedisExecutor, RedisError, String] = self.run(a)
    }

  final val exists =
    new RedisCommand("EXISTS", NonEmptyList(StringInput), BoolOutput) { self =>
      def apply(a: String, as: String*): ZIO[RedisExecutor, RedisError, Boolean] = self.run((a, as.toList))
    }

  final val expire =
    new RedisCommand("EXPIRE", Tuple2(StringInput, DurationSecondsInput), BoolOutput) { self =>
      def apply(a: String, b: Duration): ZIO[RedisExecutor, RedisError, Boolean] = self.run((a, b))
    }

  final val expireAt =
    new RedisCommand("EXPIREAT", Tuple2(StringInput, TimeSecondsInput), BoolOutput) { self =>
      def apply(a: String, b: Instant): ZIO[RedisExecutor, RedisError, Boolean] = self.run((a, b))
    }

  final val keys =
    new RedisCommand("KEYS", StringInput, ChunkOutput) { self =>
      def apply(a: String): ZIO[RedisExecutor, RedisError, Chunk[String]] = self.run(a)
    }

  final val migrate =
    new RedisCommand(
      "MIGRATE",
      Tuple9(
        StringInput,
        LongInput,
        StringInput,
        LongInput,
        LongInput,
        OptionalInput(CopyInput),
        OptionalInput(ReplaceInput),
        OptionalInput(AuthInput),
        OptionalInput(NonEmptyList(StringInput))
      ),
      MultiStringOutput
    ) { self =>
      def apply(
        a: String,
        b: Long,
        c: String,
        d: Long,
        e: Long,
        f: Option[Copy] = None,
        g: Option[Replace] = None,
        h: Option[Auth] = None,
        i: Option[(String, List[String])]
      ): ZIO[RedisExecutor, RedisError, String] = self.run((a, b, c, d, e, f, g, h, i))
    }

  final val move =
    new RedisCommand("MOVE", Tuple2(StringInput, LongInput), BoolOutput) { self =>
      def apply(a: String, b: Long): ZIO[RedisExecutor, RedisError, Boolean] = self.run((a, b))
    }

  final val persist =
    new RedisCommand("PERSIST", StringInput, BoolOutput) { self =>
      def apply(a: String): ZIO[RedisExecutor, RedisError, Boolean] = self.run(a)
    }

  final val pExpire =
    new RedisCommand("PEXPIRE", Tuple2(StringInput, DurationMillisecondsInput), BoolOutput) { self =>
      def apply(a: String, b: Duration): ZIO[RedisExecutor, RedisError, Boolean] = self.run((a, b))
    }

  final val pExpireAt =
    new RedisCommand("PEXPIREAT", Tuple2(StringInput, TimeMillisecondsInput), BoolOutput) { self =>
      def apply(a: String, b: Instant): ZIO[RedisExecutor, RedisError, Boolean] = self.run((a, b))
    }

  final val pTtl =
    new RedisCommand("PTTL", StringInput, DurationMillisecondsOutput) { self =>
      def apply(a: String): ZIO[RedisExecutor, RedisError, Duration] = self.run(a)
    }

  final val randomKey =
    new RedisCommand("RANDOMKEY", NoInput, OptionalOutput(MultiStringOutput)) { self =>
      def apply(): ZIO[RedisExecutor, RedisError, Option[String]] = self.run(())
    }

  final val rename =
    new RedisCommand("RENAME", Tuple2(StringInput, StringInput), UnitOutput) { self =>
      def apply(a: String, b: String): ZIO[RedisExecutor, RedisError, Unit] = self.run((a, b))
    }

  final val renameNx =
    new RedisCommand("RENAMENX", Tuple2(StringInput, StringInput), BoolOutput) { self =>
      def apply(a: String, b: String): ZIO[RedisExecutor, RedisError, Boolean] = self.run((a, b))
    }

  final val restore =
    new RedisCommand(
      "RESTORE",
      Tuple7(
        StringInput,
        LongInput,
        StringInput,
        OptionalInput(ReplaceInput),
        OptionalInput(AbsTtlInput),
        OptionalInput(IdleTimeInput),
        OptionalInput(FreqInput)
      ),
      UnitOutput
    ) { self =>
      def apply(
        a: String,
        b: Long,
        c: String,
        d: Option[Replace] = None,
        e: Option[AbsTtl] = None,
        f: Option[IdleTime] = None,
        g: Option[Freq] = None
      ): ZIO[RedisExecutor, RedisError, Unit] = self.run((a, b, c, d, e, f, g))
    }

  final val scan =
    new RedisCommand(
      "SCAN",
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

  final val touch =
    new RedisCommand("TOUCH", NonEmptyList(StringInput), LongOutput) { self =>
      def apply(a: String, as: String*): ZIO[RedisExecutor, RedisError, Long] = self.run((a, as.toList))
    }

  final val ttl    =
    new RedisCommand("TTL", StringInput, DurationSecondsOutput) { self =>
      def apply(a: String): ZIO[RedisExecutor, RedisError, Duration] = self.run(a)
    }

  final val typeOf = 
    new RedisCommand("TYPE", StringInput, TypeOutput) {self =>
      def apply(a: String): ZIO[RedisExecutor, RedisError, RedisType] = self.run(a)
    }

  final val unlink = 
    new RedisCommand("UNLINK", NonEmptyList(StringInput), LongOutput) { self =>
      def apply(a: String, as: String*): ZIO[RedisExecutor, RedisError, Long] = self.run((a, as.toList))
    }

  final val wait_  = 
    new RedisCommand("WAIT", Tuple2(LongInput, LongInput), LongOutput) { self =>
      def apply(a: Long, b: Long): ZIO[RedisExecutor, RedisError, Long] = self.run((a, b))
    }
}
