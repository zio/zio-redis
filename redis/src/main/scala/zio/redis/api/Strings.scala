package zio.redis.api

import zio.{ Chunk, ZIO }
import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import java.time.Duration

trait Strings {
  final val append =
    new RedisCommand("APPEND", Tuple2(StringInput, StringInput), LongOutput) { self =>
      def apply(a: String, b: String): ZIO[RedisExecutor, RedisError, Long] = self.run((a, b))
    }

  final val bitCount =
    new RedisCommand("BITCOUNT", Tuple2(StringInput, OptionalInput(RangeInput)), LongOutput) { self =>
      def apply(a: String, b: Option[Range] = None): ZIO[RedisExecutor, RedisError, Long] = self.run((a, b))
    }

  final val bitField =
    new RedisCommand(
      "BITFIELD",
      Tuple2(StringInput, NonEmptyList(BitFieldCommandInput)),
      ChunkOptionalLongOutput
    ) { self =>
      def apply(
        a: String,
        b: BitFieldCommand,
        bs: BitFieldCommand*
      ): ZIO[RedisExecutor, RedisError, Chunk[Option[Long]]] = self.run((a, (b, bs.toList)))
    }

  final val bitOp =
    new RedisCommand("BITOP", Tuple3(BitOperationInput, StringInput, NonEmptyList(StringInput)), LongOutput) { self =>
      def apply(a: BitOperation, b: String, c: String, cs: String*): ZIO[RedisExecutor, RedisError, Long] =
        self.run((a, b, (c, cs.toList)))
    }

  final val bitPos =
    new RedisCommand("BITPOS", Tuple3(StringInput, BoolInput, OptionalInput(BitPosRangeInput)), LongOutput) { self =>
      def apply(a: String, b: Boolean, c: Option[BitPosRange] = None): ZIO[RedisExecutor, RedisError, Long] =
        self.run((a, b, c))
    }

  final val decr =
    new RedisCommand("DECR", StringInput, LongOutput) { self =>
      def apply(a: String): ZIO[RedisExecutor, RedisError, Long] = self.run(a)
    }

  final val decrBy =
    new RedisCommand("DECRBY", Tuple2(StringInput, LongInput), LongOutput) { self =>
      def apply(a: String, b: Long): ZIO[RedisExecutor, RedisError, Long] = self.run((a, b))
    }

  final val get =
    new RedisCommand("GET", StringInput, OptionalOutput(MultiStringOutput)) { self =>
      def apply(a: String): ZIO[RedisExecutor, RedisError, Option[String]] = self.run(a)
    }

  final val getBit =
    new RedisCommand("GETBIT", Tuple2(StringInput, LongInput), LongOutput) { self =>
      def apply(a: String, b: Long): ZIO[RedisExecutor, RedisError, Long] = self.run((a, b))
    }

  final val getRange =
    new RedisCommand("GETRANGE", Tuple2(StringInput, RangeInput), MultiStringOutput) { self =>
      def apply(a: String, b: Range): ZIO[RedisExecutor, RedisError, String] = self.run((a, b))
    }

  final val getSet =
    new RedisCommand("GETSET", Tuple2(StringInput, StringInput), OptionalOutput(MultiStringOutput)) { self =>
      def apply(a: String, b: String): ZIO[RedisExecutor, RedisError, Option[String]] = self.run((a, b))
    }

  final val incr =
    new RedisCommand("INCR", StringInput, LongOutput) { self =>
      def apply(a: String): ZIO[RedisExecutor, RedisError, Long] = self.run(a)
    }

  final val incrBy =
    new RedisCommand("INCRBY", Tuple2(StringInput, LongInput), LongOutput) { self =>
      def apply(a: String, b: Long): ZIO[RedisExecutor, RedisError, Long] = self.run((a, b))
    }

  final val incrByFloat =
    new RedisCommand("INCRBYFLOAT", Tuple2(StringInput, DoubleInput), MultiStringOutput) { self =>
      def apply(a: String, b: Double): ZIO[RedisExecutor, RedisError, String] = self.run((a, b))
    }

  final val mGet =
    new RedisCommand("MGET", NonEmptyList(StringInput), ChunkOptionalMultiStringOutput) { self =>
      def apply(a: String, as: String*): ZIO[RedisExecutor, RedisError, Chunk[Option[String]]] =
        self.run((a, as.toList))
    }

  final val mSet =
    new RedisCommand("MSET", NonEmptyList(Tuple2(StringInput, StringInput)), UnitOutput) { self =>
      def apply(a: (String, String), as: (String, String)*): ZIO[RedisExecutor, RedisError, Unit] =
        self.run((a, as.toList))
    }

  final val mSetNx =
    new RedisCommand("MSETNX", NonEmptyList(Tuple2(StringInput, StringInput)), BoolOutput) { self =>
      def apply(a: (String, String), as: (String, String)*): ZIO[RedisExecutor, RedisError, Boolean] =
        self.run((a, as.toList))
    }

  final val pSetEx =
    new RedisCommand("PSETEX", Tuple3(StringInput, DurationMillisecondsInput, StringInput), UnitOutput) { self =>
      def apply(a: String, b: Duration, c: String): ZIO[RedisExecutor, RedisError, Unit] = self.run((a, b, c))
    }

  final val set =
    new RedisCommand(
      "SET",
      Tuple5(
        StringInput,
        StringInput,
        OptionalInput(DurationTtlInput),
        OptionalInput(UpdateInput),
        OptionalInput(KeepTtlInput)
      ),
      OptionalOutput(UnitOutput)
    ) { self =>
      def apply(
        a: String,
        b: String,
        c: Option[Duration] = None,
        d: Option[Update] = None,
        e: Option[KeepTtl] = None
      ): ZIO[RedisExecutor, RedisError, Option[Unit]] = self.run((a, b, c, d, e))
    }

  final val setBit =
    new RedisCommand("SETBIT", Tuple3(StringInput, LongInput, BoolInput), BoolOutput) { self =>
      def apply(a: String, b: Long, c: Boolean): ZIO[RedisExecutor, RedisError, Boolean] = self.run((a, b, c))
    }

  final val setEx =
    new RedisCommand("SETEX", Tuple3(StringInput, DurationSecondsInput, StringInput), UnitOutput) { self =>
      def apply(a: String, b: Duration, c: String): ZIO[RedisExecutor, RedisError, Unit] = self.run((a, b, c))
    }

  final val setNx =
    new RedisCommand("SETNX", Tuple2(StringInput, StringInput), BoolOutput) { self =>
      def apply(a: String, b: String): ZIO[RedisExecutor, RedisError, Boolean] = self.run((a, b))
    }

  final val setRange =
    new RedisCommand("SETRANGE", Tuple3(StringInput, LongInput, StringInput), LongOutput) { self =>
      def apply(a: String, b: Long, c: String): ZIO[RedisExecutor, RedisError, Long] = self.run((a, b, c))
    }

  final val strLen =
    new RedisCommand("STRLEN", StringInput, LongOutput) { self =>
      def apply(a: String): ZIO[RedisExecutor, RedisError, Long] = self.run(a)
    }
}
