package zio.redis.api

import zio.{ Chunk, ZIO }
import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import java.time.Duration
import scala.util.matching.Regex

trait SortedSets {
  final val bzPopMax =
    new RedisCommand("BZPOPMAX", Tuple2(DurationSecondsInput, NonEmptyList(StringInput)), ChunkOutput) { self =>
      def apply(a: Duration, b: String, bs: String*): ZIO[RedisExecutor, RedisError, Chunk[String]] =
        self.run((a, (b, bs.toList)))
    }

  final val bzPopMin =
    new RedisCommand("BZPOPMIN", Tuple2(DurationSecondsInput, NonEmptyList(StringInput)), ChunkOutput) { self =>
      def apply(a: Duration, b: String, bs: String*): ZIO[RedisExecutor, RedisError, Chunk[String]] =
        self.run((a, (b, bs.toList)))
    }

  final def zAdd =
    new RedisCommand(
      "ZADD",
      Tuple4(
        StringInput,
        OptionalInput(UpdateInput),
        OptionalInput(ChangedInput),
        NonEmptyList(MemberScoreInput)
      ),
      LongOutput
    ) { self =>
      def apply(a: String, b: Option[Update] = None, c: Option[Changed] = None)(
        d: MemberScore,
        ds: MemberScore*
      ): ZIO[RedisExecutor, RedisError, Long] = self.run((a, b, c, (d, ds.toList)))
    }

  final def zAddWithIncr =
    new RedisCommand(
      "ZADD",
      Tuple5(
        StringInput,
        OptionalInput(UpdateInput),
        OptionalInput(ChangedInput),
        IncrementInput,
        NonEmptyList(MemberScoreInput)
      ),
      OptionalOutput(DoubleOutput)
    ) { self =>
      def apply(a: String, b: Option[Update] = None, c: Option[Changed] = None)(
        d: Increment,
        e: MemberScore,
        es: MemberScore*
      ): ZIO[RedisExecutor, RedisError, Option[Double]] = self.run((a, b, c, d, (e, es.toList)))
    }

  final val zCard =
    new RedisCommand("ZCARD", StringInput, LongOutput) { self =>
      def apply(a: String): ZIO[RedisExecutor, RedisError, Long] = self.run(a)
    }

  final val zCount =
    new RedisCommand("ZCOUNT", Tuple2(StringInput, RangeInput), LongOutput) { self =>
      def apply(a: String, b: Range): ZIO[RedisExecutor, RedisError, Long] = self.run((a, b))
    }

  final val zIncrBy =
    new RedisCommand("ZINCRBY", Tuple3(StringInput, LongInput, StringInput), DoubleOutput) { self =>
      def apply(a: String, b: Long, c: String): ZIO[RedisExecutor, RedisError, Double] = self.run((a, b, c))
    }

  final val zInterStore =
    new RedisCommand(
      "ZINTERSTORE",
      Tuple5(
        StringInput,
        LongInput,
        NonEmptyList(StringInput),
        OptionalInput(AggregateInput),
        OptionalInput(WeightsInput)
      ),
      LongOutput
    ) { self =>
      def apply(a: String, b: Long, c: String, cs: String*)(
        d: Option[Aggregate] = None,
        e: Option[::[Double]] = None
      ): ZIO[RedisExecutor, RedisError, Long] = self.run((a, b, (c, cs.toList), d, e))

    }

  final val zLexCount =
    new RedisCommand("ZLEXCOUNT", Tuple2(StringInput, LexRangeInput), LongOutput) { self =>
      def apply(a: String, b: LexRange): ZIO[RedisExecutor, RedisError, Long] = self.run((a, b))
    }

  final val zPopMax =
    new RedisCommand("ZPOPMAX", Tuple2(StringInput, OptionalInput(LongInput)), ChunkOutput) { self =>
      def apply(a: String, b: Option[Long] = None): ZIO[RedisExecutor, RedisError, Chunk[String]] = self.run((a, b))
    }

  final val zPopMin =
    new RedisCommand("ZPOPMIN", Tuple2(StringInput, OptionalInput(LongInput)), ChunkOutput) { self =>
      def apply(a: String, b: Option[Long] = None): ZIO[RedisExecutor, RedisError, Chunk[String]] = self.run((a, b))
    }

  final val zRange =
    new RedisCommand("ZRANGE", Tuple3(StringInput, RangeInput, OptionalInput(WithScoresInput)), ChunkOutput) { self =>
      def apply(a: String, b: Range, c: Option[WithScores] = None): ZIO[RedisExecutor, RedisError, Chunk[String]] =
        self.run((a, b, c))
    }

  final val zRangeByLex =
    new RedisCommand("ZRANGEBYLEX", Tuple3(StringInput, LexRangeInput, OptionalInput(LimitInput)), ChunkOutput) {
      self =>
      def apply(a: String, b: LexRange, c: Option[Limit] = None): ZIO[RedisExecutor, RedisError, Chunk[String]] =
        self.run((a, b, c))
    }

  final val zRangeByScore =
    new RedisCommand(
      "ZRANGEBYSCORE",
      Tuple4(StringInput, ScoreRangeInput, OptionalInput(WithScoresInput), OptionalInput(LimitInput)),
      ChunkOutput
    ) { self =>
      def apply(
        a: String,
        b: ScoreRange,
        c: Option[WithScores] = None,
        d: Option[Limit] = None
      ): ZIO[RedisExecutor, RedisError, Chunk[String]] = self.run((a, b, c, d))
    }

  final val zRank =
    new RedisCommand("ZRANK", Tuple2(StringInput, StringInput), OptionalOutput(LongOutput)) { self =>
      def apply(a: String, b: String): ZIO[RedisExecutor, RedisError, Option[Long]] = self.run((a, b))
    }

  final val zRem =
    new RedisCommand("ZREM", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput) { self =>
      def apply(a: String, b: String, bs: String*): ZIO[RedisExecutor, RedisError, Long] = self.run((a, (b, bs.toList)))
    }

  final val zRemRangeByLex =
    new RedisCommand("ZREMRANGEBYLEX", Tuple2(StringInput, LexRangeInput), LongOutput) { self =>
      def apply(a: String, b: LexRange): ZIO[RedisExecutor, RedisError, Long] = self.run((a, b))
    }

  final val zRemRangeByRank =
    new RedisCommand("ZREMRANGEBYRANK", Tuple2(StringInput, RangeInput), LongOutput) { self =>
      def apply(a: String, b: Range): ZIO[RedisExecutor, RedisError, Long] = self.run((a, b))
    }

  final val zRemRangeByScore =
    new RedisCommand("ZREMRANGEBYSCORE", Tuple2(StringInput, ScoreRangeInput), LongOutput) { self =>
      def apply(a: String, b: ScoreRange): ZIO[RedisExecutor, RedisError, Long] = self.run((a, b))
    }

  final val zRevRange =
    new RedisCommand("ZREVRANGE", Tuple3(StringInput, RangeInput, OptionalInput(WithScoresInput)), ChunkOutput) {
      self =>
      def apply(a: String, b: Range, c: Option[WithScores] = None): ZIO[RedisExecutor, RedisError, Chunk[String]] =
        self.run((a, b, c))
    }

  final val zRevRangeByLex =
    new RedisCommand("ZREVRANGEBYLEX", Tuple3(StringInput, LexRangeInput, OptionalInput(LimitInput)), ChunkOutput) {
      self =>
      def apply(a: String, b: LexRange, c: Option[Limit] = None): ZIO[RedisExecutor, RedisError, Chunk[String]] =
        self.run((a, b, c))
    }

  final val zRevRangeByScore =
    new RedisCommand(
      "ZREVRANGEBYSCORE",
      Tuple4(StringInput, ScoreRangeInput, OptionalInput(WithScoresInput), OptionalInput(LimitInput)),
      ChunkOutput
    ) { self =>
      def apply(
        a: String,
        b: ScoreRange,
        c: Option[WithScores] = None,
        d: Option[Limit] = None
      ): ZIO[RedisExecutor, RedisError, Chunk[String]] =
        self.run((a, b, c, d))
    }

  final val zRevRank =
    new RedisCommand("ZREVRANK", Tuple2(StringInput, StringInput), OptionalOutput(LongOutput)) { self =>
      def apply(a: String, b: String): ZIO[RedisExecutor, RedisError, Option[Long]] = self.run((a, b))
    }

  final val zScan =
    new RedisCommand(
      "ZSCAN",
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

  final val zScore =
    new RedisCommand("ZSCORE", Tuple2(StringInput, StringInput), OptionalOutput(DoubleOutput)) { self =>
      def apply(a: String, b: String): ZIO[RedisExecutor, RedisError, Option[Double]] = self.run((a, b))
    }

  final val zUnionStore =
    new RedisCommand(
      "ZUNIONSTORE",
      Tuple5(
        StringInput,
        LongInput,
        NonEmptyList(StringInput),
        OptionalInput(WeightsInput),
        OptionalInput(AggregateInput)
      ),
      LongOutput
    ) { self =>
      def apply(a: String, b: Long, c: String, cs: String*)(
        d: Option[::[Double]] = None,
        e: Option[Aggregate] = None
      ): ZIO[RedisExecutor, RedisError, Long] = self.run((a, b, (c, cs.toList), d, e))
    }
}
