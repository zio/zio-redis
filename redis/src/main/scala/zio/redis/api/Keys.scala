package zio.redis.api

import java.time.Duration
import java.time.Instant

import scala.util.matching.Regex

import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.{ Chunk, ZIO }

trait Keys {
  import Keys.{ Keys => _, _ }

  final def del(a: String, as: String*): ZIO[RedisExecutor, RedisError, Long] = Del.run((a, as.toList))

  final def dump(a: String): ZIO[RedisExecutor, RedisError, String] = Dump.run(a)

  final def exists(a: String, as: String*): ZIO[RedisExecutor, RedisError, Boolean] = Exists.run((a, as.toList))

  final def expire(a: String, b: Duration): ZIO[RedisExecutor, RedisError, Boolean] = Expire.run((a, b))

  final def expireAt(a: String, b: Instant): ZIO[RedisExecutor, RedisError, Boolean] = ExpireAt.run((a, b))

  final def keys(a: String): ZIO[RedisExecutor, RedisError, Chunk[String]] = Keys.Keys.run(a)

  final def migrate(
    a: String,
    b: Long,
    c: String,
    d: Long,
    e: Long,
    f: Option[Copy] = None,
    g: Option[Replace] = None,
    h: Option[Auth] = None,
    i: Option[(String, List[String])]
  ): ZIO[RedisExecutor, RedisError, String] = Migrate.run((a, b, c, d, e, f, g, h, i))

  final def move(a: String, b: Long): ZIO[RedisExecutor, RedisError, Boolean] = Move.run((a, b))

  final def persist(a: String): ZIO[RedisExecutor, RedisError, Boolean] = Persist.run(a)

  final def pExpire(a: String, b: Duration): ZIO[RedisExecutor, RedisError, Boolean] = PExpire.run((a, b))

  final def pExpireAt(a: String, b: Instant): ZIO[RedisExecutor, RedisError, Boolean] = PExpireAt.run((a, b))

  final def pTtl(a: String): ZIO[RedisExecutor, RedisError, Duration] = PTtl.run(a)

  final def randomKey(): ZIO[RedisExecutor, RedisError, Option[String]] = RandomKey.run(())

  final def rename(a: String, b: String): ZIO[RedisExecutor, RedisError, Unit] = Rename.run((a, b))

  final def renameNx(a: String, b: String): ZIO[RedisExecutor, RedisError, Boolean] = RenameNx.run((a, b))

  final def restore(
    a: String,
    b: Long,
    c: String,
    d: Option[Replace] = None,
    e: Option[AbsTtl] = None,
    f: Option[IdleTime] = None,
    g: Option[Freq] = None
  ): ZIO[RedisExecutor, RedisError, Unit] = Restore.run((a, b, c, d, e, f, g))

  final def scan(
    a: Long,
    b: Option[Regex] = None,
    c: Option[Long] = None,
    d: Option[String] = None
  ): ZIO[RedisExecutor, RedisError, (String, Chunk[String])] = Scan.run((a, b, c, d))

  final def touch(a: String, as: String*): ZIO[RedisExecutor, RedisError, Long] = Touch.run((a, as.toList))

  final def ttl(a: String): ZIO[RedisExecutor, RedisError, Duration] = Ttl.run(a)

  final def typeOf(a: String): ZIO[RedisExecutor, RedisError, RedisType] = TypeOf.run(a)

  final def unlink(a: String, as: String*): ZIO[RedisExecutor, RedisError, Long] = Unlink.run((a, as.toList))

  final def wait_(a: Long, b: Long): ZIO[RedisExecutor, RedisError, Long] = Wait.run((a, b))
}

private object Keys {
  final val Del      = RedisCommand("DEL", NonEmptyList(StringInput), LongOutput)
  final val Dump     = RedisCommand("DUMP", StringInput, MultiStringOutput)
  final val Exists   = RedisCommand("EXISTS", NonEmptyList(StringInput), BoolOutput)
  final val Expire   = RedisCommand("EXPIRE", Tuple2(StringInput, DurationSecondsInput), BoolOutput)
  final val ExpireAt = RedisCommand("EXPIREAT", Tuple2(StringInput, TimeSecondsInput), BoolOutput)
  final val Keys     = RedisCommand("KEYS", StringInput, ChunkOutput)

  final val Migrate =
    RedisCommand(
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
    )

  final val Move      = RedisCommand("MOVE", Tuple2(StringInput, LongInput), BoolOutput)
  final val Persist   = RedisCommand("PERSIST", StringInput, BoolOutput)
  final val PExpire   = RedisCommand("PEXPIRE", Tuple2(StringInput, DurationMillisecondsInput), BoolOutput)
  final val PExpireAt = RedisCommand("PEXPIREAT", Tuple2(StringInput, TimeMillisecondsInput), BoolOutput)
  final val PTtl      = RedisCommand("PTTL", StringInput, DurationMillisecondsOutput)
  final val RandomKey = RedisCommand("RANDOMKEY", NoInput, OptionalOutput(MultiStringOutput))
  final val Rename    = RedisCommand("RENAME", Tuple2(StringInput, StringInput), UnitOutput)
  final val RenameNx  = RedisCommand("RENAMENX", Tuple2(StringInput, StringInput), BoolOutput)

  final val Restore =
    RedisCommand(
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
    )

  final val Scan =
    RedisCommand(
      "SCAN",
      Tuple4(LongInput, OptionalInput(RegexInput), OptionalInput(LongInput), OptionalInput(StringInput)),
      ScanOutput
    )

  final val Touch  = RedisCommand("TOUCH", NonEmptyList(StringInput), LongOutput)
  final val Ttl    = RedisCommand("TTL", StringInput, DurationSecondsOutput)
  final val TypeOf = RedisCommand("TYPE", StringInput, TypeOutput)
  final val Unlink = RedisCommand("UNLINK", NonEmptyList(StringInput), LongOutput)
  final val Wait   = RedisCommand("WAIT", Tuple2(LongInput, LongInput), LongOutput)
}
