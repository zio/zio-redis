package zio.redis.api

import zio.{ Chunk, ZIO }
import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import java.time.Duration
import java.time.Instant
import scala.util.matching.Regex

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

private[api] object Keys {
  final val Del      = new RedisCommand("DEL", NonEmptyList(StringInput), LongOutput)
  final val Dump     = new RedisCommand("DUMP", StringInput, MultiStringOutput)
  final val Exists   = new RedisCommand("EXISTS", NonEmptyList(StringInput), BoolOutput)
  final val Expire   = new RedisCommand("EXPIRE", Tuple2(StringInput, DurationSecondsInput), BoolOutput)
  final val ExpireAt = new RedisCommand("EXPIREAT", Tuple2(StringInput, TimeSecondsInput), BoolOutput)
  final val Keys     = new RedisCommand("KEYS", StringInput, ChunkOutput)

  final val Migrate =
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
    )

  final val Move      = new RedisCommand("MOVE", Tuple2(StringInput, LongInput), BoolOutput)
  final val Persist   = new RedisCommand("PERSIST", StringInput, BoolOutput)
  final val PExpire   = new RedisCommand("PEXPIRE", Tuple2(StringInput, DurationMillisecondsInput), BoolOutput)
  final val PExpireAt = new RedisCommand("PEXPIREAT", Tuple2(StringInput, TimeMillisecondsInput), BoolOutput)
  final val PTtl      = new RedisCommand("PTTL", StringInput, DurationMillisecondsOutput)
  final val RandomKey = new RedisCommand("RANDOMKEY", NoInput, OptionalOutput(MultiStringOutput))
  final val Rename    = new RedisCommand("RENAME", Tuple2(StringInput, StringInput), UnitOutput)
  final val RenameNx  = new RedisCommand("RENAMENX", Tuple2(StringInput, StringInput), BoolOutput)

  final val Restore =
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
    )

  final val Scan =
    new RedisCommand(
      "SCAN",
      Tuple4(LongInput, OptionalInput(RegexInput), OptionalInput(LongInput), OptionalInput(StringInput)),
      ScanOutput
    )

  final val Touch  = new RedisCommand("TOUCH", NonEmptyList(StringInput), LongOutput)
  final val Ttl    = new RedisCommand("TTL", StringInput, DurationSecondsOutput)
  final val TypeOf = new RedisCommand("TYPE", StringInput, TypeOutput)
  final val Unlink = new RedisCommand("UNLINK", NonEmptyList(StringInput), LongOutput)
  final val Wait   = new RedisCommand("WAIT", Tuple2(LongInput, LongInput), LongOutput)
}
