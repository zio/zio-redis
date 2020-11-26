package zio.redis.api

import zio.duration.Duration
import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.{ Chunk, ZIO }

trait Connection {
  import Connection._

  final def auth(a: String): ZIO[RedisExecutor, RedisError, Unit] = Auth.run(a)

  final def clientCaching(a: Boolean): ZIO[RedisExecutor, RedisError, Unit] = ClientCaching.run(a)

  final def clientId: ZIO[RedisExecutor, RedisError, Long] = ClientId.run(())

  final def clientKill(filters: ClientKillFilter*): ZIO[RedisExecutor, RedisError, Long] =
    ClientKill.run(Chunk.fromIterable(filters))

  final def clientList: ZIO[RedisExecutor, RedisError, Chunk[ClientInfo]] = ClientList.run(None)

  final def clientList(filter: ClientType): ZIO[RedisExecutor, RedisError, Chunk[ClientInfo]] =
    ClientList.run(Some(filter))

  final def clientGetName: ZIO[RedisExecutor, RedisError, Option[String]] = ClientGetName.run(())

  final def clientGetRedir: ZIO[RedisExecutor, RedisError, ClientTrackingRedirect] = ClientRedirect.run(())

  final def clientPause(duration: Duration): ZIO[RedisExecutor, RedisError, Unit] = ClientPause.run(duration)

  final def clientSetName(name: String): ZIO[RedisExecutor, RedisError, Unit] = ClientSetName.run(name)

  final def clientTrackingOn(
    redirect: Option[Long] = None,
    trackingMode: Option[ClientTrackingMode] = None,
    noLoop: Boolean = false,
    prefixes: Set[String]
  ): ZIO[RedisExecutor, RedisError, Unit] =
    ClientTracking.run(Some((redirect, trackingMode, noLoop, Chunk.fromIterable(prefixes))))

  final def clientTrackingOff: ZIO[RedisExecutor, RedisError, Unit] = ClientTracking.run(None)

  final def clientUnblock(clientId: Long, error: Boolean = false): ZIO[RedisExecutor, RedisError, Boolean] =
    ClientUnblock.run((clientId, error))

  final def echo(a: String): ZIO[RedisExecutor, RedisError, String] = Echo.run(a)

  final def ping(as: String*): ZIO[RedisExecutor, RedisError, String] = Ping.run(as)

  final def quit: ZIO[RedisExecutor, RedisError, Unit] = Quit.run(())

  final def reset: ZIO[RedisExecutor, RedisError, Unit] = Reset.run(())

  final def select(a: Long): ZIO[RedisExecutor, RedisError, Unit] = Select.run(a)
}

private object Connection {
  final val Auth           = RedisCommand("AUTH", StringInput, UnitOutput)
  final val ClientCaching  = RedisCommand(Chunk("CLIENT", "CACHING"), YesNoInput, UnitOutput)
  final val ClientId       = RedisCommand(Chunk("CLIENT", "ID"), NoInput, LongOutput)
  final val ClientKill     = RedisCommand(Chunk("CLIENT", "KILL"), ClientKillInput, LongOutput)
  final val ClientList     = RedisCommand(Chunk("CLIENT", "LIST"), ClientTypeInput, ClientInfoOutput)
  final val ClientGetName  = RedisCommand(Chunk("CLIENT", "GETNAME"), NoInput, OptionalOutput(MultiStringOutput))
  final val ClientRedirect = RedisCommand(Chunk("CLIENT", "GETREDIR"), NoInput, ClientTrackingRedirectOutput)
  final val ClientPause    = RedisCommand(Chunk("CLIENT", "PAUSE"), DurationMillisecondsInput, UnitOutput)
  final val ClientSetName  = RedisCommand(Chunk("CLIENT", "SETNAME"), StringInput, UnitOutput)
  final val ClientTracking = RedisCommand(Chunk("CLIENT", "TRACKING"), ClientTrackingInput, UnitOutput)
  final val ClientUnblock  = RedisCommand(Chunk("CLIENT", "UNBLOCK"), Tuple2(LongInput, UnblockInput), BoolOutput)
  final val Echo           = RedisCommand("ECHO", StringInput, MultiStringOutput)
  final val Ping           = RedisCommand("PING", Varargs(StringInput), MultiStringOutput)
  final val Quit           = RedisCommand("QUIT", NoInput, UnitOutput)
  final val Reset          = RedisCommand("RESET", NoInput, ResetOutput)
  final val Select         = RedisCommand("SELECT", LongInput, UnitOutput)
}
