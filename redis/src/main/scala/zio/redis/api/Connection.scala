package zio.redis.api

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

  final def echo(a: String): ZIO[RedisExecutor, RedisError, String] = Echo.run(a)

  final def ping(as: String*): ZIO[RedisExecutor, RedisError, String] = Ping.run(as)

  final def select(a: Long): ZIO[RedisExecutor, RedisError, Unit] = Select.run(a)
}

private object Connection {
  final val Auth          = RedisCommand("AUTH", StringInput, UnitOutput)
  final val ClientCaching = RedisCommand(Chunk("CLIENT", "CACHING"), YesNoInput, UnitOutput)
  final val ClientId      = RedisCommand(Chunk("CLIENT", "ID"), NoInput, LongOutput)
  final val ClientKill    = RedisCommand(Chunk("CLIENT", "KILL"), ClientKillInput, LongOutput)
  final val ClientList    = RedisCommand(Chunk("CLIENT", "LIST"), ClientTypeInput, ClientInfoOutput)
  final val ClientGetName = RedisCommand(Chunk("CLIENT", "GETNAME"), NoInput, OptionalOutput(MultiStringOutput))
  final val Echo          = RedisCommand("ECHO", StringInput, MultiStringOutput)
  final val Ping          = RedisCommand("PING", Varargs(StringInput), MultiStringOutput)
  final val Select        = RedisCommand("SELECT", LongInput, UnitOutput)
}
