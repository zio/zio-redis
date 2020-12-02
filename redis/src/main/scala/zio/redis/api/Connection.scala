package zio.redis.api

import zio.ZIO
import zio.redis.Input._
import zio.redis.Output._
import zio.redis._

trait Connection {
  import Connection._

  /**
   * Authenticates the current connection to the server in two cases:
   *  - If the Redis server is password protected via the the ''requirepass'' option
   *  - If a Redis 6.0 instance, or greater, is using the [[https://redis.io/topics/acl Redis ACL system]]. In this case
   *  it is assumed that the implicit username is ''default''
   *
   * @param password the password used to authenticate the connection
   * @return if the password provided via AUTH matches the password in the configuration file, the Unit value is
   *         returned and the server starts accepting commands.
   *         Otherwise, an error is returned and the client needs to try a new password
   */
  final def auth(password: String): ZIO[RedisExecutor, RedisError, Unit] = Auth.run(password)

  /**
   * Echoes the given string
   *
   * @param message the message to be echoed
   * @return the message
   */
  final def echo(message: String): ZIO[RedisExecutor, RedisError, String] = Echo.run(message)

  /**
   * Pings the server
   *
   * @param message the optional message to receive back from server
   * @return PONG if no argument is provided, otherwise return a copy of the argument as a bulk.
   *         This command is often used to test if a connection is still alive, or to measure latency
   */
  final def ping(message: Option[String] = None): ZIO[RedisExecutor, RedisError, String] = Ping.run(message)

  /**
   * Changes the database for the current connection to the database having the specified numeric index.
   * The currently selected database is a property of the connection; clients should track the selected database and
   * re-select it on reconnection
   *
   * @param index the database index. The index is zero-based. New connections always use the database 0
   * @return the Unit value
   */
  final def select(index: Long): ZIO[RedisExecutor, RedisError, Unit] = Select.run(index)
}

private[redis] object Connection {
  final val Auth: RedisCommand[String, Unit]   = RedisCommand("AUTH", StringInput, UnitOutput)
  final val Echo: RedisCommand[String, String] = RedisCommand("ECHO", StringInput, MultiStringOutput)
  final val Ping: RedisCommand[Option[String], String] =
    RedisCommand("PING", OptionalInput(StringInput), SingleOrMultiStringOutput)
  final val Select: RedisCommand[Long, Unit] = RedisCommand("SELECT", LongInput, UnitOutput)
}
