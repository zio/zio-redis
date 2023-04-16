/*
 * Copyright 2021 John A. De Goes and the ZIO contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.redis.api

import zio._
import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.redis.internal.{RedisCommand, RedisEnvironment}

trait Connection extends RedisEnvironment {
  import Connection.{Auth => _, _}

  /**
   * Authenticates the current connection to the server in two cases:
   *   - If the Redis server is password protected via the the ''requirepass'' option
   *   - If a Redis 6.0 instance, or greater, is using the [[https://redis.io/topics/acl Redis ACL system]]. In this
   *     case it is assumed that the implicit username is ''default''.
   *
   * @param password
   *   the password used to authenticate the connection
   * @return
   *   if the password provided via AUTH matches the password in the configuration file, the Unit value is returned and
   *   the server starts accepting commands. Otherwise, an error is returned and the client needs to try a new password.
   */
  final def auth(password: String): IO[RedisError, Unit] = {
    val command = RedisCommand(Connection.Auth, AuthInput, UnitOutput, executor)

    command.run(Auth(None, password))
  }

  /**
   * Authenticates the current connection to the server using username and password.
   *
   * @param username
   *   the username used to authenticate the connection
   * @param password
   *   the password used to authenticate the connection
   * @return
   *   if the password provided via AUTH matches the password in the configuration file, the Unit value is returned and
   *   the server starts accepting commands. Otherwise, an error is returned and the client needs to try a new password.
   */
  final def auth(username: String, password: String): IO[RedisError, Unit] = {
    val command = RedisCommand(Connection.Auth, AuthInput, UnitOutput, executor)

    command.run(Auth(Some(username), password))
  }

  /**
   * Returns the name of the current connection as set by clientSetName
   *
   * @return
   *   the connection name, or None if a name wasn't set.
   */
  final def clientGetName: IO[RedisError, Option[String]] = {
    val command = RedisCommand(ClientGetName, NoInput, OptionalOutput(MultiStringOutput), executor)

    command.run(())
  }

  /**
   * Returns the ID of the current connection. Every connection ID has certain guarantees:
   *   - It is never repeated, so if clientID returns the same number, the caller can be sure that the underlying client
   *     did not disconnect and reconnect the connection, but it is still the same connection.
   *   - The ID is monotonically incremental. If the ID of a connection is greater than the ID of another connection, it
   *     is guaranteed that the second connection was established with the server at a later time.
   *
   * @return
   *   the ID of the current connection.
   */
  final def clientId: IO[RedisError, Long] = {
    val command = RedisCommand(ClientId, NoInput, LongOutput, executor)

    command.run(())
  }

  /**
   * Closes a given client connection with the specified address
   *
   * @param address
   *   the address of the client to kill
   * @return
   *   the Unit value.
   */
  final def clientKill(address: Address): IO[RedisError, Unit] = {
    val command = RedisCommand(ClientKill, AddressInput, UnitOutput, executor)

    command.run(address)
  }

  /**
   * Closes client connections with the specified filters.The following filters are available:
   *   - Address(ip, port). Kill all clients connected to specified address
   *   - LocalAddress(ip, port). Kill all clients connected to specified local (bind) address
   *   - Id(id). Allows to kill a client by its unique ID field. Client ID's are retrieved using the CLIENT LIST command
   *   - ClientType, where the type is one of normal, master, replica and pubsub. This closes the connections of all the
   *     clients in the specified class. Note that clients blocked into the MONITOR command are considered to belong to
   *     the normal class
   *   - User(username). Closes all the connections that are authenticated with the specified ACL username, however it
   *     returns an error if the username does not map to an existing ACL user
   *   - SkipMe(skip). By default this option is set to yes, that is, the client calling the command will not get
   *     killed, however setting this option to no will have the effect of also killing the client calling the command
   *     It is possible to provide multiple filters at the same time. The command will handle multiple filters via
   *     logical AND
   *
   * @param filters
   *   the specified filters for killing clients
   * @return
   *   the number of clients killed.
   */
  final def clientKill(filters: ClientKillFilter*): IO[RedisError, Long] = {
    val command = RedisCommand(ClientKill, Varargs(ClientKillInput), LongOutput, executor)

    command.run(filters)
  }

  /**
   * Assigns a name to the current connection
   *
   * @param name
   *   the name to be assigned
   * @return
   *   the Unit value.
   */
  final def clientSetName(name: String): IO[RedisError, Unit] = {
    val command = RedisCommand(ClientSetName, StringInput, UnitOutput, executor)

    command.run(name)
  }

  /**
   * Unblocks, from a different connection, a client blocked in a blocking operation
   *
   * @param clientId
   *   the ID of the client to unblock
   * @param error
   *   option to specify the unblocking behavior
   * @return
   *   true if the client was unblocked successfully, or false if the client wasn't unblocked.
   */
  final def clientUnblock(clientId: Long, error: Option[UnblockBehavior] = None): IO[RedisError, Boolean] = {
    val command =
      RedisCommand(ClientUnblock, Tuple2(LongInput, OptionalInput(UnblockBehaviorInput)), BoolOutput, executor)

    command.run((clientId, error))
  }

  /**
   * Pings the server.
   *
   * @param message
   *   the optional message to receive back from server
   * @return
   *   PONG if no argument is provided, otherwise return a copy of the argument as a bulk. This command is often used to
   *   test if a connection is still alive, or to measure latency.
   */
  final def ping(message: Option[String] = None): IO[RedisError, String] = {
    val command = RedisCommand(Ping, OptionalInput(StringInput), SingleOrMultiStringOutput, executor)

    command.run(message)
  }

  /**
   * Changes the database for the current connection to the database having the specified numeric index. The currently
   * selected database is a property of the connection; clients should track the selected database and re-select it on
   * reconnection.
   *
   * @param index
   *   the database index. The index is zero-based. New connections always use the database 0
   * @return
   *   the Unit value.
   */
  final def select(index: Long): IO[RedisError, Unit] = {
    val command = RedisCommand(Select, LongInput, UnitOutput, executor)

    command.run(index)
  }
}

private[redis] object Connection {
  final val Auth          = "AUTH"
  final val ClientGetName = "CLIENT GETNAME"
  final val ClientId      = "CLIENT ID"
  final val ClientKill    = "CLIENT KILL"
  final val ClientSetName = "CLIENT SETNAME"
  final val ClientUnblock = "CLIENT UNBLOCK"
  final val Ping          = "PING"
  final val Select        = "SELECT"
}
