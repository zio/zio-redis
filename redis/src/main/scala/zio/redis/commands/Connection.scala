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

package zio.redis.commands

import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.{Chunk, Duration}

private[redis] trait Connection extends RedisEnvironment {
  import Connection.{Auth => _}

  final val _auth: RedisCommand[Auth, Unit] = RedisCommand(Connection.Auth, AuthInput, UnitOutput, codec, executor)

  final val _clientCaching: RedisCommand[Boolean, Unit] =
    RedisCommand(Connection.ClientCaching, YesNoInput, UnitOutput, codec, executor)

  final val _clientId: RedisCommand[Unit, Long] =
    RedisCommand(Connection.ClientId, NoInput, LongOutput, codec, executor)

  final val _clientKill: RedisCommand[Address, Unit] =
    RedisCommand(Connection.ClientKill, AddressInput, UnitOutput, codec, executor)

  final val _clientKillByFilter: RedisCommand[Iterable[ClientKillFilter], Long] =
    RedisCommand(Connection.ClientKill, Varargs(ClientKillInput), LongOutput, codec, executor)

  final val _clientGetName: RedisCommand[Unit, Option[String]] =
    RedisCommand(Connection.ClientGetName, NoInput, OptionalOutput(MultiStringOutput), codec, executor)

  final val _clientGetRedir: RedisCommand[Unit, ClientTrackingRedirect] =
    RedisCommand(Connection.ClientGetRedir, NoInput, ClientTrackingRedirectOutput, codec, executor)

  final val _clientUnpause: RedisCommand[Unit, Unit] =
    RedisCommand(Connection.ClientUnpause, NoInput, UnitOutput, codec, executor)

  final val _clientPause: RedisCommand[(Duration, Option[ClientPauseMode]), Unit] = RedisCommand(
    Connection.ClientPause,
    Tuple2(DurationMillisecondsInput, OptionalInput(ClientPauseModeInput)),
    UnitOutput,
    codec,
    executor
  )

  final val _clientSetName: RedisCommand[String, Unit] =
    RedisCommand(Connection.ClientSetName, StringInput, UnitOutput, codec, executor)

  final val _clientTrackingOn: RedisCommand[
    Option[(Option[Long], Option[ClientTrackingMode], Boolean, Chunk[String])],
    Unit
  ] =
    RedisCommand(Connection.ClientTracking, ClientTrackingInput, UnitOutput, codec, executor)

  final val _clientTrackingOff: RedisCommand[
    Option[(Option[Long], Option[zio.redis.ClientTrackingMode], Boolean, Chunk[String])],
    Unit
  ] =
    RedisCommand(Connection.ClientTracking, ClientTrackingInput, UnitOutput, codec, executor)

  final val _clientTrackingInfo: RedisCommand[Unit, zio.redis.ClientTrackingInfo] =
    RedisCommand(Connection.ClientTrackingInfo, NoInput, ClientTrackingInfoOutput, codec, executor)

  final val _clientUnblock: RedisCommand[(Long, Option[UnblockBehavior]), Boolean] = RedisCommand(
    Connection.ClientUnblock,
    Tuple2(LongInput, OptionalInput(UnblockBehaviorInput)),
    BoolOutput,
    codec,
    executor
  )

  final val _echo: RedisCommand[String, String] =
    RedisCommand(Connection.Echo, StringInput, MultiStringOutput, codec, executor)

  final val _ping: RedisCommand[Option[String], String] =
    RedisCommand(Connection.Ping, OptionalInput(StringInput), SingleOrMultiStringOutput, codec, executor)

  final val _quit: RedisCommand[Unit, Unit] = RedisCommand(Connection.Quit, NoInput, UnitOutput, codec, executor)

  final val _reset: RedisCommand[Unit, Unit] = RedisCommand(Connection.Reset, NoInput, ResetOutput, codec, executor)

  final val _select: RedisCommand[Long, Unit] = RedisCommand(Connection.Select, LongInput, UnitOutput, codec, executor)
}

private object Connection {
  final val Auth               = "AUTH"
  final val ClientCaching      = "CLIENT CACHING"
  final val ClientId           = "CLIENT ID"
  final val ClientKill         = "CLIENT KILL"
  final val ClientGetName      = "CLIENT GETNAME"
  final val ClientGetRedir     = "CLIENT GETREDIR"
  final val ClientUnpause      = "CLIENT UNPAUSE"
  final val ClientPause        = "CLIENT PAUSE"
  final val ClientSetName      = "CLIENT SETNAME"
  final val ClientTracking     = "CLIENT TRACKING"
  final val ClientTrackingInfo = "CLIENT TRACKINGINFO"
  final val ClientUnblock      = "CLIENT UNBLOCK"
  final val Echo               = "ECHO"
  final val Ping               = "PING"
  final val Quit               = "QUIT"
  final val Reset              = "RESET"
  final val Select             = "SELECT"
}
