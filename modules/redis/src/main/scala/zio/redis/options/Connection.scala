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

package zio.redis.options

import zio.{Chunk, Duration}

import java.net.InetAddress
import scala.collection.compat._

trait Connection {
  sealed case class Address(ip: InetAddress, port: Int) {
    private[redis] final def asString: String = s"${ip.getHostAddress}:$port"
  }

  object Address {
    private[redis] final def fromString(addr: String): Option[Address] =
      addr.split(":").toList match {
        case ip :: port :: Nil => port.toIntOption.map(new Address(InetAddress.getByName(ip), _))
        case _                 => None
      }
  }

  sealed case class ClientEvents(readable: Boolean = false, writable: Boolean = false)

  sealed trait ClientFlag

  object ClientFlag {
    case object Blocked                     extends ClientFlag
    case object BroadcastTrackingMode       extends ClientFlag
    case object IsMaster                    extends ClientFlag
    case object KeysTrackingEnabled         extends ClientFlag
    case object MonitorMode                 extends ClientFlag
    case object MultiExecContext            extends ClientFlag
    case object PubSub                      extends ClientFlag
    case object ReadOnlyMode                extends ClientFlag
    case object Replica                     extends ClientFlag
    case object ToBeClosedAfterReply        extends ClientFlag
    case object ToBeClosedAsap              extends ClientFlag
    case object TrackingTargetClientInvalid extends ClientFlag
    case object Unblocked                   extends ClientFlag
    case object UnixDomainSocket            extends ClientFlag
    case object WatchedKeysModified         extends ClientFlag

    private[redis] lazy val Flags =
      Map(
        'A' -> ClientFlag.ToBeClosedAsap,
        'b' -> ClientFlag.Blocked,
        'B' -> ClientFlag.BroadcastTrackingMode,
        'c' -> ClientFlag.ToBeClosedAfterReply,
        'd' -> ClientFlag.WatchedKeysModified,
        'M' -> ClientFlag.IsMaster,
        'O' -> ClientFlag.MonitorMode,
        'P' -> ClientFlag.PubSub,
        'r' -> ClientFlag.ReadOnlyMode,
        'R' -> ClientFlag.TrackingTargetClientInvalid,
        'S' -> ClientFlag.Replica,
        't' -> ClientFlag.KeysTrackingEnabled,
        'u' -> ClientFlag.Unblocked,
        'U' -> ClientFlag.UnixDomainSocket,
        'x' -> ClientFlag.MultiExecContext
      )
  }

  sealed case class ClientInfo(
    id: Option[Long] = None,
    name: Option[String] = None,
    address: Option[Address] = None,
    localAddress: Option[Address] = None,
    fileDescriptor: Option[Long] = None,
    age: Option[Duration] = None,
    idle: Option[Duration] = None,
    flags: Set[ClientFlag] = Set.empty,
    databaseId: Option[Long] = None,
    subscriptions: Option[Int] = None,
    patternSubscriptions: Option[Int] = None,
    multiCommands: Option[Int] = None,
    queryBufferLength: Option[Int] = None,
    queryBufferFree: Option[Int] = None,
    outputBufferLength: Option[Int] = None,
    outputListLength: Option[Int] = None,
    outputBufferMem: Option[Long] = None,
    events: ClientEvents = ClientEvents(),
    lastCommand: Option[String] = None,
    argvMemory: Option[Long] = None,
    totalMemory: Option[Long] = None,
    redirectionClientId: Option[Long] = None,
    user: Option[String] = None
  )

  object ClientInfo {
    private[redis] final def from(line: String): ClientInfo = {
      val data   = line.trim.split(" ").map(_.split("=").toList).collect { case k :: v :: Nil => k -> v }.toMap
      val events = data.get("events")
      new ClientInfo(
        id = data.get("id").flatMap(_.toLongOption),
        name = data.get("name"),
        address = data.get("addr").flatMap(Address.fromString),
        localAddress = data.get("laddr").flatMap(Address.fromString),
        fileDescriptor = data.get("fd").flatMap(_.toLongOption),
        age = data.get("age").flatMap(_.toLongOption).map(Duration.fromSeconds),
        idle = data.get("idle").flatMap(_.toLongOption).map(Duration.fromSeconds),
        flags = data
          .get("flags")
          .fold(Set.empty[ClientFlag])(_.foldLeft(Set.empty[ClientFlag])((fs, f) => fs ++ ClientFlag.Flags.get(f))),
        databaseId = data.get("id").flatMap(_.toLongOption),
        subscriptions = data.get("sub").flatMap(_.toIntOption),
        patternSubscriptions = data.get("psub").flatMap(_.toIntOption),
        multiCommands = data.get("multi").flatMap(_.toIntOption),
        queryBufferLength = data.get("qbuf").flatMap(_.toIntOption),
        queryBufferFree = data.get("qbuf-free").flatMap(_.toIntOption),
        outputListLength = data.get("oll").flatMap(_.toIntOption),
        outputBufferMem = data.get("omem").flatMap(_.toLongOption),
        events = ClientEvents(readable = events.exists(_.contains("r")), writable = events.exists(_.contains("w"))),
        lastCommand = data.get("cmd"),
        argvMemory = data.get("argv-mem").flatMap(_.toLongOption),
        totalMemory = data.get("total-mem").flatMap(_.toLongOption),
        redirectionClientId = data.get("redir").flatMap(_.toLongOption),
        user = data.get("user")
      )
    }

    private[redis] final def from(lines: Array[String]): Chunk[ClientInfo] =
      Chunk.fromArray(lines.map(from))
  }

  sealed trait ClientKillFilter

  object ClientKillFilter {
    sealed case class Address(ip: InetAddress, port: Int) extends ClientKillFilter {
      private[redis] final def asString: String = s"${ip.getHostAddress}:$port"
    }
    sealed case class LocalAddress(ip: InetAddress, port: Int) extends ClientKillFilter {
      private[redis] final def asString: String = s"${ip.getHostAddress}:$port"
    }
    sealed case class Id(id: Long)                 extends ClientKillFilter
    sealed case class Type(clientType: ClientType) extends ClientKillFilter
    sealed case class User(username: String)       extends ClientKillFilter
    sealed case class SkipMe(skip: Boolean)        extends ClientKillFilter
  }

  sealed trait ClientPauseMode { self =>
    private[redis] final def asString: String =
      self match {
        case ClientPauseMode.All   => "ALL"
        case ClientPauseMode.Write => "WRITE"
      }
  }

  object ClientPauseMode {
    case object All   extends ClientPauseMode
    case object Write extends ClientPauseMode
  }

  sealed case class ClientTrackingFlags(
    clientSideCaching: Boolean,
    trackingMode: Option[ClientTrackingMode] = None,
    noLoop: Boolean = false,
    caching: Option[Boolean] = None,
    brokenRedirect: Boolean = false
  )

  sealed case class ClientTrackingInfo(
    flags: ClientTrackingFlags,
    redirect: ClientTrackingRedirect,
    prefixes: Set[String] = Set.empty
  )

  sealed trait ClientTrackingMode { self =>
    private[redis] final def asString: String =
      self match {
        case ClientTrackingMode.OptIn     => "OPTIN"
        case ClientTrackingMode.OptOut    => "OPTOUT"
        case ClientTrackingMode.Broadcast => "BCAST"
      }

  }

  object ClientTrackingMode {
    case object OptIn     extends ClientTrackingMode
    case object OptOut    extends ClientTrackingMode
    case object Broadcast extends ClientTrackingMode
  }

  sealed trait ClientTrackingRedirect

  object ClientTrackingRedirect {
    case object NotEnabled                         extends ClientTrackingRedirect
    case object NotRedirected                      extends ClientTrackingRedirect
    sealed case class RedirectedTo(clientId: Long) extends ClientTrackingRedirect
  }

  sealed trait ClientType { self =>
    private[redis] final def asString: String =
      self match {
        case ClientType.Normal  => "NORMAL"
        case ClientType.Master  => "MASTER"
        case ClientType.Replica => "REPLICA"
        case ClientType.PubSub  => "PUBSUB"
      }
  }

  object ClientType {
    case object Normal  extends ClientType
    case object Master  extends ClientType
    case object Replica extends ClientType
    case object PubSub  extends ClientType
  }

  sealed trait UnblockBehavior { self =>
    private[redis] final def asString: String =
      self match {
        case UnblockBehavior.Timeout => "TIMEOUT"
        case UnblockBehavior.Error   => "ERROR"
      }
  }

  object UnblockBehavior {
    case object Timeout extends UnblockBehavior
    case object Error   extends UnblockBehavior
  }

}
