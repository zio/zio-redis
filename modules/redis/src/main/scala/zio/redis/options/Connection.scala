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

trait Connection {
  sealed case class Address(ip: InetAddress, port: Int) {
    private[redis] final def asString: String = s"${ip.getHostAddress}:$port"
  }

  object Address {
    private[redis] final def fromString(addr: String): Option[Address] =
      addr.split(":").toList match {
        case ip :: port :: Nil => Some(new Address(InetAddress.getByName(ip), port.toInt))
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
  }

  sealed case class ClientInfo(
    id: Long,
    name: Option[String] = None,
    address: Option[Address] = None,
    localAddress: Option[Address] = None,
    fileDescriptor: Option[Long] = None,
    age: Option[Duration] = None,
    idle: Option[Duration] = None,
    flags: Set[ClientFlag] = Set.empty,
    databaseId: Option[Long] = None,
    subscriptions: Int = 0,
    patternSubscriptions: Int = 0,
    multiCommands: Int = 0,
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
    private def clientFlag(cf: Char): Option[ClientFlag] = cf match {
      case 'A' => Some(ClientFlag.ToBeClosedAsap)
      case 'b' => Some(ClientFlag.Blocked)
      case 'B' => Some(ClientFlag.BroadcastTrackingMode)
      case 'c' => Some(ClientFlag.ToBeClosedAfterReply)
      case 'd' => Some(ClientFlag.WatchedKeysModified)
      case 'M' => Some(ClientFlag.IsMaster)
      case 'O' => Some(ClientFlag.MonitorMode)
      case 'P' => Some(ClientFlag.PubSub)
      case 'r' => Some(ClientFlag.ReadOnlyMode)
      case 'R' => Some(ClientFlag.TrackingTargetClientInvalid)
      case 'S' => Some(ClientFlag.Replica)
      case 't' => Some(ClientFlag.KeysTrackingEnabled)
      case 'u' => Some(ClientFlag.Unblocked)
      case 'U' => Some(ClientFlag.UnixDomainSocket)
      case 'x' => Some(ClientFlag.MultiExecContext)
      case _   => None
    }

    private def getFlags(flags: String): Set[ClientFlag] =
      flags.foldLeft(Set.empty[ClientFlag])((fs, f) => fs ++ clientFlag(f))

    private[redis] final def from(line: String): ClientInfo = {
      val data   = line.trim.split(" ").map(_.split("=").toList).collect { case k :: v :: Nil => k -> v }.toMap
      val events = data.get("events")
      new ClientInfo(
        id = data("id").toLong,
        name = data.get("name"),
        address = data.get("addr").flatMap(Address.fromString),
        localAddress = data.get("laddr").flatMap(Address.fromString),
        fileDescriptor = data.get("fd").flatMap(_.toLongOption),
        age = data.get("age").flatMap(_.toLongOption).map(Duration.fromSeconds),
        idle = data.get("idle").flatMap(_.toLongOption).map(Duration.fromSeconds),
        flags = data.get("flags").fold(Set.empty[ClientFlag])(getFlags),
        databaseId = data.get("id").flatMap(_.toLongOption),
        subscriptions = data("sub").toInt,
        patternSubscriptions = data("psub").toInt,
        multiCommands = data("multi").toInt,
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
