package zio.redis.options

import java.net.InetAddress

import zio.duration.Duration

trait Connection {
  sealed case class Address(ip: InetAddress, port: Int) {
    private[redis] final def stringify: String = s"${ip.getHostAddress}:$port"
  }

  sealed case class ClientEvents(readable: Boolean, writable: Boolean)

  sealed trait ClientFlag

  object ClientFlag {
    case object ToBeClosedAsap              extends ClientFlag
    case object Blocked                     extends ClientFlag
    case object ToBeClosedAfterReply        extends ClientFlag
    case object WatchedKeysModified         extends ClientFlag
    case object IsMaster                    extends ClientFlag
    case object MonitorMode                 extends ClientFlag
    case object PubSub                      extends ClientFlag
    case object ReadOnlyMode                extends ClientFlag
    case object Replica                     extends ClientFlag
    case object Unblocked                   extends ClientFlag
    case object UnixDomainSocket            extends ClientFlag
    case object MultiExecContext            extends ClientFlag
    case object KeysTrackingEnabled         extends ClientFlag
    case object TrackingTargetClientInvalid extends ClientFlag
    case object BroadcastTrackingMode       extends ClientFlag
  }

  sealed case class ClientInfo(
    id: Long,
    name: Option[String],
    address: Option[InetAddress],
    localAddress: Option[InetAddress],
    fileDescriptor: Option[Long],
    age: Option[Duration],
    idle: Option[Duration],
    flags: Set[ClientFlag],
    databaseId: Option[Long],
    subscriptions: Int,
    patternSubscriptions: Int,
    multiCommands: Int,
    queryBufferLength: Option[Int],
    queryBufferFree: Option[Int],
    outputBufferLength: Option[Int],
    outputListLength: Option[Int],
    outputBufferMem: Option[Long],
    events: ClientEvents,
    lastCommand: Option[String],
    argvMemory: Option[Long],
    totalMemory: Option[Long],
    redirectionClientId: Option[Long],
    user: Option[String]
  )

  sealed trait ClientKillFilter

  object ClientKillFilter {
    sealed case class Address(ip: InetAddress, port: Int) extends ClientKillFilter {
      private[redis] final def stringify: String = s"${ip.getHostAddress}:$port"
    }
    sealed case class LocalAddress(ip: InetAddress, port: Int) extends ClientKillFilter {
      private[redis] final def stringify: String = s"${ip.getHostAddress}:$port"
    }
    sealed case class Id(id: Long)                 extends ClientKillFilter
    sealed case class Type(clientType: ClientType) extends ClientKillFilter
    sealed case class User(username: String)       extends ClientKillFilter
    sealed case class SkipMe(skip: Boolean)        extends ClientKillFilter
  }

  sealed trait ClientPauseMode { self =>
    private[redis] final def stringify: String =
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
    trackingMode: Option[ClientTrackingMode],
    noLoop: Boolean,
    caching: Option[Boolean],
    brokenRedirect: Boolean
  )

  sealed case class ClientTrackingInfo(
    flags: ClientTrackingFlags,
    redirect: Option[Long],
    prefixes: Set[String]
  )

  sealed trait ClientTrackingMode { self =>
    private[redis] final def stringify: String =
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
    private[redis] final def stringify: String =
      self match {
        case ClientType.Normal  => "normal"
        case ClientType.Master  => "master"
        case ClientType.Replica => "replica"
        case ClientType.PubSub  => "pubsub"
      }
  }

  object ClientType {
    case object Normal  extends ClientType
    case object Master  extends ClientType
    case object Replica extends ClientType
    case object PubSub  extends ClientType
  }

  sealed trait UnblockBehavior { self =>
    private[redis] final def stringify: String =
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
