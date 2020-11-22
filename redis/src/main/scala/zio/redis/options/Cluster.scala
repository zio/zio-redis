package zio.redis.options

import zio.Chunk
import zio.redis.RedisError.ProtocolError
import scala.util.Try

trait Cluster {

  case object Nodes {
    private[redis] def stringify: String = "NODES"
  }
  type Nodes = Nodes.type

  sealed trait ClusterState

  object ClusterState {
    case object Ok   extends ClusterState
    case object Fail extends ClusterState
  }

  sealed case class ClusterInfo(
    state: ClusterState,
    slotsAssigned: Int,
    slotsOk: Int,
    slotsPfail: Int,
    slotsFail: Int,
    knownNodes: Int,
    size: Int,
    currentEpoch: Int,
    myEpoch: Int,
    statsMessagesSent: Int,
    statsMessagesReceived: Int
  )

  sealed trait LinkState

  object LinkState {
    case object Connected    extends LinkState
    case object Disconnected extends LinkState
  }

  sealed trait Slot {
    def covers(slot: Int): Boolean
  }

  object Slot {
    sealed case class HashSlot(asInt: Int)            extends Slot {
      override def covers(slot: Int): Boolean = asInt == slot
    }
    sealed case class RangeSlot(start: Int, end: Int) extends Slot {
      override def covers(slot: Int): Boolean = slot >= start && slot <= end
    }

    def fromString(slot: String): Option[Slot] =
      Try(fromStringUnsafe(slot)).toOption

    def fromStringUnsafe(slot: String): Slot =
      slot.split('-') match {
        case Array(h)          =>
          HashSlot(
            try h.toInt
            catch {
              case _: NumberFormatException => throw ProtocolError(s"slot: $slot is not a number")
            }
          )
        case Array(start, end) =>
          val s =
            try start.toInt
            catch {
              case _: NumberFormatException =>
                throw ProtocolError(s"slot: $slot has a non numeric start range")
            }
          val e =
            try end.toInt
            catch {
              case _: NumberFormatException =>
                throw ProtocolError(s"slot: $slot has a non numeric end range")
            }
          if (s < e) RangeSlot(s, e) else HashSlot(s)
        case _                 => throw ProtocolError(s"$slot: unexpected slot format")
      }
  }

  sealed case class ClusterNode(
    id: String,
    ip: String,
    port: Int,
    cport: Int,
    flags: Chunk[String],
    master: Option[String],
    pingSent: Long,
    pongRecv: Long,
    configEpoch: Int,
    linkState: LinkState,
    slots: Chunk[Slot]
  ) {
    def isConnected: Boolean = linkState == LinkState.Connected
  }
}
