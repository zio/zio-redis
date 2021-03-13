package zio.redis.options

import zio.duration._

trait Streams {

  case object WithForce {
    private[redis] def stringify = "FORCE"
  }

  type WithForce = WithForce.type

  case object WithJustId {
    private[redis] def stringify = "JUSTID"
  }

  type WithJustId = WithJustId.type

  sealed trait XGroupCommand

  object XGroupCommand {

    case class Create(key: String, group: String, id: String, mkStream: Boolean) extends XGroupCommand

    case class SetId(key: String, group: String, id: String) extends XGroupCommand

    case class Destroy(key: String, group: String) extends XGroupCommand

    case class CreateConsumer(key: String, group: String, consumer: String) extends XGroupCommand

    case class DelConsumer(key: String, group: String, consumer: String) extends XGroupCommand

  }

  sealed trait XInfoCommand

  object XInfoCommand {

    case class Group(key: String) extends XInfoCommand

    case class Stream(key: String) extends XInfoCommand

    case class Consumer(key: String, group: String) extends XInfoCommand
  }

  case object MkStream {
    private[redis] def stringify = "MKSTREAM"
  }

  type MkStream = MkStream.type

  case class PendingInfo(
    total: Long,
    first: Option[String],
    last: Option[String],
    consumers: Map[String, Long]
  )

  case class PendingMessage(
    id: String,
    owner: String,
    lastDelivered: Duration,
    counter: Long
  )

  case class Group(group: String, consumer: String)

  case object NoAck {
    private[redis] def stringify: String = "NOACK"
  }

  type NoAck = NoAck.type

  case class MaxLen(approximate: Boolean, count: Long)

  case class StreamEntry(id: String, fields: Map[String, String])

  case class StreamInfo(
    length: Option[Long],
    radixTreeKeys: Option[Long],
    radixTreeNodes: Option[Long],
    groups: Option[Long],
    lastGeneratedId: Option[String],
    firstEntry: Option[StreamEntry],
    lastEntry: Option[StreamEntry]
  )

  object StreamInfo {
    def empty: StreamInfo = StreamInfo(None, None, None, None, None, None, None)
  }

  case class StreamGroupInfo(
    name: Option[String],
    consumers: Option[Long],
    pending: Option[Long],
    lastDeliveredId: Option[String]
  )

  object StreamGroupInfo {
    def empty: StreamGroupInfo = StreamGroupInfo(None, None, None, None)
  }

  case class StreamConsumerInfo(
    name: Option[String],
    idle: Option[Long],
    pending: Option[Long]
  )

  object StreamConsumerInfo {
    def empty: StreamConsumerInfo = StreamConsumerInfo(None, None, None)
  }

  private[redis] object XInfoFields {
    val Name: String    = "name"
    val Idle: String    = "idle"
    val Pending: String = "pending"

    val Consumers: String     = "consumers"
    val LastDelivered: String = "last-delivered-id"

    val Length: String          = "length"
    val RadixTreeKeys: String   = "radix-tree-keys"
    val RadixTreeNodes: String  = "radix-tree-nodes"
    val Groups: String          = "groups"
    val LastGeneratedId: String = "last-generated-id"
    val FirstEntry: String      = "first-entry"
    val LastEntry: String       = "last-entry"

    val Entries: String  = "entries"
    val PelCount: String = "pel-count"
    val SeenTime: String = "seen-time"
  }

}
