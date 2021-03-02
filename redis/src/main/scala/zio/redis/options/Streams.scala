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
    length: Long,
    radixTreeKeys: Long,
    radixTreeNodes: Long,
    groups: Long,
    lastGeneratedId: String,
    firstEntry: StreamEntry,
    lastEntry: StreamEntry
  )

  case class StreamGroupInfo(name: String, consumers: Long, pending: Long, lastDeliveredId: String)

  case class StreamConsumerInfo(name: String, idle: Long, pending: Long)

}
