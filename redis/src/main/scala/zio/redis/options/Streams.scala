package zio.redis.options

import zio.duration._

trait Streams {
  sealed case class Idle(millis: Long)

  sealed case class Time(millis: Long)

  sealed case class RetryCount(count: Long)

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
    case class SetId(key: String, group: String, id: String)                     extends XGroupCommand
    case class Destroy(key: String, group: String)                               extends XGroupCommand
    case class CreateConsumer(key: String, group: String, consumer: String)      extends XGroupCommand
    case class DelConsumer(key: String, group: String, consumer: String)         extends XGroupCommand
  }

  case object MkStream {
    private[redis] def stringify = "MKSTREAM"
  }

  type MkStream = MkStream.type

  case class PendingInfo(
    total: Long,
    first: String,
    last: String,
    consumers: Map[String, Long]
  )

  case class PendingMessage(
    id: String,
    owner: String,
    lastDelivered: Duration,
    counter: Long
  )

  case class Block(value: Duration)

  case class Group(group: String, consumer: String)

  case object NoAck {
    private[redis] def stringify: String = "NOACK"
  }

  type NoAck = NoAck.type

  case class MaxLen(approximate: Boolean, count: Long)
}
