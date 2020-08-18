package zio.redis

sealed trait ConnectionType

object ConnectionType {
  case object Base         extends ConnectionType
  case object Streams      extends ConnectionType
  case object Transactions extends ConnectionType
}
