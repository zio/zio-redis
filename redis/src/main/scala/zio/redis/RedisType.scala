package zio.redis

sealed trait RedisType

object RedisType {
  case object String    extends RedisType
  case object List      extends RedisType
  case object Set       extends RedisType
  case object SortedSet extends RedisType
  case object Hash      extends RedisType
}
