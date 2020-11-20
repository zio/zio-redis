package zio.redis.options

trait Keys {

  case object AbsTtl {
    private[redis] def stringify: String = "ABSTTL"
  }

  type AbsTtl = AbsTtl.type

  case object Alpha
  type Alpha = Alpha.type

  sealed case class Auth(password: String)

  sealed case class By(pattern: String)

  case object Copy {
    private[redis] def stringify: String = "COPY"
  }

  type Copy = Copy.type

  sealed case class IdleTime(seconds: Long)

  sealed case class Freq(frequency: String)

  sealed trait RedisType

  object RedisType {
    case object String    extends RedisType
    case object List      extends RedisType
    case object Set       extends RedisType
    case object SortedSet extends RedisType
    case object Hash      extends RedisType
  }

  case object Replace {
    private[redis] def stringify: String = "REPLACE"
  }

  type Replace = Replace.type

}
