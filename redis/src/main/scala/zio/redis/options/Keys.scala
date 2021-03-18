package zio.redis.options

import zio.redis.RedisError.WrongType

trait Keys {

  case object AbsTtl {
    private[redis] def stringify: String = "ABSTTL"
  }

  type AbsTtl = AbsTtl.type

  case object Alpha {
    private[redis] def stringify: String = "ALPHA"
  }

  type Alpha = Alpha.type

  sealed case class Auth(password: String)

  sealed case class By(pattern: String)

  case object Copy {
    private[redis] def stringify: String = "COPY"
  }

  type Copy = Copy.type

  sealed case class IdleTime(seconds: Long)

  sealed case class Freq(frequency: String)

  sealed trait RedisType extends Product with Serializable { self =>
    private[redis] final def stringify: String =
      self match {
        case RedisType.String    => "string"
        case RedisType.List      => "list"
        case RedisType.Set       => "set"
        case RedisType.SortedSet => "zset"
        case RedisType.Hash      => "hash"
        case RedisType.Stream    => "stream"
      }
  }

  object RedisType {
    def from(value: String): RedisType = value match {
      case "string" => String
      case "list"   => List
      case "set"    => Set
      case "zset"   => SortedSet
      case "hash"   => Hash
      case "stream" => Stream
      case _ =>
        throw WrongType(s"$value is not a valid redis type use one of string, list, set, zset, hash and stream.")
    }

    case object String    extends RedisType
    case object List      extends RedisType
    case object Set       extends RedisType
    case object SortedSet extends RedisType
    case object Hash      extends RedisType
    case object Stream    extends RedisType
  }

  case object Replace {
    private[redis] def stringify: String = "REPLACE"
  }

  type Replace = Replace.type

}
