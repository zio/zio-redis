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

  case object Replace {
    private[redis] def stringify: String = "REPLACE"
  }

  type Replace = Replace.type

}
