package zio.redis.options

trait Keys {

  type AbsTtl = AbsTtl.type

  case object AbsTtl {
    private[redis] def stringify: String = "ABSTTL"
  }

  type Alpha = Alpha.type
  case object Alpha

  sealed case class Auth(password: String)

  sealed case class By(pattern: String)

  type Copy = Copy.type

  case object Copy {
    private[redis] def stringify: String = "COPY"
  }

  sealed case class IdleTime(seconds: Long)

  sealed case class Freq(frequency: String)

  case object Replace {
    private[redis] def stringify: String = "REPLACE"
  }

  type Replace = Replace.type

}
