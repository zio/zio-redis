package zio.redis.options

trait Keys {

  case object AbsTtl
  type AbsTtl = AbsTtl.type

  case object Alpha
  type Alpha = Alpha.type

  sealed case class Auth(password: String)

  sealed case class By(pattern: String)

  case object Copy
  type Copy = Copy.type

  sealed case class IdleTime(seconds: Long)

  sealed case class Freq(frequency: String)

  case object Replace
  type Replace = Replace.type

}
