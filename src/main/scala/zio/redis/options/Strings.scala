package zio.redis.options

trait Strings {

  sealed trait BitFieldType
  object BitFieldType {
    sealed case class UnsignedInt(size: Int) extends BitFieldType
    sealed case class SignedInt(size: Int)   extends BitFieldType
  }

  case class BitFieldGet(`type`: BitFieldType, offset: Int)
  case class BitFieldSet(`type`: BitFieldType, offset: Int, value: BigInt)
  case class BitFieldIncr(`type`: BitFieldType, offset: Int, increment: BigInt)

  sealed trait BitFieldOverflow
  object BitFieldOverflow {
    case object FAIL extends BitFieldOverflow
    case object SAT  extends BitFieldOverflow
    case object WRAP extends BitFieldOverflow
  }

  sealed trait BitOperation
  object BitOperation {
    case object AND extends BitOperation
    case object OR  extends BitOperation
    case object XOR extends BitOperation
  }

  case class BitPosRange(start: Long, end: Option[Long])

  sealed trait Expiration
  object Expiration {
    sealed case class EX(seconds: Long)      extends Expiration
    sealed case class PX(milliSeconds: Long) extends Expiration
  }

  case object KEEPTTL
  type KEEPTTL = KEEPTTL.type
}
