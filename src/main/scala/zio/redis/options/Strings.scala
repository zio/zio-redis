package zio.redis.options

trait Strings {

  sealed case class BitFieldGet(`type`: BitFieldType, offset: Int)

  sealed case class BitFieldIncr(`type`: BitFieldType, offset: Int, increment: BigInt)

  sealed case class BitFieldSet(`type`: BitFieldType, offset: Int, value: BigInt)

  sealed trait BitFieldOverflow

  object BitFieldOverflow {
    case object Fail extends BitFieldOverflow
    case object Sat  extends BitFieldOverflow
    case object Wrap extends BitFieldOverflow
  }

  sealed trait BitFieldType

  object BitFieldType {
    sealed case class UnsignedInt(size: Int) extends BitFieldType
    sealed case class SignedInt(size: Int)   extends BitFieldType
  }

  sealed trait BitOperation

  object BitOperation {
    case object AND extends BitOperation
    case object OR  extends BitOperation
    case object XOR extends BitOperation
  }

  sealed case class BitPosRange(start: Long, end: Option[Long])

  case object KeepTtl
  type KeepTtl = KeepTtl.type
}
