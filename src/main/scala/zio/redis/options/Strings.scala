package zio.redis.options

trait Strings {

  sealed case class BitFieldGet(`type`: BitFieldType, offset: Int)

  sealed case class BitFieldIncr(`type`: BitFieldType, offset: Int, increment: Long)

  sealed case class BitFieldSet(`type`: BitFieldType, offset: Int, value: Long)

  sealed trait BitFieldOverflow { self =>
    private[redis] final def stringify: String =
      self match {
        case BitFieldOverflow.Fail => "FAIL"
        case BitFieldOverflow.Sat  => "SAT"
        case BitFieldOverflow.Wrap => "WRAP"
      }
  }

  object BitFieldOverflow {
    case object Fail extends BitFieldOverflow
    case object Sat  extends BitFieldOverflow
    case object Wrap extends BitFieldOverflow
  }

  sealed trait BitFieldType { self =>
    private[redis] final def stringify: String =
      self match {
        case BitFieldType.UnsignedInt(size) => s"u$size"
        case BitFieldType.SignedInt(size)   => s"i$size"
      }
  }

  object BitFieldType {
    sealed case class UnsignedInt(size: Int) extends BitFieldType
    sealed case class SignedInt(size: Int)   extends BitFieldType
  }

  sealed trait BitOperation { self =>
    private[redis] final def stringify: String =
      self match {
        case BitOperation.AND => "AND"
        case BitOperation.OR  => "OR"
        case BitOperation.XOR => "XOR"
      }
  }

  object BitOperation {
    case object AND extends BitOperation
    case object OR  extends BitOperation
    case object XOR extends BitOperation
  }

  sealed case class BitPosRange(start: Long, end: Option[Long])

  case object KeepTtl {
    private[redis] def stringify: String = "KEEPTTL"
  }

  type KeepTtl = KeepTtl.type
}
