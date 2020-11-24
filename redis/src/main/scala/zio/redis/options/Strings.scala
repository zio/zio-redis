package zio.redis.options

trait Strings {

  sealed trait BitFieldCommand

  object BitFieldCommand {
    sealed case class BitFieldGet(`type`: BitFieldType, offset: Int) extends BitFieldCommand

    sealed case class BitFieldSet(`type`: BitFieldType, offset: Int, value: Long) extends BitFieldCommand

    sealed case class BitFieldIncr(`type`: BitFieldType, offset: Int, increment: Long) extends BitFieldCommand

    sealed trait BitFieldOverflow extends BitFieldCommand { self =>
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
        case BitOperation.NOT => "NOT"
      }
  }

  object BitOperation {
    case object AND extends BitOperation
    case object OR  extends BitOperation
    case object XOR extends BitOperation
    case object NOT extends BitOperation
  }

  sealed case class BitPosRange(start: Long, end: Option[Long])

  case object KeepTtl {
    private[redis] def stringify: String = "KEEPTTL"
  }

  type KeepTtl = KeepTtl.type

  sealed trait StrAlgoCommand

  object StrAlgoCommand {
    case class LongestCommonSubsequence(
      lcsType: LcsType,
      input1: String,
      input2: String,
      len: Option[QueryType] = None,
      minMatchLen: Option[MinMatchLen] = None,
      withMatchLen: Option[WithMatchLen] = None
    ) extends StrAlgoCommand

    sealed trait LcsType { self =>
      private[redis] final def stringify: String =
        self match {
          case LcsType.Keys    => "KEYS"
          case LcsType.Strings => "STRINGS"
        }
    }

    object LcsType {
      case object Keys    extends LcsType
      case object Strings extends LcsType
    }

    sealed trait QueryType { self =>
      private[redis] final def stringify: String =
        self match {
          case QueryType.Len => "LEN"
          case QueryType.Idx => "IDX"
        }
    }

    object QueryType {
      case object Len extends QueryType
      case object Idx extends QueryType
    }

    sealed case class MinMatchLen(length: Integer)

    case object WithMatchLen {
      private[redis] def stringify: String = "WITHMATCHLEN"
    }

    type WithMatchLen = WithMatchLen.type
  }

  case class LongestCommonSubsequenceResult(lcs: Option[String], length: Option[Long], matches: Option[String])
}
