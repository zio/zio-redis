package zio.redis.options

trait Shared {
  sealed trait Update { self =>
    private[redis] final def stringify: String =
      self match {
        case Update.SetExisting => "XX"
        case Update.SetNew      => "NX"
      }
  }

  object Update {
    case object SetExisting extends Update
    case object SetNew      extends Update
  }

  sealed case class Count(count: Long)

  sealed trait Order { self =>
    private[redis] final def stringify: String =
      self match {
        case Order.Ascending  => "ASC"
        case Order.Descending => "DESC"
      }
  }

  object Order {
    case object Ascending  extends Order
    case object Descending extends Order
  }

  sealed case class Limit(offset: Long, count: Long)

  sealed case class Store(key: String)

  sealed case class Pattern(pattern: String)
}
