package zio.redis.options

trait Shared {

  sealed trait Update { self =>
    private[redis] final def stringify: String =
      self match {
        case Update.SetExisting    => "XX"
        case Update.SetNew         => "NX"
        case Update.SetLessThan    => "LT"
        case Update.SetGreaterThan => "GT"
      }
  }

  object Update {
    case object SetExisting    extends Update
    case object SetNew         extends Update
    case object SetLessThan    extends Update
    case object SetGreaterThan extends Update
  }

  sealed trait Expire { self =>
    private[redis] final def stringify: String =
      self match {
        case Expire.SetExpireSeconds      => "EX"
        case Expire.SetExpireMilliseconds => "PX"
      }
  }
  object Expire {
    case object SetExpireSeconds      extends Expire
    case object SetExpireMilliseconds extends Expire
  }

  sealed trait ExpiredAt { self =>
    private[redis] final def stringify: String =
      self match {
        case ExpiredAt.SetExpireAtSeconds      => "EXAT"
        case ExpiredAt.SetExpireAtMilliseconds => "PXAT"
      }
  }
  object ExpiredAt {
    case object SetExpireAtSeconds      extends ExpiredAt
    case object SetExpireAtMilliseconds extends ExpiredAt
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
