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
        case Expire.SetExpireSeconds        => "EX"
        case Expire.SetExpireMilliseconds   => "PX"
        case Expire.SetExpireAtSeconds      => "EXAT"
        case Expire.SetExpireAtMilliseconds => "PXAT"
      }
  }
  object Expire {
    case object SetExpireSeconds        extends Expire
    case object SetExpireMilliseconds   extends Expire
    case object SetExpireAtSeconds      extends Expire
    case object SetExpireAtMilliseconds extends Expire
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
