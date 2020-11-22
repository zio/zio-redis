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
}
