package zio.redis.options

trait Lists {
  sealed trait Position { self =>
    private[redis] final def stringify: String =
      self match {
        case Position.Before => "BEFORE"
        case Position.After  => "AFTER"
      }
  }

  object Position {
    final case object Before extends Position
    final case object After  extends Position
  }
}
