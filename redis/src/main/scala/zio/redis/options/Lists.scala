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
    case object Before extends Position
    case object After  extends Position
  }

  sealed trait Side { self =>
    private[redis] final def stringify: String =
      self match {
        case Side.Left  => "LEFT"
        case Side.Right => "RIGHT"
      }
  }

  object Side {
    case object Left  extends Side
    case object Right extends Side
  }
}
