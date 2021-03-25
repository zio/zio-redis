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

  sealed case class ListMaxLen(count: Long)

  sealed case class Rank(rank: Long)
}
