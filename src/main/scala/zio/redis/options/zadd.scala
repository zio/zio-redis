package zio.redis.options

object zadd {
  sealed trait Updates

  object Updates {
    case object XX extends Updates
    case object NX extends Updates
  }

  case object CH
  case object INCR

  type CH   = CH.type
  type INCR = INCR.type
}
