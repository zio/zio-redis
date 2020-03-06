package zio.redis.options

object zadd {
  sealed trait Updates

  object Updates {
    case object XX extends Updates
    case object NX extends Updates
  }

  sealed trait CH
  sealed trait INCR
}
