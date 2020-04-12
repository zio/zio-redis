package zio.redis.options

trait Shared {
  sealed trait Updates

  object Updates {
    case object XX extends Updates
    case object NX extends Updates
  }
}
