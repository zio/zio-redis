package zio.redis.options

trait Shared {
  sealed trait Update

  object Update {
    case object SetExisting extends Update
    case object SetNew      extends Update
  }
}
