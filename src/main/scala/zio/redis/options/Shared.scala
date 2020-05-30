package zio.redis.options

trait Shared {
  sealed case class Limit(offset: Int, count: Int)

  sealed trait Update

  object Update {
    case object SetExisting extends Update
    case object SetNew      extends Update
  }
}
