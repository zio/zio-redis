package zio.redis

trait Type {
  sealed trait Type

  object Type {
    case object String    extends Type
    case object List      extends Type
    case object Set       extends Type
    case object SortedSet extends Type
    case object Hash      extends Type
  }
}
