package zio.redis

sealed abstract class Sort(val value: String)

object Sort {
  case object Asc  extends Sort("ASC")
  case object Desc extends Sort("DESC")
}
