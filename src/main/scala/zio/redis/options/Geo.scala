package zio.redis.options

trait Geo {

  sealed case class Count(count: Long)

  sealed case class LongLat(longitude: Double, latitude: Double)

  sealed trait Order

  object Order {
    case object Ascending  extends Order
    case object Descending extends Order
  }

  sealed trait RadiusUnit

  object RadiusUnit {
    case object Meters     extends RadiusUnit
    case object Kilometers extends RadiusUnit
    case object Feet       extends RadiusUnit
    case object Miles      extends RadiusUnit
  }

  sealed case class Store(key: String)

  sealed case class StoreDist(key: String)

  case object WithCoord
  type WithCoord = WithCoord.type

  case object WithDist
  type WithDist = WithDist.type

  case object WithHash
  type WithHash = WithHash.type

}
