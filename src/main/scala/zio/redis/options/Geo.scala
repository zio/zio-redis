package zio.redis.options

trait Geo {

  sealed case class Count(count: Long)

  sealed case class LongLat(longitude: Double, latitude: Double)

  sealed trait Order

  object Order {
    case object ASC  extends Order
    case object DESC extends Order
  }

  sealed trait RadiusUnit

  object RadiusUnit {
    case object m  extends RadiusUnit
    case object km extends RadiusUnit
    case object ft extends RadiusUnit
    case object mi extends RadiusUnit
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
