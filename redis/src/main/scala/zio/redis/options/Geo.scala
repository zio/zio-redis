package zio.redis.options

trait Geo {

  sealed case class Count(count: Long)

  sealed case class LongLat(longitude: Double, latitude: Double)

  sealed trait Order { self =>
    private[redis] final def stringify: String =
      self match {
        case Order.Ascending  => "ASC"
        case Order.Descending => "DESC"
      }
  }

  object Order {
    case object Ascending  extends Order
    case object Descending extends Order
  }

  sealed trait RadiusUnit { self =>
    private[redis] final def stringify: String =
      self match {
        case RadiusUnit.Meters     => "m"
        case RadiusUnit.Kilometers => "km"
        case RadiusUnit.Feet       => "ft"
        case RadiusUnit.Miles      => "mi"
      }
  }

  object RadiusUnit {
    case object Meters     extends RadiusUnit
    case object Kilometers extends RadiusUnit
    case object Feet       extends RadiusUnit
    case object Miles      extends RadiusUnit
  }

  sealed case class Store(key: String)

  sealed case class StoreDist(key: String)

  case object WithCoord {
    private[redis] def stringify: String = "WITHCOORD"
  }

  type WithCoord = WithCoord.type

  case object WithDist {
    private[redis] def stringify: String = "WITHDIST"
  }

  type WithDist = WithDist.type

  case object WithHash {
    private[redis] def stringify: String = "WITHHASH"
  }

  type WithHash = WithHash.type

}
