package zio.redis.options

trait Geo {
  this: Shared =>

  sealed case class LongLat(longitude: Double, latitude: Double)

  sealed case class GeoView(member: String, dist: Option[Double], hash: Option[Long], longLat: Option[LongLat])

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

  sealed trait StoreOptions {
    def store: Option[Store]
    def storeDist: Option[StoreDist]
  }
  case class StoreResults(results: Store) extends StoreOptions {
    override def store: Option[Store]         = Some(results)
    override def storeDist: Option[StoreDist] = None
  }
  case class StoreDistances(distances: StoreDist) extends StoreOptions {
    override def store: Option[Store]         = None
    override def storeDist: Option[StoreDist] = Some(distances)
  }
  case class StoreBoth(results: Store, distances: StoreDist) extends StoreOptions {
    override def store: Option[Store]         = Some(results)
    override def storeDist: Option[StoreDist] = Some(distances)
  }

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
