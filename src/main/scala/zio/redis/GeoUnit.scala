package zio.redis

sealed abstract class GeoUnit(val str: String)

object GeoUnit {
  case object Meter     extends GeoUnit("m")
  case object Kilometer extends GeoUnit("km")
  case object Feet      extends GeoUnit("ft")
  case object Mile      extends GeoUnit("mi")
}
