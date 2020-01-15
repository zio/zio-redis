package zio.redis

final case class GeoInfo(
  member: String,
  coord: Option[(Longitude, Latitude)] = None,
  dist: Option[Double] = None,
  hash: Option[Int] = None
)
