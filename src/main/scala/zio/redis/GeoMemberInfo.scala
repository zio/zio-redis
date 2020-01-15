package zio.redis

final case class GeoMemberInfo(longitude: Longitude, latitude: Latitude, member: String)
