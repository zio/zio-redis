package zio.redis

import zio._

trait Redis {
  def redis: Redis.Service[Any]
}

object Redis {
  trait Service[R] {
    // connection
    def auth(password: String): RIO[R, Unit]
    def echo: RIO[R, Unit]
    def ping: RIO[R, Unit]
    def quit: RIO[R, Unit]
    def select(index: Int): RIO[R, Unit]
    def swapdb(index1: Int, index2: Int): RIO[R, Unit]

    // geo
    def geoAdd(key: String, member: GeoMemberInfo, members: GeoMemberInfo*): RIO[R, Long]
    def geoDist(key: String, member1: String, member2: String, unit: GeoUnit = GeoUnit.Meter): RIO[R, Double]
    def geoHash(key: String, keys: String*): RIO[R, List[String]]
    def geoPos(key: String, member: String, members: String*): RIO[R, List[GeoMemberInfo]]

    def geoRadius(
      key: String,
      longitude: Longitude,
      latitude: Latitude,
      radius: GeoRadius,
      unit: GeoUnit,
      withCoord: Boolean = false,
      withDist: Boolean = false,
      withHash: Boolean = false,
      count: Option[Int] = None,
      sort: Option[Sort] = None,
      store: Option[String] = None,
      storeDist: Option[String] = None
    ): RIO[R, List[GeoInfo]]

    def geoRadiusByMember(
      key: String,
      member: String,
      radius: GeoRadius,
      unit: GeoUnit,
      withCoord: Boolean = false,
      withDist: Boolean = false,
      withHash: Boolean = false,
      count: Option[Int] = None,
      sort: Option[Sort] = None,
      store: Option[String] = None,
      storeDist: Option[String] = None
    ): RIO[R, List[GeoInfo]]

    // TODO: hashes
    // TODO: keys
    // TODO: lists
    // TODO: server
    // TODO: sets
    // TODO: sorted sets
    // TODO: strings
  }
}
