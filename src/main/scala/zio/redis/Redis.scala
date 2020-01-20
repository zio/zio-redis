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
    def swapDb(index1: Int, index2: Int): RIO[R, Unit]

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

    // hashes
    def hDel(key: String, field: String, fields: String*): RIO[R, Long]
    def hExists(key: String, field: String): RIO[R, Boolean]
    def hGet(key: String, field: String): RIO[R, Option[String]]
    def hGetAll(key: String): RIO[R, List[(String, String)]]
    def hIncrBy(key: String, field: String, amount: Long): RIO[R, Long]
    def hIncrByFloat(key: String, field: String, amount: Float): RIO[R, Float]
    def hKeys(key: String): RIO[R, List[String]]
    def hLen(key: String): RIO[R, Long]
    def hMGet(key: String, field: String, fields: String*): RIO[R, List[HashPair]]
    def hSet(key: String, pair: HashPair, pairs: HashPair*): RIO[R, Long]
    def hSetNx(key: String, pair: HashPair): RIO[R, Boolean]
    def hStrLen(key: String, field: String): RIO[R, Long]
    def hVals(key: String): RIO[R, List[String]]
    def hScan(key: String, pattern: Option[String], count: Option[Long], `type`: Option[String]): RIO[R, Chunk[Byte]]

    // TODO: keys
    // TODO: lists
    // TODO: server
    // TODO: sets
    // TODO: sorted sets
    // TODO: strings
  }
}
