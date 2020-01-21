package zio.redis

import zio._
import zio.stream.ZStreamChunk
import zio.stream.ZStream

trait Redis {
  def redis: Redis.Service[Any]
}

object Redis {
  trait Service[-R] {
    // connection
    def auth(password: String): ZIO[R, RedisError, Unit]
    def echo: ZIO[R, RedisError, Unit]
    def ping: ZIO[R, RedisError, Unit]
    def quit: ZIO[R, RedisError, Unit]
    def select(index: Int): ZIO[R, RedisError, Unit]
    def swapDb(index1: Int, index2: Int): ZIO[R, RedisError, Unit]

    // geo
    def geoAdd(key: String, member: GeoMemberInfo, members: GeoMemberInfo*): ZIO[R, RedisError, Long]
    def geoDist(key: String, member1: String, member2: String, unit: GeoUnit = GeoUnit.Meter): ZIO[R, RedisError, Double]
    def geoHash(key: String, keys: String*): ZIO[R, RedisError, List[String]]
    def geoPos(key: String, member: String, members: String*): ZIO[R, RedisError, List[GeoMemberInfo]]

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
    ): ZIO[R, RedisError, List[GeoInfo]]

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
    ): ZIO[R, RedisError, List[GeoInfo]]

    // hashes
    def hDel(key: String, field: String, fields: String*): ZIO[R, RedisError, Long]
    def hExists(key: String, field: String): ZIO[R, RedisError, Boolean]
    def hGet(key: String, field: String): ZIO[R, RedisError, Option[String]]
    def hGetAll(key: String): ZStream[R, RedisError, (String, Chunk[Byte])]
    def hIncrBy(key: String, field: String, amount: Long): ZIO[R, RedisError, Long]
    def hIncrByFloat(key: String, field: String, amount: Float): ZIO[R, RedisError, Float]
    def hKeys(key: String): ZStream[R, RedisError, String]
    def hLen(key: String): ZIO[R, RedisError, Long]
    def hMGet(key: String, field: String, fields: String*): ZStream[R, RedisError, HashPair]
    def hSet(key: String, pair: HashPair, pairs: HashPair*): ZIO[R, RedisError, Long]
    def hSetNx(key: String, pair: HashPair): ZIO[R, RedisError, Boolean]
    def hStrLen(key: String, field: String): ZIO[R, RedisError, Long]
    def hVals(key: String): ZStreamChunk[R, RedisError, Byte]
    def hScan(key: String, pattern: Option[String], count: Option[Long], `type`: Option[String]): ZStreamChunk[R, RedisError, Byte]

    // TODO: keys
    // TODO: lists
    // TODO: server
    // TODO: sets
    // TODO: sorted sets
    // TODO: strings
  }
}
