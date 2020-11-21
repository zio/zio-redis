package zio.redis.api

import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.{ Chunk, ZIO }

trait Geo {
  import Geo._

  /** Adds the specified geospatial `items` (latitude, longitude, name) to the specified `key`. */
  final def geoAdd(
    key: String,
    item: (LongLat, String),
    items: (LongLat, String)*
  ): ZIO[RedisExecutor, RedisError, Long] =
    GeoAdd.run((key, (item, items.toList)))

  /** Return the distance between two members in the geospatial index represented by the sorted set. */
  final def geoDist(
    key: String,
    member1: String,
    member2: String,
    radiusUnit: Option[RadiusUnit] = None
  ): ZIO[RedisExecutor, RedisError, Option[Double]] = GeoDist.run((key, member1, member2, radiusUnit))

  /** Return valid Geohash strings representing the position of one or more elements in a sorted set value representing a geospatial index. */
  final def geoHash(key: String, member: String, members: String*): ZIO[RedisExecutor, RedisError, Chunk[String]] =
    GeoHash.run((key, (member, members.toList)))

  /** Return the positions (longitude, latitude) of all the specified members of the geospatial index represented by the sorted set at `key`. */
  final def geoPos(key: String, member: String, members: String*): ZIO[RedisExecutor, RedisError, Chunk[LongLat]] =
    GeoPos.run((key, (member, members.toList)))

  /** Return geospatial members of a sorted set which are within the area specified with a *center location* and the *maximum distance from the center*. */
  final def geoRadius(
    key: String,
    center: LongLat,
    radius: Double,
    radiusUnit: RadiusUnit,
    withCoord: Option[WithCoord] = None,
    withDist: Option[WithDist] = None,
    withHash: Option[WithHash] = None,
    count: Option[Count] = None,
    order: Option[Order] = None,
    store: Option[Store] = None,
    storeDist: Option[StoreDist] = None
  ): ZIO[RedisExecutor, RedisError, Chunk[GeoView]] =
    GeoRadius.run((key, center, radius, radiusUnit, withCoord, withDist, withHash, count, order, store, storeDist))

  /** Return geospatial members of a sorted set which are within the area specified with an *existing member* in the set and the *maximum distance from the location of that member*. */
  final def geoRadiusByMember(
    key: String,
    member: String,
    radius: Double,
    radiusUnit: RadiusUnit,
    withCoord: Option[WithCoord] = None,
    withDist: Option[WithDist] = None,
    withHash: Option[WithHash] = None,
    count: Option[Count] = None,
    order: Option[Order] = None,
    store: Option[Store] = None,
    storeDist: Option[StoreDist] = None
  ): ZIO[RedisExecutor, RedisError, Chunk[GeoView]] =
    GeoRadiusByMember.run(
      (key, member, radius, radiusUnit, withCoord, withDist, withHash, count, order, store, storeDist)
    )
}

private object Geo {
  final val GeoAdd =
    RedisCommand("GEOADD", Tuple2(StringInput, NonEmptyList(Tuple2(LongLatInput, StringInput))), LongOutput)

  final val GeoDist =
    RedisCommand(
      "GEODIST",
      Tuple4(StringInput, StringInput, StringInput, OptionalInput(RadiusUnitInput)),
      OptionalOutput(DoubleOutput)
    )

  final val GeoHash = RedisCommand("GEOHASH", Tuple2(StringInput, NonEmptyList(StringInput)), ChunkOutput)

  final val GeoPos = RedisCommand("GEOPOS", Tuple2(StringInput, NonEmptyList(StringInput)), GeoOutput)

  final val GeoRadius =
    RedisCommand(
      "GEORADIUS",
      Tuple11(
        StringInput,
        LongLatInput,
        DoubleInput,
        RadiusUnitInput,
        OptionalInput(WithCoordInput),
        OptionalInput(WithDistInput),
        OptionalInput(WithHashInput),
        OptionalInput(CountInput),
        OptionalInput(OrderInput),
        OptionalInput(StoreInput),
        OptionalInput(StoreDistInput)
      ),
      GeoRadiusOutput
    )

  final val GeoRadiusByMember =
    RedisCommand(
      "GEORADIUSBYMEMBER",
      Tuple11(
        StringInput,
        StringInput,
        DoubleInput,
        RadiusUnitInput,
        OptionalInput(WithCoordInput),
        OptionalInput(WithDistInput),
        OptionalInput(WithHashInput),
        OptionalInput(CountInput),
        OptionalInput(OrderInput),
        OptionalInput(StoreInput),
        OptionalInput(StoreDistInput)
      ),
      GeoRadiusOutput
    )
}
