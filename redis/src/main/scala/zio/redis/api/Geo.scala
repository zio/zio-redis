package zio.redis.api

import zio.redis.ConnectionType._
import zio.redis.Input._
import zio.redis.Output._
import zio.redis.RedisCommand

trait Geo {
  final val geoAdd =
    RedisCommand("GEOADD", Tuple2(StringInput, NonEmptyList(Tuple2(LongLatInput, StringInput))), LongOutput, Base)

  final val geoDist =
    RedisCommand(
      "GEODIST",
      Tuple4(StringInput, StringInput, StringInput, OptionalInput(RadiusUnitInput)),
      OptionalOutput(DoubleOutput),
      Base
    )

  final val geoHash = RedisCommand("GEOHASH", Tuple2(StringInput, NonEmptyList(StringInput)), ChunkOutput, Base)
  final val geoPos  = RedisCommand("GEOPOS", Tuple2(StringInput, NonEmptyList(StringInput)), GeoOutput, Base)

  final val geoRadius =
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
      GeoRadiusOutput,
      Base
    )

  final val geoRadiusByMember =
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
      GeoRadiusOutput,
      Base
    )
}
