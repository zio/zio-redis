package zio.redis.api

import zio.redis.Command
import zio.redis.Input._
import zio.redis.Output._

trait Geo {
  final val geoAdd =
    Command("GEOADD", Tuple2(StringInput, NonEmptyList(Tuple2(LongLatInput, StringInput))), LongOutput)

  final val geoDist =
    Command(
      "GEODIST",
      Tuple4(StringInput, StringInput, StringInput, OptionalInput(RadiusUnitInput)),
      OptionalOutput(DoubleOutput)
    )

  final val geoHash = Command("GEOHASH", Tuple2(StringInput, NonEmptyList(StringInput)), ChunkOutput)
  final val geoPos  = Command("GEOPOS", Tuple2(StringInput, NonEmptyList(StringInput)), ChunkOutput)

  final val geoRadius =
    Command(
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
      ChunkOutput
    )

  final val geoRadiusByMember =
    Command(
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
      ChunkOutput
    )
}
