package zio.redis.api

import zio.redis.Command
import zio.redis.Input._
import zio.redis.Output.{ DoubleOutput, LongOutput, OptionalOutput, StreamOutput, StringOutput }

trait Geo {
  lazy val geoAdd =
    Command(
      "GEOADD",
      Tuple2(
        StringInput,
        NonEmptyList(Tuple2(GeoLongLatInput, StringInput))
      ),
      LongOutput
    )

  lazy val geoDist =
    Command(
      "GEODIST",
      Tuple4(
        StringInput,
        StringInput,
        StringInput,
        OptionalInput(GeoRadiusUnitInput)
      ),
      OptionalOutput(DoubleOutput)
    )

  lazy val geoHash =
    Command(
      "GEOHASH",
      Tuple2(StringInput, NonEmptyList(StringInput)),
      StreamOutput
    )

  lazy val geoPos =
    Command(
      "GEOPOS",
      Tuple2(
        StringInput,
        NonEmptyList(StringInput)
      ),
      StreamOutput
    )

  lazy val geoRadius =
    Command(
      "GEORADIUS",
      Tuple11(
        StringInput,
        GeoLongLatInput,
        DoubleInput,
        GeoRadiusUnitInput,
        OptionalInput(GeoWithCoordInput),
        OptionalInput(GeoWithDistInput),
        OptionalInput(GeoWithHashInput),
        OptionalInput(GeoCountInput),
        OptionalInput(GeoOrderInput),
        OptionalInput(GeoStoreInput),
        OptionalInput(GeoStoreDistInput)
      ),
      StreamOutput
    )

  lazy val geoRadiusByMember =
    Command(
      "GEORADIUSBYMEMBER",
      Tuple11(
        StringInput,
        StringInput,
        DoubleInput,
        GeoRadiusUnitInput,
        OptionalInput(GeoWithCoordInput),
        OptionalInput(GeoWithDistInput),
        OptionalInput(GeoWithHashInput),
        OptionalInput(GeoCountInput),
        OptionalInput(GeoOrderInput),
        OptionalInput(GeoStoreInput),
        OptionalInput(GeoStoreDistInput)
      ),
      StreamOutput
    )
}
