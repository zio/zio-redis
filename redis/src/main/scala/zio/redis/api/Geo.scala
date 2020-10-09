package zio.redis.api

import zio.{ Chunk, ZIO }
import zio.redis.Input._
import zio.redis.Output._
import zio.redis._

trait Geo {
  final val geoAdd =
    new RedisCommand("GEOADD", Tuple2(StringInput, NonEmptyList(Tuple2(LongLatInput, StringInput))), LongOutput) {
      self =>
      def apply(a: String, b: (LongLat, String), bs: (LongLat, String)*): ZIO[RedisExecutor, RedisError, Long] =
        self.run((a, (b, bs.toList)))
    }

  final val geoDist =
    new RedisCommand(
      "GEODIST",
      Tuple4(StringInput, StringInput, StringInput, OptionalInput(RadiusUnitInput)),
      OptionalOutput(DoubleOutput)
    ) { self =>
      def apply(
        a: String,
        b: String,
        c: String,
        d: Option[RadiusUnit] = None
      ): ZIO[RedisExecutor, RedisError, Option[Double]] = self.run((a, b, c, d))
    }

  final val geoHash =
    new RedisCommand("GEOHASH", Tuple2(StringInput, NonEmptyList(StringInput)), ChunkOutput) { self =>
      def apply(a: String, b: String, bs: String*): ZIO[RedisExecutor, RedisError, Chunk[String]] = self.run((a, (b, bs.toList)))
    }

  final val geoPos =
    new RedisCommand("GEOPOS", Tuple2(StringInput, NonEmptyList(StringInput)), GeoOutput) { self =>
      def apply(a: String, b: String, bs: String*): ZIO[RedisExecutor, RedisError, Chunk[LongLat]] = self.run((a, (b, bs.toList)))
    }

  final val geoRadius =
    new RedisCommand(
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
    ) { self =>
      def apply(
        a: String,
        b: LongLat,
        c: Double,
        d: RadiusUnit,
        e: Option[WithCoord] = None,
        f: Option[WithDist] = None,
        g: Option[WithHash] = None,
        h: Option[Count] = None,
        i: Option[Order] = None,
        j: Option[Store] = None,
        k: Option[StoreDist] = None
      ): ZIO[RedisExecutor, RedisError, Chunk[GeoView]] = self.run((a, b, c, d, e, f, g, h, i, j, k))
    }

  final val geoRadiusByMember =
    new RedisCommand(
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
    ) { self =>
      def apply(
        a: String,
        b: String,
        c: Double,
        d: RadiusUnit,
        e: Option[WithCoord] = None,
        f: Option[WithDist] = None,
        g: Option[WithHash] = None,
        h: Option[Count] = None,
        i: Option[Order] = None,
        j: Option[Store] = None,
        k: Option[StoreDist] = None
      ): ZIO[RedisExecutor, RedisError, Chunk[GeoView]] = self.run((a, b, c, d, e, f, g, h, i, j, k))

    }
}
