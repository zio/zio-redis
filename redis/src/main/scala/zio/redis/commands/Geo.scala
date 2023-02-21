/*
 * Copyright 2021 John A. De Goes and the ZIO contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.redis.commands

import zio.Chunk
import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.schema.Schema

private[redis] trait Geo extends RedisEnvironment {
  import Geo._

  final def _geoAdd[K: Schema, M: Schema]: RedisCommand[(K, ((LongLat, M), List[(LongLat, M)])), Long] = RedisCommand(
    Geo.GeoAdd,
    Tuple2(ArbitraryKeyInput[K](), NonEmptyList(Tuple2(LongLatInput, ArbitraryValueInput[M]()))),
    LongOutput,
    codec,
    executor
  )

  final def _geoDist[K: Schema, M: Schema]: RedisCommand[(K, M, M, Option[RadiusUnit]), Option[Double]] = RedisCommand(
    GeoDist,
    Tuple4(ArbitraryKeyInput[K](), ArbitraryValueInput[M](), ArbitraryValueInput[M](), OptionalInput(RadiusUnitInput)),
    OptionalOutput(DoubleOutput),
    codec,
    executor
  )

  final def _geoHash[K: Schema, M: Schema]: RedisCommand[(K, (M, List[M])), Chunk[Option[String]]] = RedisCommand(
    GeoHash,
    Tuple2(ArbitraryKeyInput[K](), NonEmptyList(ArbitraryValueInput[M]())),
    ChunkOutput(OptionalOutput(MultiStringOutput)),
    codec,
    executor
  )

  final def _geoPos[K: Schema, M: Schema]: RedisCommand[(K, (M, List[M])), Chunk[Option[zio.redis.LongLat]]] =
    RedisCommand(
      GeoPos,
      Tuple2(ArbitraryKeyInput[K](), NonEmptyList(ArbitraryValueInput[M]())),
      GeoOutput,
      codec,
      executor
    )

  final def _geoRadius[K: Schema]: RedisCommand[
    (
      K,
      LongLat,
      Double,
      RadiusUnit,
      Option[WithCoord],
      Option[WithDist],
      Option[WithHash],
      Option[Count],
      Option[Order]
    ),
    Chunk[GeoView]
  ] = RedisCommand(
    GeoRadius,
    Tuple9(
      ArbitraryKeyInput[K](),
      LongLatInput,
      DoubleInput,
      RadiusUnitInput,
      OptionalInput(WithCoordInput),
      OptionalInput(WithDistInput),
      OptionalInput(WithHashInput),
      OptionalInput(CountInput),
      OptionalInput(OrderInput)
    ),
    GeoRadiusOutput,
    codec,
    executor
  )

  final def _geoRadiusStore[K: Schema]: RedisCommand[
    (
      K,
      zio.redis.LongLat,
      Double,
      zio.redis.RadiusUnit,
      Option[WithCoord],
      Option[WithDist],
      Option[WithHash],
      Option[Count],
      Option[Order],
      Option[Store],
      Option[StoreDist]
    ),
    Long
  ] = RedisCommand(
    GeoRadius,
    Tuple11(
      ArbitraryKeyInput[K](),
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
    LongOutput,
    codec,
    executor
  )

  final def _geoRadiusByMember[K: Schema, M: Schema]: RedisCommand[
    (
      K,
      M,
      Double,
      zio.redis.RadiusUnit,
      Option[WithCoord],
      Option[WithDist],
      Option[WithHash],
      Option[zio.redis.Count],
      Option[zio.redis.Order]
    ),
    Chunk[GeoView]
  ] = RedisCommand(
    GeoRadiusByMember,
    Tuple9(
      ArbitraryKeyInput[K](),
      ArbitraryValueInput[M](),
      DoubleInput,
      RadiusUnitInput,
      OptionalInput(WithCoordInput),
      OptionalInput(WithDistInput),
      OptionalInput(WithHashInput),
      OptionalInput(CountInput),
      OptionalInput(OrderInput)
    ),
    GeoRadiusOutput,
    codec,
    executor
  )

  final def _geoRadiusByMemberStore[K: Schema, M: Schema]: RedisCommand[
    (
      K,
      M,
      Double,
      zio.redis.RadiusUnit,
      Option[WithCoord],
      Option[WithDist],
      Option[WithHash],
      Option[zio.redis.Count],
      Option[zio.redis.Order],
      Option[zio.redis.Store],
      Option[zio.redis.StoreDist]
    ),
    Long
  ] = RedisCommand(
    GeoRadiusByMember,
    Tuple11(
      ArbitraryKeyInput[K](),
      ArbitraryValueInput[M](),
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
    LongOutput,
    codec,
    executor
  )
}

private object Geo {
  final val GeoAdd            = "GEOADD"
  final val GeoDist           = "GEODIST"
  final val GeoHash           = "GEOHASH"
  final val GeoPos            = "GEOPOS"
  final val GeoRadius         = "GEORADIUS"
  final val GeoRadiusByMember = "GEORADIUSBYMEMBER"
}
