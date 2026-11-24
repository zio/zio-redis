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

package zio.redis.options

/**
 * Types for the geospatial commands, such as `GEOADD` and `GEOSEARCH`.
 */
trait Geo { this: Shared =>
  /**
   * A position given as longitude and latitude in degrees. The class does not check the range. Redis rejects
   * a pair outside longitude -180 to 180 or latitude -85.05112878 to 85.05112878 with the error
   * `ERR invalid longitude,latitude pair`.
   */
  sealed case class LongLat(longitude: Double, latitude: Double)

  /**
   * A member returned by a geospatial search. `dist`, `hash` and `longLat` hold the values requested with
   * `WithDist`, `WithHash` and `WithCoord`, and are `None` when the option was not requested.
   */
  sealed case class GeoView(member: String, dist: Option[Double], hash: Option[Long], longLat: Option[LongLat])

  /**
   * The unit of a search radius, as used by `GEOSEARCH` and `GEOSEARCHSTORE`.
   */
  sealed trait RadiusUnit { self =>
    private[redis] final def asString: String =
      self match {
        case RadiusUnit.Meters     => "m"
        case RadiusUnit.Kilometers => "km"
        case RadiusUnit.Feet       => "ft"
        case RadiusUnit.Miles      => "mi"
      }
  }

  /**
   * The units that a search radius can be given in.
   */
  object RadiusUnit {
    /**
     * Meters. Sent as `m`.
     */
    case object Meters     extends RadiusUnit
    /**
     * Kilometers. Sent as `km`.
     */
    case object Kilometers extends RadiusUnit
    /**
     * Feet. Sent as `ft`.
     */
    case object Feet       extends RadiusUnit
    /**
     * Miles. Sent as `mi`.
     */
    case object Miles      extends RadiusUnit
  }

  /**
   * Where a geospatial search stores its output: the matched members, their distances, or both. The
   * subtypes describe which forms Redis 7.2 accepts.
   */
  sealed trait StoreOptions {
    /**
     * The destination key for the matched members, or `None` when no members are stored.
     */
    def store: Option[Store]
    /**
     * The destination key for the distances, or `None` when no distances are stored.
     */
    def storeDist: Option[StoreDist]
  }

  /**
   * Stores the matched members in the sorted set `results`. This form works on Redis 7.2. The `geoRadiusStore`
   * method returns the number of members stored.
   *
   * {{{
   * import zio._
   * import zio.redis._
   *
   * // Stores the members within 200 km of (15, 37) in the sorted set "nearby"
   * val stored: ZIO[Redis, RedisError, Long] =
   *   ZIO.serviceWithZIO[Redis](
   *     _.geoRadiusStore("cities", LongLat(15.0, 37.0), 200.0, RadiusUnit.Kilometers, StoreResults(Store("nearby")))
   *   )
   * }}}
   *
   * @see [[https://zio.dev/zio-redis/ ZIO Redis]]
   */
  case class StoreResults(results: Store) extends StoreOptions {
    /**
     * The destination key for the matched members, `results`.
     */
    def store: Option[Store]         = Some(results)
    /**
     * Always `None`: distances are not stored.
     */
    def storeDist: Option[StoreDist] = None
  }

  /**
   * Stores only the distances, in the sorted set `distances`. Redis 7.2 rejects this form with
   * `ERR syntax error`, because no destination key is sent for the members. See `StoreDist`.
   */
  case class StoreDistances(distances: StoreDist) extends StoreOptions {
    /**
     * Always `None`: no destination key is sent for the members.
     */
    def store: Option[Store]         = None
    /**
     * The destination for the distances, `distances`.
     */
    def storeDist: Option[StoreDist] = Some(distances)
  }

  /**
   * Stores the matched members in `results` and their distances in `distances`. Redis 7.2 rejects this form
   * with `ERR syntax error`, because the distance key follows `STOREDIST`. See `StoreDist`.
   */
  case class StoreBoth(results: Store, distances: StoreDist) extends StoreOptions {
    /**
     * The destination key for the matched members, `results`.
     */
    def store: Option[Store]         = Some(results)
    /**
     * The destination key for the distances, `distances`.
     */
    def storeDist: Option[StoreDist] = Some(distances)
  }

  /**
   * A destination key for the distances. It is sent as `STOREDIST` followed by the key. In `GEOSEARCHSTORE`,
   * which `geoRadiusStore` and `geoRadiusByMemberStore` use, Redis 7.2 takes `STOREDIST` as a flag with no
   * key, so every search that sends this value fails with `ERR syntax error`.
   */
  sealed case class StoreDist(key: String)

  /**
   * The `WITHCOORD` option. It returns the longitude and latitude of each matched member.
   */
  case object WithCoord {
    private[redis] def asString: String = "WITHCOORD"
  }

  /**
   * The type of the `WithCoord` option.
   */
  type WithCoord = WithCoord.type

  /**
   * The `WITHDIST` option. It returns the distance of each matched member from the search center.
   */
  case object WithDist {
    private[redis] def asString: String = "WITHDIST"
  }

  /**
   * The type of the `WithDist` option.
   */
  type WithDist = WithDist.type

  /**
   * The `WITHHASH` option. It returns the geohash of each matched member as an integer.
   */
  case object WithHash {
    private[redis] def asString: String = "WITHHASH"
  }

  /**
   * The type of the `WithHash` option.
   */
  type WithHash = WithHash.type

}
