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

package zio.redis.api

import zio.IO
import zio.redis._
import zio.schema.Schema

trait HyperLogLog extends commands.HyperLogLog {

  /**
   * Adds the specified elements to the specified HyperLogLog.
   *
   * @param key
   *   HLL key where the elements will be added
   * @param element
   *   element to count
   * @param elements
   *   additional elements to count
   * @return
   *   boolean indicating if at least 1 HyperLogLog register was altered.
   */
  final def pfAdd[K: Schema, V: Schema](key: K, element: V, elements: V*): IO[RedisError, Boolean] =
    _pfAdd[K, V].run((key, (element, elements.toList)))

  /**
   * Return the approximated cardinality of the set(s) observed by the HyperLogLog at key(s).
   *
   * @param key
   *   HLL key
   * @param keys
   *   additional keys
   * @return
   *   approximate number of unique elements observed via PFADD.
   */
  final def pfCount[K: Schema](key: K, keys: K*): IO[RedisError, Long] = _pfCount[K].run((key, keys.toList))

  /**
   * Merge N different HyperLogLogs into a single one.
   *
   * @param destKey
   *   HLL key where the merged HLLs will be stored
   * @param sourceKey
   *   HLL key to merge
   * @param sourceKeys
   *   additional keys to merge
   */
  final def pfMerge[K: Schema](destKey: K, sourceKey: K, sourceKeys: K*): IO[RedisError, Unit] =
    _pfMerge[K].run((destKey, (sourceKey, sourceKeys.toList)))
}
