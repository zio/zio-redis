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

package zio

package object redis
    extends api.Connection
    with api.Geo
    with api.Hashes
    with api.HyperLogLog
    with api.Keys
    with api.Lists
    with api.Sets
    with api.Strings
    with api.SortedSets
    with api.Streams
    with api.Scripting
    with api.Cluster
    with options.Connection
    with options.Geo
    with options.Keys
    with options.Shared
    with options.SortedSets
    with options.Strings
    with options.Lists
    with options.Streams
    with options.Scripting
    with options.PubSub {

  type Id[+A] = A

  private[redis] def logScopeFinalizer(msg: String): URIO[Scope, Unit] =
    for {
      scope <- ZIO.scope
      _ <- scope.addFinalizerExit {
             case Exit.Success(_)  => ZIO.logTrace(s"$msg with success")
             case Exit.Failure(th) => ZIO.logTraceCause(s"$msg with failure", th)
           }
    } yield ()
}
