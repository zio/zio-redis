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

package zio.redis.example

import zio.schema.{DeriveSchema, Schema}

import scala.util.control.NoStackTrace

sealed trait ApiError extends NoStackTrace
object ApiError {
  final case class CacheMiss(key: String)       extends ApiError
  case object CorruptedData                     extends ApiError
  case object GithubUnreachable                 extends ApiError
  final case class UnknownProject(path: String) extends ApiError

  type CorruptedData     = CorruptedData.type
  type GithubUnreachable = GithubUnreachable.type

  implicit val schemaCacheMiss: Schema[CacheMiss]                 = DeriveSchema.gen[CacheMiss]
  implicit val schemaCorruptedData: Schema[CorruptedData]         = DeriveSchema.gen[CorruptedData]
  implicit val schemaGithubUnreachable: Schema[GithubUnreachable] = DeriveSchema.gen[GithubUnreachable]
  implicit val schemaUnknownProject: Schema[UnknownProject]       = DeriveSchema.gen[UnknownProject]
}
