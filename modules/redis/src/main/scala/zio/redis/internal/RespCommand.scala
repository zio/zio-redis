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

package zio.redis.internal

import zio.Chunk

private[redis] final case class RespCommand(name: RespCommandName, args: RespCommandArguments) {
  def bulkStrings: Chunk[RespValue.BulkString] = name.bulkStrings ++ args.bulkStrings
}

private[redis] final case class RespCommandName(str: String) extends AnyVal {
  def bulkStrings: Chunk[RespValue.BulkString] = Chunk.fromArray(str.split(" ").map(RespValue.bulkString))
}

private[redis] final case class RespCommandArguments(values: Chunk[RespCommandArgument]) extends AnyVal {
  def ++(that: RespCommandArguments): RespCommandArguments = RespCommandArguments(this.values ++ that.values)

  def map(f: RespCommandArgument => RespCommandArgument): RespCommandArguments = RespCommandArguments(
    values.map(f(_))
  )

  def bulkStrings: Chunk[RespValue.BulkString] = values.map(_.value)
}

private[redis] object RespCommandArguments {
  def empty: RespCommandArguments = new RespCommandArguments(Chunk.empty)

  def apply(args: Chunk[RespCommandArgument]): RespCommandArguments = new RespCommandArguments(args)

  def apply(args: RespCommandArgument*): RespCommandArguments = new RespCommandArguments(Chunk.fromIterable(args))

  def apply(arg: RespCommandArgument): RespCommandArguments = new RespCommandArguments(Chunk.single(arg))
}
