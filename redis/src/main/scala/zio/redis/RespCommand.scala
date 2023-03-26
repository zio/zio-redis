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

package zio.redis

import zio.Chunk

final case class RespCommand(args: Chunk[RespArgument]) {
  def ++(that: RespCommand): RespCommand = RespCommand(this.args ++ that.args)

  def mapArguments(f: RespArgument => RespArgument): RespCommand = RespCommand(args.map(f(_)))
}

object RespCommand {

  def empty: RespCommand = new RespCommand(Chunk.empty)

  def apply(args: Chunk[RespArgument]): RespCommand = new RespCommand(args)

  def apply(args: RespArgument*): RespCommand = new RespCommand(Chunk.fromIterable(args))

  def apply(arg: RespArgument): RespCommand = new RespCommand(Chunk.single(arg))
}
