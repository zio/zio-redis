package github.contributors

import sttp.client.{NothingT, SttpBackend}
import zio.{Has, Task}

package object api {
  type Api = Has[Api.Service]
  type SttpClient = Has[SttpBackend[Task, Nothing, NothingT]]
}
