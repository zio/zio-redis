package example

import zio.{Has, Task}

package object api {
  type Api = Has[Api.Service]
}
