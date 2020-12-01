package example

import zio.Has

package object domain {
  type Contributors = Has[Contributors.Service]
}
