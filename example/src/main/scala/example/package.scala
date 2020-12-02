import zio.Has

package object example {
  type Contributors = Has[Contributors.Service]
}
