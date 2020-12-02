import zio.Has

package object example {
  type ContributorsCache = Has[ContributorsCache.Service]
}
