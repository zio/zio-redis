package zio.redis
import zio.{IO, UIO}

trait GenRedis[G[+_]]
    extends api.Connection[G]
    with api.Geo[G]
    with api.Hashes[G]
    with api.HyperLogLog[G]
    with api.Keys[G]
    with api.Lists[G]
    with api.Sets[G]
    with api.Strings[G]
    with api.SortedSets[G]
    with api.Streams[G]
    with api.Scripting[G]
    with api.Cluster[G]
    with api.Publishing[G]

object GenRedis {
  type Async[+A] = IO[RedisError, IO[RedisError, A]]
  private[redis] def async[A](io: UIO[IO[RedisError, A]]) = io
  type Sync[+A] = IO[RedisError, A]
  private[redis] def sync[A](io: UIO[IO[RedisError, A]]) = io.flatten
}
