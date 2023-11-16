package zio.redis
import zio.{IO, UIO}

private[redis] trait GenRedis[G[+_]]
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

private[redis] object GenRedis {
  type Async[+A] = UIO[IO[RedisError, A]]
  type Sync[+A]  = IO[RedisError, A]

  def async[A](io: UIO[IO[RedisError, A]]) = io
  def sync[A](io: UIO[IO[RedisError, A]])  = io.flatten
}
