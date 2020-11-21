package zio.redis

import cats.effect.{Blocker, IO, Resource}
import dev.profunktor.redis4cats.RedisCommands
import fs2.io.tcp.SocketGroup
import io.chrisdavenport.rediculous.{RedisConnection => RediculousClient}
import laserdisc.fs2.{RedisClient => LaserDiskClient}
import zio.ZIO
import zio.logging.Logging

object BenchmarksUtils {

  import BenchmarkRuntime._

  private val redicoulusConnection: Resource[IO, RediculousClient[IO]] =
    for {
      blocker <- Blocker[IO]
      sg <- SocketGroup[IO](blocker)
      c <- RediculousClient.queued[IO](sg, RedisHost, RedisPort, maxQueued = 10000, workers = 2)
    } yield c

  import _root_.laserdisc.auto.autoRefine
  private val laserDiskConnection: Resource[IO, LaserDiskClient[IO]] = LaserDiskClient.to(RedisHost, RedisPort)

  import dev.profunktor.redis4cats.Redis
  import dev.profunktor.redis4cats.effect.Log.NoOp.instance
  private val redis4CatsConnection: Resource[IO, RedisCommands[IO, String, String]] = Redis[IO].utf8(s"redis://$RedisHost:$RedisPort")


  def laserdiscQuery(f: LaserDiskClient[IO] => IO[Unit]): Unit = laserDiskConnection.use(f).unsafeRunSync

  def rediculousQuery(f: RediculousClient[IO] => IO[Unit]): Unit = redicoulusConnection.use(f).unsafeRunSync

  def redis4catsQuery(f: RedisCommands[IO, String, String] => IO[Unit]): Unit = redis4CatsConnection.use(f).unsafeRunSync

  def zioQuery(source: ZIO[RedisExecutor, RedisError, Unit]): Unit =
    unsafeRun(source.provideLayer(Logging.ignore >>> RedisExecutor.live(RedisHost, RedisPort).orDie))
}
