package zio.redis

import zio.clock.Clock
import zio.logging.Logging
import zio.test._
import Assertion._

object ApiSpec
    extends ClusterSpec
    with KeysSpec
    with ListSpec
    with SetsSpec
    with SortedSetsSpec
    with StringsSpec
    with GeoSpec
    with HyperLogLogSpec
    with HashSpec {

  def spec =
    suite("Redis commands")(
      suite("Cluster")(
        clusterSuite,
        testM("get and set a key") {
          for {
            key <- uuid
            a   <- uuid
            _   <- sAdd(key, a)
            res <- sCard(key)
          } yield assert(res)(equalTo(1L))
        }
      ).provideCustomLayerShared(Logging.console() >>> ClusterExecutor ++ Clock.live),
      suite("Single Node")(
        // keysSuite,
        // listSuite,
        // setsSuite,
        // sortedSetsSuite,
        // stringsSuite,
        // geoSuite,
        // hyperLogLogSuite,
        // hashSuite
      ).provideCustomLayerShared(Logging.ignore >>> Executor ++ Clock.live)
    )

  private val Executor        = RedisExecutor.loopback().orDie
  private val ClusterExecutor = RedisExecutor.localCluster().orDie
}
