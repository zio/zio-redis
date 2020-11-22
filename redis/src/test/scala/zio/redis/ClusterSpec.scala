package zio.redis

import zio.test._
import zio.test.Assertion._

trait ClusterSpec extends BaseSpec {

  val clusterSuite =
    suite("cluster")(
      suite("nodes, ...")(
        testM("calls CLUSTER NODES") {
          clusterNodes().map(
            assert(_)(isNonEmpty)
          )
        }
      )
    )
}
