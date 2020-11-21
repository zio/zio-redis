package zio.redis

import zio.test._

trait ClusterSpec extends BaseSpec {

  val clusterSuite =
    suite("cluster")(
      suite("info, ...")(
        testM("retrieve cluster info") {
          info() map (_ => assertCompletes)
        }
      )
    )
}
