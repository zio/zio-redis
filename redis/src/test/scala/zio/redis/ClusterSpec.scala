package zio.redis

import zio.redis.executor.cluster.RedisUri
import zio.test.Assertion._
import zio.test._

trait ClusterSpec extends BaseSpec {
  def clusterSpec: Spec[Redis, RedisError] =
    suite("cluster")(
      suite("slots")(
        test("get cluster slots") {
          for {
            res <- slots
          } yield {
            val addresses    = (5000 to 5005).map(port => RedisUri("127.0.0.1", port))
            val resAddresses = res.map(_.master.address) ++ res.flatMap(_.slaves.map(_.address))
            assert(resAddresses.distinct)(hasSameElements(addresses))
          }
        }
      )
    )
}
