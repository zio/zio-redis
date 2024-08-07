package zio.redis

import com.dimafeng.testcontainers.DockerComposeContainer
import zio._
import zio.test._

trait ClusterSpec extends IntegrationSpec {
  def clusterSpec: Spec[DockerComposeContainer & Redis, RedisError] =
    suite("cluster")(
      suite("slots")(
        test("get cluster slots") {
          for {
            res      <- ZIO.serviceWithZIO[Redis](_.slots)
            docker   <- ZIO.service[DockerComposeContainer]
            port      = 6379
            expected <-
              ZIO
                .foreach(0 to 5) { n =>
                  ZIO
                    .attempt(docker.getServiceHost(s"cluster-node$n", port))
                    .map(host => RedisUri(host, port))
                }
                .orDie
            actual    = res.map(_.master.address) ++ res.flatMap(_.slaves.map(_.address))
          } yield assertTrue(actual.distinct.size == expected.size)
        }
      )
    )
}
