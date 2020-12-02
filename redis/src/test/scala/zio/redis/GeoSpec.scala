package zio.redis

import zio.Chunk
import zio.test.Assertion._
import zio.test._

trait GeoSpec extends BaseSpec {

  val geoSuite: Spec[RedisExecutor, TestFailure[RedisError], TestSuccess] =
    suite("geo")(
      testM("geoAdd followed by geoPos") {
        import GeoSpec.Serbia._
        val nonExistentMember = "Tokyo"
        for {
          _         <- geoAdd(key, member1LongLat -> member1, member2LongLat -> member2)
          locations <- geoPos(key, member1, nonExistentMember, member2)
        } yield assert(locations)(hasSameElements(Chunk(Some(member1LongLat), None, Some(member2LongLat))))
      },
      testM("calculate distance between geospatial items") {
        val key     = "key"
        val member1 = "point1"
        val member2 = "point2"
        val longLat = LongLat(100d, 50d)
        for {
          _        <- geoAdd(key, longLat -> member1, longLat -> member2)
          distance <- geoDist(key, member1, member2, None)
        } yield assert(distance)(isSome(equalTo(0d)))
      },
      testM("get geoHash") {
        import GeoSpec.Sicily._
        val nonExistentMember = "Tokyo"
        for {
          _      <- geoAdd(key, member1LongLat -> member1)
          _      <- geoAdd(key, member2LongLat -> member2)
          result <- geoHash(key, member1, nonExistentMember, member2)
        } yield assert(result)(hasSameElements(Chunk(Some(member1GeoHash), None, Some(member2GeoHash))))
      },
      suite("geoRadius")(
        testM("without details") {
          import GeoSpec.Sicily._
          for {
            _        <- geoAdd(key, member1LongLat -> member1, member2LongLat -> member2)
            response <- geoRadius(key, LongLat(15d, 37d), 200d, RadiusUnit.Kilometers)
          } yield assert(response)(
            hasSameElements(Chunk(GeoView(member1, None, None, None), GeoView(member2, None, None, None)))
          )
        },
        testM("with coordinates") {
          import GeoSpec.Sicily._
          for {
            _        <- geoAdd(key, member1LongLat -> member1, member2LongLat -> member2)
            response <- geoRadius(key, LongLat(15d, 37d), 200d, RadiusUnit.Kilometers, Some(WithCoord))
          } yield assert(response)(
            hasSameElements(
              Chunk(
                GeoView(member1, None, None, Some(member1LongLat)),
                GeoView(member2, None, None, Some(member2LongLat))
              )
            )
          )
        },
        testM("with coordinates and distance") {
          import GeoSpec.Sicily._
          for {
            _        <- geoAdd(key, member1LongLat -> member1, member2LongLat -> member2)
            response <- geoRadius(key, LongLat(15d, 37d), 200d, RadiusUnit.Kilometers, Some(WithCoord), Some(WithDist))
          } yield assert(response)(
            hasSameElements(
              Chunk(
                GeoView(member1, Some(member1Distance), None, Some(member1LongLat)),
                GeoView(member2, Some(member2Distance), None, Some(member2LongLat))
              )
            )
          )
        },
        testM("with coordinates, distance and hash") {
          import GeoSpec.Sicily._
          for {
            _ <- geoAdd(key, member1LongLat -> member1, member2LongLat -> member2)
            response <- geoRadius(
                          key,
                          LongLat(15d, 37d),
                          200d,
                          RadiusUnit.Kilometers,
                          Some(WithCoord),
                          Some(WithDist),
                          Some(WithHash)
                        )
          } yield assert(response)(
            hasSameElements(
              Chunk(
                GeoView(member1, Some(member1Distance), Some(member1Hash), Some(member1LongLat)),
                GeoView(member2, Some(member2Distance), Some(member2Hash), Some(member2LongLat))
              )
            )
          )
        },
        testM("with hash") {
          import GeoSpec.Sicily._
          for {
            _        <- geoAdd(key, member1LongLat -> member1, member2LongLat -> member2)
            response <- geoRadius(key, LongLat(15d, 37d), 200d, RadiusUnit.Kilometers, withHash = Some(WithHash))
          } yield assert(response)(
            hasSameElements(
              Chunk(
                GeoView(member1, None, Some(member1Hash), None),
                GeoView(member2, None, Some(member2Hash), None)
              )
            )
          )
        },
        testM("with distance and hash") {
          import GeoSpec.Sicily._
          for {
            _ <- geoAdd(key, member1LongLat -> member1, member2LongLat -> member2)
            response <- geoRadius(
                          key,
                          LongLat(15d, 37d),
                          200d,
                          RadiusUnit.Kilometers,
                          withDist = Some(WithDist),
                          withHash = Some(WithHash)
                        )
          } yield assert(response)(
            hasSameElements(
              Chunk(
                GeoView(member1, Some(member1Distance), Some(member1Hash), None),
                GeoView(member2, Some(member2Distance), Some(member2Hash), None)
              )
            )
          )
        },
        testM("with distance") {
          import GeoSpec.Sicily._
          for {
            _        <- geoAdd(key, member1LongLat -> member1, member2LongLat -> member2)
            response <- geoRadius(key, LongLat(15d, 37d), 200d, RadiusUnit.Kilometers, withDist = Some(WithDist))
          } yield assert(response)(
            hasSameElements(
              Chunk(
                GeoView(member1, Some(member1Distance), None, None),
                GeoView(member2, Some(member2Distance), None, None)
              )
            )
          )
        },
        testM("with coordinates and hash") {
          import GeoSpec.Sicily._
          for {
            _ <- geoAdd(key, member1LongLat -> member1, member2LongLat -> member2)
            response <-
              geoRadius(key, LongLat(15d, 37d), 200d, RadiusUnit.Kilometers, Some(WithCoord), withHash = Some(WithHash))
          } yield assert(response)(
            hasSameElements(
              Chunk(
                GeoView(member1, None, Some(member1Hash), Some(member1LongLat)),
                GeoView(member2, None, Some(member2Hash), Some(member2LongLat))
              )
            )
          )
        }
      ),
      suite("geoRadiusByMember")(
        testM("without details") {
          import GeoSpec.Sicily._
          for {
            _        <- geoAdd(key, member1LongLat -> member1, member2LongLat -> member2)
            response <- geoRadiusByMember(key, member1, 200d, RadiusUnit.Kilometers)
          } yield assert(response)(
            hasSameElements(Chunk(GeoView(member1, None, None, None), GeoView(member2, None, None, None)))
          )
        },
        testM("with coordinates") {
          import GeoSpec.Sicily._
          for {
            _        <- geoAdd(key, member1LongLat -> member1, member2LongLat -> member2)
            response <- geoRadiusByMember(key, member1, 200d, RadiusUnit.Kilometers, Some(WithCoord))
          } yield assert(response)(
            hasSameElements(
              Chunk(
                GeoView(member1, None, None, Some(member1LongLat)),
                GeoView(member2, None, None, Some(member2LongLat))
              )
            )
          )
        },
        testM("with coordinates and distance") {
          import GeoSpec.Sicily._
          val member1Distance = 0d
          val member2Distance = 166.2742
          for {
            _        <- geoAdd(key, member1LongLat -> member1, member2LongLat -> member2)
            response <- geoRadiusByMember(key, member1, 200d, RadiusUnit.Kilometers, Some(WithCoord), Some(WithDist))
          } yield assert(response)(
            hasSameElements(
              Chunk(
                GeoView(member1, Some(member1Distance), None, Some(member1LongLat)),
                GeoView(member2, Some(member2Distance), None, Some(member2LongLat))
              )
            )
          )
        },
        testM("with coordinates, distance and hash") {
          import GeoSpec.Sicily._
          val member1Distance = 0d
          val member2Distance = 166.2742
          for {
            _ <- geoAdd(key, member1LongLat -> member1, member2LongLat -> member2)
            response <- geoRadiusByMember(
                          key,
                          member1,
                          200d,
                          RadiusUnit.Kilometers,
                          Some(WithCoord),
                          Some(WithDist),
                          Some(WithHash)
                        )
          } yield assert(response)(
            hasSameElements(
              Chunk(
                GeoView(member1, Some(member1Distance), Some(member1Hash), Some(member1LongLat)),
                GeoView(member2, Some(member2Distance), Some(member2Hash), Some(member2LongLat))
              )
            )
          )
        },
        testM("with hash") {
          import GeoSpec.Sicily._
          for {
            _        <- geoAdd(key, member1LongLat -> member1, member2LongLat -> member2)
            response <- geoRadiusByMember(key, member1, 200d, RadiusUnit.Kilometers, withHash = Some(WithHash))
          } yield assert(response)(
            hasSameElements(
              Chunk(
                GeoView(member1, None, Some(member1Hash), None),
                GeoView(member2, None, Some(member2Hash), None)
              )
            )
          )
        },
        testM("with distance and hash") {
          import GeoSpec.Sicily._
          val member1Distance = 0d
          val member2Distance = 166.2742
          for {
            _ <- geoAdd(key, member1LongLat -> member1, member2LongLat -> member2)
            response <- geoRadiusByMember(
                          key,
                          member1,
                          200d,
                          RadiusUnit.Kilometers,
                          withDist = Some(WithDist),
                          withHash = Some(WithHash)
                        )
          } yield assert(response)(
            hasSameElements(
              Chunk(
                GeoView(member1, Some(member1Distance), Some(member1Hash), None),
                GeoView(member2, Some(member2Distance), Some(member2Hash), None)
              )
            )
          )
        },
        testM("with distance") {
          import GeoSpec.Sicily._
          val member1Distance = 0d
          val member2Distance = 166.2742
          for {
            _        <- geoAdd(key, member1LongLat -> member1, member2LongLat -> member2)
            response <- geoRadiusByMember(key, member1, 200d, RadiusUnit.Kilometers, withDist = Some(WithDist))
          } yield assert(response)(
            hasSameElements(
              Chunk(
                GeoView(member1, Some(member1Distance), None, None),
                GeoView(member2, Some(member2Distance), None, None)
              )
            )
          )
        },
        testM("with coordinates and hash") {
          import GeoSpec.Sicily._
          for {
            _ <- geoAdd(key, member1LongLat -> member1, member2LongLat -> member2)
            response <-
              geoRadiusByMember(key, member1, 200d, RadiusUnit.Kilometers, Some(WithCoord), withHash = Some(WithHash))
          } yield assert(response)(
            hasSameElements(
              Chunk(
                GeoView(member1, None, Some(member1Hash), Some(member1LongLat)),
                GeoView(member2, None, Some(member2Hash), Some(member2LongLat))
              )
            )
          )
        },
        testM("with a non-existent member") {
          import GeoSpec.Sicily._
          val nonExistentMember = "Tokyo"
          for {
            _        <- geoAdd(key, member1LongLat -> member1, member2LongLat -> member2)
            response <- geoRadiusByMember(key, nonExistentMember, 200d, RadiusUnit.Kilometers).either
          } yield assert(response)(isLeft)
        }
      )
    )
}

object GeoSpec {
  object Serbia {
    val key                     = "Serbia"
    val member1                 = "Novi Sad"
    val member1LongLat: LongLat = LongLat(19.833548963069916, 45.26713527162855)
    val member2                 = "Belgrade"
    val member2LongLat: LongLat = LongLat(20.457275211811066, 44.787195958992356)
  }

  object Sicily {
    val key                     = "Sicily"
    val member1                 = "Palermo"
    val member1Distance         = 190.4424
    val member1Hash             = 3479099956230698L
    val member1GeoHash          = "sqc8b49rny0"
    val member1LongLat: LongLat = LongLat(13.361389338970184, 38.1155563954963)
    val member2                 = "Catania"
    val member2Distance         = 56.4413
    val member2Hash             = 3479447370796909L
    val member2GeoHash          = "sqdtr74hyu0"
    val member2LongLat: LongLat = LongLat(15.087267458438873, 37.50266842333162)
  }
}
