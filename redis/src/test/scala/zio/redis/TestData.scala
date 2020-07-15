package zio.redis

object TestData {

  val serbia = new {
    val key            = "Serbia"
    val member1        = "Novi Sad"
    val member1LongLat = LongLat(19.833548963069916, 45.26713527162855)
    val member2        = "Belgrade"
    val member2LongLat = LongLat(20.457275211811066, 44.787195958992356)
  }

  val sicily = new {
    val key             = "Sicily"
    val member1         = "Palermo"
    val member1Distance = 190.4424
    val member1Hash     = 3479099956230698L
    val member1LongLat  = LongLat(13.361389338970184, 38.1155563954963)
    val member2         = "Catania"
    val member2Distance = 56.4413
    val member2Hash     = 3479447370796909L
    val member2LongLat  = LongLat(15.087267458438873, 37.50266842333162)
  }
}
