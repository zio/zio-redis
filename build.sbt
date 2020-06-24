import BuildHelper._

inThisBuild(
  List(
    organization := "dev.zio",
    homepage := Some(url("https://github.com/zio/zio-redis/")),
    licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    developers := List(
      Developer("jdegoes", "John De Goes", "john@degoes.net", url("https://degoes.net")),
      Developer("mijicd", "Dejan Mijic", "dmijic@acm.org", url("https://github.com/mijicd"))
    ),
    pgpPassphrase := sys.env.get("PGP_PASSWORD").map(_.toArray),
    pgpPublicRing := file("/tmp/public.asc"),
    pgpSecretRing := file("/tmp/secret.asc"),
    scmInfo := Some(
      ScmInfo(url("https://github.com/zio/zio-redis/"), "scm:git:git@github.com:zio/zio-redis.git")
    )
  )
)

addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")
addCommandAlias("check", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")

lazy val root =
  project
    .in(file("."))
    .settings(
      skip in publish := true
    )
    .aggregate(redis)

lazy val redis =
  project
    .in(file("redis"))
    .settings(stdSettings("zio-redis"))
    .settings(buildInfoSettings("zio.redis"))
    .settings(
      libraryDependencies ++= Seq(
        "dev.zio" %% "zio"          % "1.0.0-RC21",
        "dev.zio" %% "zio-test"     % "1.0.0-RC21" % Test,
        "dev.zio" %% "zio-test-sbt" % "1.0.0-RC21" % Test
      ),
      testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))
    )
    .enablePlugins(BuildInfoPlugin)
