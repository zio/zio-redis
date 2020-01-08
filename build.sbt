import BuildHelper._

inThisBuild(
  List(
    organization := "dev.zio",
    homepage := Some(url("https://github.com/zio/zio-redis/")),
    licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    developers := List(
      Developer(
        "jdegoes",
        "John De Goes",
        "john@degoes.net",
        url("http://degoes.net")
      )
    ),
    pgpPassphrase := sys.env.get("PGP_PASSWORD").map(_.toArray),
    pgpPublicRing := file("/tmp/public.asc"),
    pgpSecretRing := file("/tmp/secret.asc"),
    scmInfo := Some(
      ScmInfo(url("https://github.com/zio/zio-redis/"), "scm:git:git@github.com:zio/zio-redis.git")
    )
  )
)

ThisBuild / publishTo := sonatypePublishToBundle.value

addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")
addCommandAlias("check", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")

val zioVersion    = "1.0.0-RC17"
val zioNioVersion = "0.4.0"

lazy val redis =
  project.in(file("."))
    .settings(stdSettings("zio-redis"))
    .settings(buildInfoSettings("zio.redis"))
    .settings(
        libraryDependencies ++= Seq(
          "dev.zio"        %% "zio"          % zioVersion,
          "dev.zio"        %% "zio-nio"      % zioNioVersion,
          "dev.zio"        %% "zio-test"     % zioVersion % Test,
          "dev.zio"        %% "zio-test-sbt" % zioVersion % Test
        ),
        testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))
    )
    .enablePlugins(BuildInfoPlugin)
