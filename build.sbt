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
    pgpPassphrase := sys.env.get("PGP_PASSPHRASE").map(_.toArray),
    pgpPublicRing := file("/tmp/public.asc"),
    pgpSecretRing := file("/tmp/secret.asc"),
    scmInfo := Some(
      ScmInfo(url("https://github.com/zio/zio-redis/"), "scm:git:git@github.com:zio/zio-redis.git")
    ),
    semanticdbEnabled := true,
    semanticdbVersion := scalafixSemanticdb.revision,
    scalafixDependencies += "com.nequissimus" %% "sort-imports" % "0.5.5"
  )
)

addCommandAlias("prepare", "fix; fmt")
addCommandAlias("fmt", "all scalafmtSbt scalafmtAll")
addCommandAlias("fmtCheck", "all scalafmtSbtCheck scalafmtCheckAll")
addCommandAlias("fix", "scalafixAll")
addCommandAlias("fixCheck", "scalafixAll --check")
addCommandAlias("testJVM", ";redis/test;benchmarks/test:compile")
addCommandAlias("testJVM211", ";redis/test")

lazy val root =
  project
    .in(file("."))
    .settings(skip in publish := true)
    .aggregate(redis, benchmarks, examples)

lazy val redis =
  project
    .in(file("redis"))
    .enablePlugins(BuildInfoPlugin)
    .settings(stdSettings("zio-redis"))
    .settings(buildInfoSettings("zio.redis"))
    .settings(
      libraryDependencies ++= Seq(
        "dev.zio" %% "zio-streams"  % "1.0.3",
        "dev.zio" %% "zio-logging"  % "0.5.3",
        "dev.zio" %% "zio-test"     % "1.0.3" % Test,
        "dev.zio" %% "zio-test-sbt" % "1.0.3" % Test
      ),
      testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))
    )

lazy val benchmarks =
  project
    .in(file("benchmarks"))
    .dependsOn(redis)
    .enablePlugins(JmhPlugin)
    .settings(
      crossScalaVersions -= Scala211,
      skip in publish := true,
      libraryDependencies ++= Seq(
        "dev.profunktor"    %% "redis4cats-effects" % "0.10.3",
        "io.chrisdavenport" %% "rediculous"         % "0.0.8",
        "io.laserdisc"      %% "laserdisc-fs2"      % "0.4.1"
      ),
      scalacOptions in Compile := Seq("-Xlint:unused")
    )

lazy val examples =
  project
    .in(file("examples"))
    .settings(stdSettings("examples"))
    .dependsOn(redis)
    .settings(
      skip in publish := true,
      libraryDependencies ++= Seq(
        "io.scalac" %% "zio-akka-http-interop" % "0.4.0",
        "com.softwaremill.sttp.client" %% "core" % "2.0.3",
        "com.softwaremill.sttp.client" %% "async-http-client-backend-zio" % "2.0.3",
        "com.softwaremill.sttp.client" %% "circe" % "2.0.3",
        "io.circe" %% "circe-core" % "0.12.3",
        "io.circe" %% "circe-generic" % "0.12.3",
        "de.heikoseeberger" %% "akka-http-circe" % "1.31.0",
        "dev.zio" %% "zio-json" % "0.0.1",
        "dev.zio" %% "zio-config" % "1.0.0-RC29-1",
        "dev.zio" %% "zio-config-magnolia" % "1.0.0-RC29-1",
        "dev.zio" %% "zio-config-typesafe" % "1.0.0-RC29-1"
      ),
      scalacOptions in Compile := Seq("-Xlint:unused")
    )