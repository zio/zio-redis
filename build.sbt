import BuildHelper._

Global / onChangedBuildSource := ReloadOnSourceChanges

inThisBuild(
  List(
    organization := "dev.zio",
    homepage     := Some(url("https://github.com/zio/zio-redis/")),
    licenses     := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    developers := List(
      Developer("jdegoes", "John De Goes", "john@degoes.net", url("https://degoes.net")),
      Developer("mijicd", "Dejan Mijic", "dmijic@acm.org", url("https://github.com/mijicd"))
    )
  )
)

addCommandAlias("fmt", "all scalafmtSbt scalafmtAll")
addCommandAlias("fmtCheck", "all scalafmtSbtCheck scalafmtCheckAll")

lazy val root =
  project
    .in(file("."))
    .settings(publish / skip := true)
    .aggregate(redis, benchmarks, example)

lazy val redis =
  project
    .in(file("redis"))
    .enablePlugins(BuildInfoPlugin)
    .settings(stdSettings("zio-redis"))
    .settings(buildInfoSettings("zio.redis"))
    .settings(
      libraryDependencies ++= Seq(
        "dev.zio" %% "zio-streams"  % Zio,
        "dev.zio" %% "zio-logging"  % "0.5.14",
        "dev.zio" %% "zio-schema"   % "0.1.1",
        "dev.zio" %% "zio-test"     % Zio % Test,
        "dev.zio" %% "zio-test-sbt" % Zio % Test
      ),
      testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))
    )

lazy val benchmarks =
  project
    .in(file("benchmarks"))
    .settings(stdSettings("benchmarks"))
    .dependsOn(redis)
    .enablePlugins(JmhPlugin)
    .settings(
      publish / skip := true,
      libraryDependencies ++= Seq(
        "dev.profunktor"    %% "redis4cats-effects" % "1.0.0",
        "io.chrisdavenport" %% "rediculous"         % "0.1.1",
        "io.laserdisc"      %% "laserdisc-fs2"      % "0.5.0"
      )
    )

lazy val example =
  project
    .in(file("example"))
    .settings(stdSettings("example"))
    .dependsOn(redis)
    .settings(
      publish / skip := true,
      libraryDependencies ++= Seq(
        "com.softwaremill.sttp.client3" %% "async-http-client-backend-zio" % "3.3.18",
        "com.softwaremill.sttp.client3" %% "zio-json"                      % "3.3.18",
        "dev.zio"                       %% "zio-streams"                   % Zio,
        "dev.zio"                       %% "zio-config-magnolia"           % "1.0.10",
        "dev.zio"                       %% "zio-config-typesafe"           % "1.0.10",
        "dev.zio"                       %% "zio-prelude"                   % "1.0.0-RC7",
        "dev.zio"                       %% "zio-json"                      % "0.1.5",
        "io.d11"                        %% "zhttp"                         % "1.0.0.0-RC17",
        "io.github.kitlangton"          %% "zio-magic"                     % "0.3.11"
      )
    )
