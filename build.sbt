import BuildHelper._

Global / onChangedBuildSource := ReloadOnSourceChanges

inThisBuild(
  List(
    developers := List(
      Developer("jdegoes", "John De Goes", "john@degoes.net", url("https://degoes.net")),
      Developer("mijicd", "Dejan Mijic", "dmijic@acm.org", url("https://github.com/mijicd"))
    ),
    homepage         := Some(url("https://zio.dev/zio-redis/")),
    licenses         := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    organization     := "dev.zio",
    organizationName := "John A. De Goes and the ZIO contributors",
    startYear        := Some(2021)
  )
)

ThisBuild / libraryDependencySchemes ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
)

addCommandAlias("compileBenchmarks", "benchmarks/Jmh/compile")
addCommandAlias("compileSources", "example/Test/compile; redis/Test/compile")
addCommandAlias("check", "fixCheck; fmtCheck; headerCheck")
addCommandAlias("fix", "scalafixAll")
addCommandAlias("fixCheck", "scalafixAll --check")
addCommandAlias("fmt", "all scalafmtSbt scalafmtAll")
addCommandAlias("fmtCheck", "all scalafmtSbtCheck scalafmtCheckAll")
addCommandAlias("prepare", "fix; fmt; headerCreate")

lazy val root =
  project
    .in(file("."))
    .settings(publish / skip := true)
    .aggregate(redis, embedded, benchmarks, example, docs)

lazy val redis =
  project
    .in(file("redis"))
    .enablePlugins(BuildInfoPlugin)
    .settings(buildInfoSettings("zio.redis"))
    .settings(scala3Settings)
    .settings(stdSettings("zio-redis"))
    .settings(
      libraryDependencies ++= List(
        "dev.zio"                %% "zio-streams"             % "2.0.10",
        "dev.zio"                %% "zio-logging"             % "2.1.11",
        "dev.zio"                %% "zio-schema"              % "0.3.1",
        "dev.zio"                %% "zio-schema-protobuf"     % "0.3.1"  % Test,
        "dev.zio"                %% "zio-test"                % "2.0.10" % Test,
        "dev.zio"                %% "zio-test-sbt"            % "2.0.10" % Test,
        "org.scala-lang.modules" %% "scala-collection-compat" % "2.9.0"
      ),
      testFrameworks := List(new TestFramework("zio.test.sbt.ZTestFramework"))
    )

lazy val embedded =
  project
    .in(file("embedded"))
    .enablePlugins(BuildInfoPlugin)
    .settings(buildInfoSettings("zio.redis.embedded"))
    .settings(scala3Settings)
    .settings(stdSettings("zio-redis-embedded"))
    .settings(
      libraryDependencies ++= List(
        "dev.zio"          %% "zio"                 % "2.0.8",
        "com.github.kstyrc" % "embedded-redis"      % "0.6",
        "dev.zio"          %% "zio-schema"          % "0.3.1" % Test,
        "dev.zio"          %% "zio-schema-protobuf" % "0.3.1" % Test,
        "dev.zio"          %% "zio-test"            % "2.0.8" % Test,
        "dev.zio"          %% "zio-test-sbt"        % "2.0.8" % Test
      ),
      testFrameworks := List(new TestFramework("zio.test.sbt.ZTestFramework"))
    )
    .dependsOn(redis)

lazy val benchmarks =
  project
    .in(file("benchmarks"))
    .enablePlugins(JmhPlugin)
    .dependsOn(redis)
    .settings(stdSettings("benchmarks"))
    .settings(
      crossScalaVersions -= Scala3,
      publish / skip := true,
      libraryDependencies ++= List(
        "dev.profunktor"    %% "redis4cats-effects"  % "1.4.0",
        "io.chrisdavenport" %% "rediculous"          % "0.4.0",
        "io.laserdisc"      %% "laserdisc-fs2"       % "0.6.0",
        "dev.zio"           %% "zio-schema-protobuf" % "0.3.1"
      )
    )

lazy val example =
  project
    .in(file("example"))
    .dependsOn(redis)
    .settings(stdSettings("example"))
    .settings(
      publish / skip := true,
      libraryDependencies ++= List(
        "com.softwaremill.sttp.client3" %% "zio"                 % "3.8.13",
        "com.softwaremill.sttp.client3" %% "zio-json"            % "3.8.13",
        "dev.zio"                       %% "zio-streams"         % "2.0.10",
        "dev.zio"                       %% "zio-config-magnolia" % "3.0.7",
        "dev.zio"                       %% "zio-config-typesafe" % "3.0.7",
        "dev.zio"                       %% "zio-schema-protobuf" % "0.3.1",
        "dev.zio"                       %% "zio-json"            % "0.4.2",
        "io.d11"                        %% "zhttp"               % "2.0.0-RC11"
      )
    )

lazy val docs = project
  .in(file("zio-redis-docs"))
  .settings(
    publish / skip := true,
    moduleName     := "zio-redis-docs",
    scalacOptions -= "-Yno-imports",
    scalacOptions -= "-Xfatal-warnings",
    projectName                                := "ZIO Redis",
    mainModuleName                             := (redis / moduleName).value,
    projectStage                               := ProjectStage.Development,
    ScalaUnidoc / unidoc / unidocProjectFilter := inProjects(redis),
    docsPublishBranch                          := "master",
    libraryDependencies += "dev.zio"           %% "zio-schema-protobuf" % "0.3.1"
  )
  .dependsOn(redis)
  .enablePlugins(WebsitePlugin)
