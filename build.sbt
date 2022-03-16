import BuildHelper._

Global / onChangedBuildSource := ReloadOnSourceChanges

inThisBuild(
  List(
    developers := List(
      Developer("jdegoes", "John De Goes", "john@degoes.net", url("https://degoes.net")),
      Developer("mijicd", "Dejan Mijic", "dmijic@acm.org", url("https://github.com/mijicd"))
    ),
    homepage         := Some(url("https://github.com/zio/zio-redis/")),
    licenses         := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    organization     := "dev.zio",
    organizationName := "John A. De Goes and the ZIO contributors",
    startYear        := Some(2021)
  )
)

addCommandAlias("check", "fixCheck; fmtCheck")
addCommandAlias("fix", "scalafixAll")
addCommandAlias("fixCheck", "scalafixAll --check")
addCommandAlias("fmt", "all scalafmtSbt scalafmtAll")
addCommandAlias("fmtCheck", "all scalafmtSbtCheck scalafmtCheckAll")
addCommandAlias("prepare", "fix; fmt")

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
        "dev.zio" %% "zio-streams"         % Zio,
        "dev.zio" %% "zio-logging"         % "0.5.14",
        "dev.zio" %% "zio-schema"          % "0.1.8",
        "dev.zio" %% "zio-schema-protobuf" % "0.1.8" % Test,
        "dev.zio" %% "zio-test"            % Zio     % Test,
        "dev.zio" %% "zio-test-sbt"        % Zio     % Test
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
        "dev.profunktor"    %% "redis4cats-effects"  % "1.1.1",
        "io.chrisdavenport" %% "rediculous"          % "0.2.0",
        "io.laserdisc"      %% "laserdisc-fs2"       % "0.5.0",
        "dev.zio"           %% "zio-schema-protobuf" % "0.1.8"
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
        "com.softwaremill.sttp.client3" %% "async-http-client-backend-zio1" % "3.5.1",
        "com.softwaremill.sttp.client3" %% "zio1-json"                      % "3.5.1",
        "dev.zio"                       %% "zio-streams"                    % Zio,
        "dev.zio"                       %% "zio-config-magnolia"            % "2.0.3",
        "dev.zio"                       %% "zio-config-typesafe"            % "2.0.3",
        "dev.zio"                       %% "zio-schema-protobuf"            % "0.1.8",
        "dev.zio"                       %% "zio-json"                       % "0.1.5",
        "io.d11"                        %% "zhttp"                          % "1.0.0.0-RC25",
        "io.github.kitlangton"          %% "zio-magic"                      % "0.3.11"
      )
    )

lazy val docs = project
  .in(file("zio-redis-docs"))
  .settings(
    publish / skip := true,
    moduleName     := "zio-redis-docs",
    scalacOptions -= "-Yno-imports",
    scalacOptions -= "-Xfatal-warnings",
    ScalaUnidoc / unidoc / unidocProjectFilter := inProjects(redis),
    ScalaUnidoc / unidoc / target              := (LocalRootProject / baseDirectory).value / "website" / "static" / "api",
    cleanFiles += (ScalaUnidoc / unidoc / target).value,
    docusaurusCreateSite     := docusaurusCreateSite.dependsOn(Compile / unidoc).value,
    docusaurusPublishGhpages := docusaurusPublishGhpages.dependsOn(Compile / unidoc).value
  )
  .settings(macroDefinitionSettings)
  .dependsOn(redis)
  .enablePlugins(MdocPlugin, DocusaurusPlugin, ScalaUnidocPlugin)
