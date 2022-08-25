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
        "dev.zio"                %% "zio-streams"             % Zio,
        "dev.zio"                %% "zio-logging"             % "2.1.0",
        "dev.zio"                %% "zio-schema"              % "0.2.1",
        "dev.zio"                %% "zio-schema-protobuf"     % "0.2.1" % Test,
        "dev.zio"                %% "zio-test"                % Zio     % Test,
        "dev.zio"                %% "zio-test-sbt"            % Zio     % Test,
        "org.scala-lang.modules" %% "scala-collection-compat" % "2.8.1"
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
        "dev.profunktor"    %% "redis4cats-effects"  % "1.2.0",
        "io.chrisdavenport" %% "rediculous"          % "0.4.0",
        "io.laserdisc"      %% "laserdisc-fs2"       % "0.5.0",
        "dev.zio"           %% "zio-schema-protobuf" % "0.2.1"
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
        "com.softwaremill.sttp.client3" %% "async-http-client-backend-zio" % "3.7.4",
        "com.softwaremill.sttp.client3" %% "zio-json"                      % "3.7.4",
        "dev.zio"                       %% "zio-streams"                   % Zio,
        "dev.zio"                       %% "zio-config-magnolia"           % "3.0.2",
        "dev.zio"                       %% "zio-config-typesafe"           % "3.0.2",
        "dev.zio"                       %% "zio-schema-protobuf"           % "0.2.1",
        "dev.zio"                       %% "zio-json"                      % "0.3.0-RC11",
        "io.d11"                        %% "zhttp"                         % "2.0.0-RC10"
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
