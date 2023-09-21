import zio.sbt.githubactions.Step.SingleStep

enablePlugins(ZioSbtEcosystemPlugin, ZioSbtCiPlugin)

inThisBuild(
  List(
    name              := "ZIO Redis",
    zioVersion        := "2.0.16",
    scala212          := "2.12.18",
    scala213          := "2.13.12",
    scala3            := "3.3.1",
    ciEnabledBranches := List("master"),
    ciExtraTestSteps  := List(
      SingleStep(
        name = "Run Redis",
        run = Some("docker-compose -f docker/redis-compose.yml up -d")
      ),
      SingleStep(
        name = "Run Redis cluster",
        run = Some("docker-compose -f docker/redis-cluster-compose.yml up -d")
      ),
      SingleStep(
        name = "Run integration tests",
        run = Some("sbt ++${{ matrix.scala }} IntegrationTest/test")
      )
    ),
    developers        := List(
      Developer("jdegoes", "John De Goes", "john@degoes.net", url("https://degoes.net")),
      Developer("mijicd", "Dejan Mijic", "dmijic@acm.org", url("https://github.com/mijicd"))
    ),
    startYear         := Some(2021)
  )
)

lazy val root =
  project
    .in(file("."))
    .settings(
      name               := "zio-redis",
      crossScalaVersions := Nil,
      publish / skip     := true
    )
    .aggregate(redis, embedded, benchmarks, example, docs)

lazy val redis =
  project
    .in(file("modules/redis"))
    .settings(addOptionsOn("2.13")("-Xlint:-infer-any"))
    .settings(stdSettings(name = Some("zio-redis"), packageName = Some("zio.redis")))
    .settings(enableZIO(enableStreaming = true))
    .settings(libraryDependencies ++= Dependencies.redis(zioVersion.value))
    .settings(Defaults.itSettings)
    .configs(IntegrationTest)

lazy val embedded =
  project
    .in(file("modules/embedded"))
    .settings(stdSettings(name = Some("zio-redis-embedded"), packageName = Some("zio.redis.embedded")))
    .settings(enableZIO())
    .settings(libraryDependencies ++= Dependencies.Embedded)
    .dependsOn(redis)

lazy val benchmarks =
  project
    .in(file("modules/benchmarks"))
    .enablePlugins(JmhPlugin)
    .dependsOn(redis)
    .settings(stdSettings(name = Some("benchmarks"), packageName = Some("zio.redis.benchmarks")))
    .settings(
      crossScalaVersions -= scala3.value,
      libraryDependencies ++= Dependencies.Benchmarks,
      publish / skip := true
    )

lazy val example =
  project
    .in(file("modules/example"))
    .dependsOn(redis)
    .settings(stdSettings(name = Some("example"), packageName = Some("zio.redis.example")))
    .settings(enableZIO(enableStreaming = true))
    .settings(
      publish / skip := true,
      libraryDependencies ++= Dependencies.Example
    )

lazy val docs = project
  .in(file("zio-redis-docs"))
  .settings(
    libraryDependencies ++= Dependencies.docs(zioVersion.value),
    scalacOptions --= List("-Yno-imports", "-Xfatal-warnings"),
    publish / skip := true
  )
  .settings(
    moduleName                                 := "zio-redis-docs",
    projectName                                := (ThisBuild / name).value,
    mainModuleName                             := (redis / moduleName).value,
    projectStage                               := ProjectStage.Development,
    ScalaUnidoc / unidoc / unidocProjectFilter := inProjects(redis)
  )
  .dependsOn(redis, embedded)
  .enablePlugins(WebsitePlugin)
