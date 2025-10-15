import Dependencies.Versions

Global / onChangedBuildSource := ReloadOnSourceChanges

enablePlugins(ZioSbtEcosystemPlugin)

inThisBuild(
  List(
    name               := "ZIO Redis",
    developers         := List(
      Developer("jdegoes", "John De Goes", "john@degoes.net", url("https://degoes.net")),
      Developer("mijicd", "Dejan Mijic", "dmijic@acm.org", url("https://github.com/mijicd"))
    ),
    startYear          := Some(2021),
    scala212           := "2.12.20",
    scala213           := "2.13.16",
    scala3             := "3.3.7",
    zioVersion         := Versions.Zio,
    crossScalaVersions := List(scala212.value, scala213.value, scala3.value),
    scalaVersion       := scala213.value
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
    .aggregate(
      benchmarks,
      client,
      docs,
      embedded,
      example,
      integrationTest
    )

lazy val benchmarks =
  project
    .in(file("modules/benchmarks"))
    .enablePlugins(JmhPlugin)
    .settings(stdSettings(name = Some("benchmarks"), packageName = Some("zio.redis.benchmarks")))
    .settings(
      crossScalaVersions -= scala3.value,
      libraryDependencies ++= Dependencies.Benchmarks,
      publish / skip := true
    )
    .dependsOn(client)

lazy val client =
  project
    .in(file("modules/redis"))
    .settings(addOptionsOn("2.13")("-Xlint:-infer-any"))
    .settings(stdSettings(name = Some("zio-redis"), packageName = Some("zio.redis")))
    .settings(enableZIO(enableStreaming = true))
    .settings(libraryDependencies ++= Dependencies.redis)

lazy val docs = project
  .in(file("zio-redis-docs"))
  .settings(
    libraryDependencies ++= Dependencies.docs,
    scalacOptions --= List("-Yno-imports", "-Xfatal-warnings"),
    publish / skip := true
  )
  .settings(
    moduleName                                 := "zio-redis-docs",
    projectName                                := (ThisBuild / name).value,
    mainModuleName                             := (client / moduleName).value,
    projectStage                               := ProjectStage.Development,
    ScalaUnidoc / unidoc / unidocProjectFilter := inProjects(client)
  )
  .dependsOn(client, embedded)
  .enablePlugins(WebsitePlugin)

lazy val embedded =
  project
    .in(file("modules/embedded"))
    .settings(stdSettings(name = Some("zio-redis-embedded"), packageName = Some("zio.redis.embedded")))
    .settings(enableZIO())
    .settings(libraryDependencies ++= Dependencies.Embedded)
    .dependsOn(client)

lazy val example =
  project
    .in(file("modules/example"))
    .dependsOn(client)
    .settings(stdSettings(name = Some("example"), packageName = Some("zio.redis.example")))
    .settings(enableZIO(enableStreaming = true))
    .settings(
      publish / skip := true,
      libraryDependencies ++= Dependencies.Example
    )

lazy val integrationTest =
  project
    .in(file("modules/redis-it"))
    .settings(stdSettings(name = Some("zio-redis-it")))
    .settings(enableZIO(enableStreaming = true))
    .settings(
      libraryDependencies ++= Dependencies.redis,
      publish / skip := true,
      Test / fork    := false
    )
    .dependsOn(client)
