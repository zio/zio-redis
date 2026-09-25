import Dependencies.Versions

import zio.sbt.githubactions.{Condition, Step}

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
    scala212           := "2.12.21",
    scala213           := "2.13.18",
    scala3             := "3.9.0",
    zioVersion         := Versions.Zio,
    crossScalaVersions := List(scala212.value, scala213.value, scala3.value),
    scalaVersion       := scala213.value,
    libraryDependencySchemes ++= Seq("dev.zio" %% "zio-json" % VersionScheme.Always),

    // zio-sbt-ci: generates .github/workflows/{ci,auto-approve,auto-merge}.yml via
    // `sbt ciGenerateGithubWorkflow`; `ciCheckGithubWorkflow` (run as part of `sbt lint`) fails
    // the build if the committed files have drifted from what these settings produce.
    ciEnabledBranches := Seq("main"),
    // The handwritten workflow ran this on every push to main, not just on release - keep that.
    ciUpdateReadmeCondition := Some(Condition.Expression("github.event_name == 'push'")),
    // Matches the handwritten workflow's env block; the default omits SBT_OPTS and the heap sizing.
    ciWorkflowEnv := {
      val opts = "-XX:+PrintCommandLineFlags -Xms6G -Xmx6G"
      Map("JDK_JAVA_OPTIONS" -> opts, "SBT_OPTS" -> opts)
    },
    // The generated release-docs job otherwise has no way to know `npm publish` needs
    // NODE_AUTH_TOKEN in its environment to authenticate against the npm registry. Its steps are
    // nested inside a single StepSequence, so the fix-up has to recurse rather than map the job's
    // top-level steps directly.
    ciPostReleaseJobs := {
      def addNpmToken(step: Step): Step = step match {
        case s: Step.SingleStep if s.name == "Publish Docs to NPM Registry" =>
          s.copy(env = s.env + ("NODE_AUTH_TOKEN" -> "${{ secrets.NPM_TOKEN }}"))
        case s: Step.StepSequence => Step.StepSequence(s.steps.map(addNpmToken))
        case other => other
      }

      ciPostReleaseJobs.value.map { job =>
        if (job.id != "release-docs") job else job.copy(steps = job.steps.map(addNpmToken))
      }
    }
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
    .settings(stdSettings(name = Some("benchmarks"), packageName = Some("zio.redis.benchmarks"), javaPlatform = "17"))
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
    .settings(stdSettings(name = Some("zio-redis"), packageName = Some("zio.redis"), javaPlatform = "17"))
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
    .settings(stdSettings(name = Some("zio-redis-embedded"), packageName = Some("zio.redis.embedded"), javaPlatform = "17"))
    .settings(enableZIO())
    .settings(libraryDependencies ++= Dependencies.Embedded)
    .dependsOn(client)

lazy val example =
  project
    .in(file("modules/example"))
    .dependsOn(client)
    .settings(stdSettings(name = Some("example"), packageName = Some("zio.redis.example"), javaPlatform = "17"))
    .settings(enableZIO(enableStreaming = true))
    .settings(
      publish / skip := true,
      libraryDependencies ++= Dependencies.Example
    )

lazy val integrationTest =
  project
    .in(file("modules/redis-it"))
    .settings(stdSettings(name = Some("zio-redis-it"), javaPlatform = "17"))
    .settings(enableZIO(enableStreaming = true))
    .settings(
      libraryDependencies ++= Dependencies.redis,
      publish / skip := true,
      Test / fork    := false
    )
    .dependsOn(client)
