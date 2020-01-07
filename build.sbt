// *****************************************************************************
// Projects
// *****************************************************************************

lazy val root =
  project
    .in(file("."))
    .settings(settings)
    .settings(
      libraryDependencies ++= Seq(
        library.zio,
        library.zioNio,
        library.zioTest    % Test,
        library.zioTestSbt % Test
      ),
      publishArtifact := false,
      testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
    )

// *****************************************************************************
// Library dependencies
// *****************************************************************************

lazy val library =
  new {
    object Version {
      val zio    = "1.0.0-RC17"
      val zioNio = "0.4.0"
    }
    val zio        = "dev.zio" %% "zio"          % Version.zio
    val zioNio     = "dev.zio" %% "zio-nio"      % Version.zioNio
    val zioTest    = "dev.zio" %% "zio-test"     % Version.zio
    val zioTestSbt = "dev.zio" %% "zio-test-sbt" % Version.zio
  }

// *****************************************************************************
// Settings
// *****************************************************************************

lazy val settings =
  commonSettings ++
    scalafmtSettings ++
    commandAliases

lazy val commonSettings =
  Seq(
    name := "ZIO Redis",
    scalaVersion := "2.13.1",
    organization := "com.leadiq"
  )

lazy val scalafmtSettings =
  Seq(
    scalafmtOnCompile := true
  )

lazy val commandAliases =
  addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt") ++
    addCommandAlias("check", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")
