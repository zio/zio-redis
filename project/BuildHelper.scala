import sbt._
import sbt.Keys._
import sbtbuildinfo._
import BuildInfoKeys._

object BuildHelper {
  def buildInfoSettings(packageName: String) =
    Seq(
      buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion, isSnapshot),
      buildInfoPackage := packageName,
      buildInfoObject := "BuildInfo"
    )

  def stdSettings(prjName: String) =
    Seq(
      name := s"$prjName",
      crossScalaVersions := Seq(Scala211, Scala212, Scala213),
      scalaVersion in ThisBuild := Scala212,
      scalacOptions := StdOpts ++ extraOptions(scalaVersion.value),
      libraryDependencies ++=
        Seq(
          "com.github.ghik" % "silencer-lib" % SilencerVersion % Provided cross CrossVersion.full,
          compilerPlugin("com.github.ghik" % "silencer-plugin" % SilencerVersion cross CrossVersion.full)
        ),
      incOptions ~= (_.withLogRecompileOnMacro(false))
    )

  val Scala211        = "2.11.12"
  val Scala212        = "2.12.10"
  val Scala213        = "2.13.1"

  private val SilencerVersion = "1.4.4"

  private val StdOpts =
    Seq(
      "-deprecation",
      "-encoding",
      "UTF-8",
      "-feature",
      "-unchecked",
      "-language:higherKinds",
      "-language:existentials",
      "-explaintypes",
      "-Yrangepos",
      "-Xlint:_,-missing-interpolator,-type-parameter-shadow",
      "-Ywarn-dead-code",
      "-Ywarn-numeric-widen",
      "-Ywarn-value-discard",
      "-Xfatal-warnings"
    )

  private def extraOptions(scalaVersion: String): Seq[String] =
    CrossVersion.partialVersion(scalaVersion) match {
      case Some((2, 13)) =>
        Seq(
          "-Wunused:imports",
          "-Wvalue-discard",
          "-Wunused:patvars",
          "-Wunused:privates",
          "-Wunused:params",
          "-Wvalue-discard"
        )

      case Some((2, 12)) =>
        Seq(
          "-Xfuture",
          "-Ypartial-unification",
          "-Ywarn-nullary-override",
          "-Yno-adapted-args",
          "-Ywarn-infer-any",
          "-Ywarn-inaccessible",
          "-Ywarn-nullary-unit",
          "-Ywarn-unused-import",
          "-Ywarn-extra-implicit",
          "-Ywarn-unused:_,imports",
          "-Ywarn-unused:imports"
        )

      case Some((2, 11)) =>
        Seq(
          "-Xexperimental",
          "-Xfuture",
          "-Ypartial-unification",
          "-Ywarn-nullary-override",
          "-Yno-adapted-args",
          "-Ywarn-infer-any",
          "-Ywarn-inaccessible",
          "-Ywarn-nullary-unit",
          "-Ywarn-unused-import"
        )

      case _             => Seq.empty
    }
}
