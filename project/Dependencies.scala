import sbt._

object Dependencies {
  private object Versions {
    val CatsEffect        = "3.5.3"
    val EmbeddedRedis     = "0.6"
    val Redis4Cats        = "1.5.2"
    val Sttp              = "3.9.2"
    val TlsChannel        = "0.8.1"
    val ZHttp             = "2.0.0-RC11"
    val ZioConfig         = "3.0.7"
    val ZioJson           = "0.6.2"
    val ZioSchema         = "0.4.16"
    val ZioTestContainers = "0.4.1"
  }

  lazy val Benchmarks =
    List(
      "dev.profunktor" %% "redis4cats-effects"  % Versions.Redis4Cats,
      "dev.zio"        %% "zio-schema-protobuf" % Versions.ZioSchema,
      "org.typelevel"  %% "cats-effect"         % Versions.CatsEffect
    )

  lazy val Embedded =
    List(
      "com.github.kstyrc" % "embedded-redis"      % Versions.EmbeddedRedis,
      "dev.zio"          %% "zio-schema"          % Versions.ZioSchema % Test,
      "dev.zio"          %% "zio-schema-protobuf" % Versions.ZioSchema % Test
    )

  lazy val Example =
    List(
      "com.softwaremill.sttp.client3" %% "zio"                 % Versions.Sttp,
      "com.softwaremill.sttp.client3" %% "zio-json"            % Versions.Sttp,
      "dev.zio"                       %% "zio-config-magnolia" % Versions.ZioConfig,
      "dev.zio"                       %% "zio-config-typesafe" % Versions.ZioConfig,
      "dev.zio"                       %% "zio-schema-protobuf" % Versions.ZioSchema,
      "dev.zio"                       %% "zio-json"            % Versions.ZioJson,
      "io.d11"                        %% "zhttp"               % Versions.ZHttp
    )

  def docs(zioVersion: String) =
    List(
      "dev.zio" %% "zio-schema-protobuf" % Versions.ZioSchema,
      "dev.zio" %% "zio-test"            % zioVersion
    )

  def redis(zioVersion: String) =
    List(
      "com.github.marianobarrios" % "tls-channel"         % Versions.TlsChannel,
      "dev.zio"                  %% "zio-concurrent"      % zioVersion,
      "dev.zio"                  %% "zio-schema"          % Versions.ZioSchema,
      "dev.zio"                  %% "zio-schema-protobuf" % Versions.ZioSchema         % Test,
      "dev.zio"                  %% "zio-test"            % zioVersion                 % Test,
      "dev.zio"                  %% "zio-test-sbt"        % zioVersion                 % Test,
      "com.github.sideeffffect"  %% "zio-testcontainers"  % Versions.ZioTestContainers % Test
    )
}
