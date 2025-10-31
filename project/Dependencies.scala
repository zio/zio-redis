import sbt.*

object Dependencies {

  object Versions {
    val Zio               = "2.1.22"
    val CatsEffect        = "3.6.3"
    val EmbeddedRedis     = "0.6"
    val Redis4Cats        = "2.0.1"
    val Sttp              = "3.11.0"
    val TlsChannel        = "0.9.1"
    val ZioConfig         = "4.0.5"
    val ZioJson           = "0.7.44"
    val ZioSchema         = "1.7.5"
    val ZioTestContainers = "0.6.0"
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
      "dev.zio"                       %% "zio-http"            % "3.4.0"
    )

  val docs =
    List(
      "dev.zio" %% "zio-schema-protobuf" % Versions.ZioSchema,
      "dev.zio" %% "zio-test"            % Versions.Zio
    )

  val redis =
    List(
      "com.github.marianobarrios" % "tls-channel"         % Versions.TlsChannel,
      "dev.zio"                  %% "zio-concurrent"      % Versions.Zio,
      "dev.zio"                  %% "zio-schema"          % Versions.ZioSchema,
      "dev.zio"                  %% "zio-schema-protobuf" % Versions.ZioSchema         % Test,
      "dev.zio"                  %% "zio-test"            % Versions.Zio               % Test,
      "dev.zio"                  %% "zio-test-sbt"        % Versions.Zio               % Test,
      "com.github.sideeffffect"  %% "zio-testcontainers"  % Versions.ZioTestContainers % Test
    )
}
