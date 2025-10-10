val ZioSbtVersion = "0.4.0-alpha.35"

addSbtPlugin("com.thoughtworks.sbt-api-mappings" % "sbt-api-mappings"  % "3.0.2")
addSbtPlugin("dev.zio"                           % "zio-sbt-website"   % ZioSbtVersion)
addSbtPlugin("dev.zio"                           % "zio-sbt-ecosystem" % ZioSbtVersion)
addSbtPlugin("ch.epfl.scala"                     % "sbt-scalafix"      % "0.14.4")
addSbtPlugin("com.github.sbt"                    % "sbt-ci-release"    % "1.11.2")
addSbtPlugin("com.typesafe"                      % "sbt-mima-plugin"   % "1.1.4")
addSbtPlugin("org.scalameta"                     % "sbt-scalafmt"      % "2.5.5")
addSbtPlugin("pl.project13.scala"                % "sbt-jmh"           % "0.4.7")
