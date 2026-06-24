val ZioSbtVersion = "0.6.0"

addSbtPlugin("com.thoughtworks.sbt-api-mappings" % "sbt-api-mappings"  % "3.0.3")
addSbtPlugin("dev.zio"                           % "zio-sbt-website"   % ZioSbtVersion)
addSbtPlugin("dev.zio"                           % "zio-sbt-ecosystem" % ZioSbtVersion)
addSbtPlugin("ch.epfl.scala"                     % "sbt-scalafix"      % "0.14.7")
addSbtPlugin("com.github.sbt"                    % "sbt-ci-release"    % "1.11.2")
addSbtPlugin("com.typesafe"                      % "sbt-mima-plugin"   % "1.1.5")
addSbtPlugin("org.scalameta"                     % "sbt-scalafmt"      % "2.6.1")
addSbtPlugin("pl.project13.scala"                % "sbt-jmh"           % "0.4.8")
