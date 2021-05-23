addSbtPlugin("ch.epfl.scala"                     % "sbt-bloop"        % "1.4.8")
addSbtPlugin("ch.epfl.scala"                     % "sbt-scalafix"     % "0.9.27")
addSbtPlugin("com.eed3si9n"                      % "sbt-buildinfo"    % "0.10.0")
addSbtPlugin("com.geirsson"                      % "sbt-ci-release"   % "1.5.7")
addSbtPlugin("com.thoughtworks.sbt-api-mappings" % "sbt-api-mappings" % "3.0.0")
addSbtPlugin("io.github.davidgregory084"         % "sbt-tpolecat"     % "0.1.18")
addSbtPlugin("org.scalameta"                     % "sbt-mdoc"         % "2.2.21")
addSbtPlugin("org.scalameta"                     % "sbt-scalafmt"     % "2.4.2")
addSbtPlugin("pl.project13.scala"                % "sbt-jmh"          % "0.4.2")

libraryDependencies += "org.snakeyaml" % "snakeyaml-engine" % "2.3"
