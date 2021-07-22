addSbtPlugin("ch.epfl.scala"                     % "sbt-bloop"        % "1.4.8")
addSbtPlugin("ch.epfl.scala"                     % "sbt-scalafix"     % "0.9.29")
addSbtPlugin("com.eed3si9n"                      % "sbt-buildinfo"    % "0.10.0")
addSbtPlugin("com.geirsson"                      % "sbt-ci-release"   % "1.5.7")
addSbtPlugin("com.thoughtworks.sbt-api-mappings" % "sbt-api-mappings" % "3.0.0")
addSbtPlugin("org.scalameta"                     % "sbt-mdoc"         % "2.2.22")
addSbtPlugin("org.scalameta"                     % "sbt-scalafmt"     % "2.4.3")
addSbtPlugin("pl.project13.scala"                % "sbt-jmh"          % "0.4.3")

libraryDependencies += "org.snakeyaml" % "snakeyaml-engine" % "2.3"
