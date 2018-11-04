version in ThisBuild := "0.1.0-SNAPSHOT"

addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")
addCommandAlias("check", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")

lazy val root =
  (project in file("."))
    .settings(
      name := "scalaz-analytics",
      libraryDependencies +=
        "org.scalaz" %% "scalaz-zio" % "0.3.1"
    )
