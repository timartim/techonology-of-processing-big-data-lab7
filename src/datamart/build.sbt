ThisBuild / scalaVersion := "2.13.14"
ThisBuild / organization := "ru.technology-of-processing-big-data"

lazy val root = (project in file("."))
  .settings(
    name := "openfoodfacts-datamart",
    Compile / scalaSource := baseDirectory.value / "main" / "scala",
    libraryDependencies ++= Seq(
      "org.mongodb.scala" %% "mongo-scala-driver" % "5.2.0",
      "io.circe" %% "circe-core" % "0.14.9",
      "io.circe" %% "circe-parser" % "0.14.9"
    ),
    Compile / mainClass := Some("ru.technology-of-processing-big-data.datamart.DataMartApp")
  )
