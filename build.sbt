name := "kafka-example"

ThisBuild / scalaVersion := "2.12.10"

val circeVersion = "0.14.1"
val json4sVersion = "4.0.6"
val scoptVersion = "4.1.0"


libraryDependencies ++= Seq(
  "com.github.tototoshi" %% "scala-csv" % "1.3.10",
  "org.apache.commons" % "commons-csv" % "1.9.0",
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  "org.json4s" %% "json4s-jackson" % json4sVersion,
  "org.json4s" %% "json4s-core" % json4sVersion,
  "com.github.scopt" %% "scopt" % "4.1.0",
  "io.scalaland" %% "chimney" % "0.6.2",
  "org.apache.kafka" % "kafka-clients" % "3.3.1",
  "ch.qos.logback" % "logback-classic" % "1.3.2",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5"

)

