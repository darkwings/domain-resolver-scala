ThisBuild / organization := "com.nttdata"
ThisBuild / version      := "0.0.1"
ThisBuild / scalaVersion := "2.13.8"

val scalaTest = "org.scalatest" %% "scalatest" % "3.2.9"
val kafkaClients = "org.apache.kafka" % "kafka-streams" % "3.1.0"
val gson = "com.google.code.gson" % "gson" % "2.8.9"
val http = "com.softwaremill.sttp.client3" %% "core" % "3.4.1"

lazy val hello = (project in file("."))
  .enablePlugins(JavaAppPackaging)
  .settings(
    name := "domain-resolution-scala",
    libraryDependencies ++= Seq(kafkaClients, gson, http),
    libraryDependencies += scalaTest % Test,
  )
