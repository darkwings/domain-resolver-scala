ThisBuild / organization := "com.nttdata"
ThisBuild / version      := "0.0.1"
ThisBuild / scalaVersion := "2.13.8"

val scalaTest = "org.scalatest" %% "scalatest" % "3.2.9"
val kafkaClients = "org.apache.kafka" % "kafka-streams" % "3.1.0"
val gson = "com.google.code.gson" % "gson" % "2.9.0"
val http = "com.softwaremill.sttp.client3" %% "core" % "3.4.1"
val pureConfig = "com.github.pureconfig" %% "pureconfig" % "0.17.1"
val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4"
val logging = "ch.qos.logback" % "logback-classic" % "1.2.10"

lazy val hello = (project in file("."))
  .enablePlugins(JavaAppPackaging)
  .settings(
    name := "domain-resolution-scala",
    libraryDependencies ++= Seq(kafkaClients, gson, http, pureConfig, scalaLogging, logging),
    libraryDependencies += scalaTest % Test,
  )
