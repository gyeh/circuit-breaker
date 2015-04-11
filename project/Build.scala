import sbt._
import Keys._

object BuildSettings extends Build {
  lazy val root = Project("CircuitBreaker", file(".")) settings(coreSettings : _*)

  lazy val commonSettings: Seq[Setting[_]] = Seq(
    organization := "net.gyeh",
    version := "0.1-SNAPSHOT",
    scalaVersion := "2.11.6",
    scalacOptions := Seq("-deprecation", "-unchecked", "-feature")
  )

  lazy val coreSettings = commonSettings ++ Seq(
    name := "CircuitBreaker",
    parallelExecution in Test := false,
    libraryDependencies :=
      Seq(
        "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0"
      )
  )
}

