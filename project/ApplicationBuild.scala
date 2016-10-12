// Copyright (C) 2011-2012 the original author or authors.
// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import sbt.Keys._
import sbt._

object ApplicationBuild extends Build {

  object Versions {
    val spark = "2.0.1"
  }

  val projectName = "example-spark"

  val common = Seq(
    version := "1.0",
    organization := "http://mkuthan.github.io/",
    scalaVersion := "2.11.8"
  )

  val customScalacOptions = Seq(
    "-deprecation",
    "-encoding", "UTF-8",
    "-feature",
    "-language:existentials",
    "-language:higherKinds",
    "-language:implicitConversions",
    "-unchecked",
    "-Xfatal-warnings",
    "-Xfuture",
    "-Xlint",
    "-Yno-adapted-args",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    "-Ywarn-unused-import"
  )

  val customJavaInRuntimeOptions = Seq(
    "-Xmx512m"
  )

  val customJavaInTestOptions = Seq(
    "-Xmx512m"
  )

  val customLibraryDependencies = Seq(
    "org.apache.spark" %% "spark-core" % Versions.spark,// % "provided",
    "org.apache.spark" %% "spark-sql" % Versions.spark,// % "provided",
    "org.apache.spark" %% "spark-hive" % Versions.spark,// % "provided",
    "org.apache.spark" %% "spark-streaming" % Versions.spark,// % "provided",

    "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",

    "org.slf4j" % "slf4j-api" % "1.7.21",

    "org.slf4j" % "slf4j-log4j12" % "1.7.21"
      exclude("log4j", "log4j"),

    "log4j" % "log4j" % "1.2.17" % "provided",

    "org.scalatest" %% "scalatest" % "2.2.4" % "test"
  )

  lazy val testScalastyle = taskKey[Unit]("testScalastyle")
  lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")

  lazy val main = Project(projectName, base = file("."))
    .settings(common)
    .settings(javaOptions in Runtime ++= customJavaInRuntimeOptions)
    .settings(javaOptions in Test ++= customJavaInTestOptions)
    .settings(scalacOptions ++= customScalacOptions)
    .settings(libraryDependencies ++= customLibraryDependencies)
    // spark does not support parallel tests and requires JVM fork
    .settings(parallelExecution in Test := false)
    .settings(fork in Test := true)
    // add provided dependencies to the classpath for run and run-main tasks
    .settings(run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run)))
    .settings(runMain in Compile <<= Defaults.runMainTask(fullClasspath in Compile, runner in(Compile, run)))
    // enable scalastyle checks during compilation
    .settings(compileScalastyle := org.scalastyle.sbt.ScalastylePlugin.scalastyle.in(Compile).toTask("").value)
    .settings(compile in Compile <<= (compile in Compile) dependsOn compileScalastyle)
    // enable scalastyle checks during tests compilation
    .settings(testScalastyle := org.scalastyle.sbt.ScalastylePlugin.scalastyle.in(Test).toTask("").value)
    .settings(test in Test <<= (test in Test) dependsOn testScalastyle)
    // do not execute tests during assembly
    .settings(test in sbtassembly.AssemblyKeys.assembly := {})
}

