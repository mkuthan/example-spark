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
    val spark = "1.5.1"
  }

  val projectName = "example-spark"

  val common = Seq(
    version := "1.0",
    organization := "http://mkuthan.github.io/",
    scalaVersion := "2.11.7"
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

  val customResolvers = Seq(
    Classpaths.sbtPluginReleases,
    Classpaths.typesafeReleases,
    "Sonatype OSS Releases" at "http://oss.sonatype.org/content/repositories/releases/",
    "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases"
  )

  val customLibraryDependencies = Seq(
    "org.apache.spark" %% "spark-core" % Versions.spark,
    "org.apache.spark" %% "spark-sql" % Versions.spark,
    "org.apache.spark" %% "spark-streaming" % Versions.spark,

    "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",

    "org.slf4j" % "slf4j-api" % "1.7.10",
    "ch.qos.logback" % "logback-classic" % "1.1.2",

    "org.scalatest" %% "scalatest" % "2.2.4" % "test"
  )

  val customExcludeDependencies = Seq(
    "org.slf4j" % "slf4j-log4j12"
  )

  lazy val main = Project(projectName, base = file("."))
    .settings(common)
    .settings(javaOptions in Runtime ++= customJavaInRuntimeOptions)
    .settings(javaOptions in Test ++= customJavaInTestOptions)
    .settings(scalacOptions ++= customScalacOptions)
    .settings(resolvers ++= customResolvers)
    .settings(libraryDependencies ++= customLibraryDependencies)
    .settings(excludeDependencies ++= customExcludeDependencies)
    .settings(parallelExecution in Test := false)
    .settings(fork in Test := true)

}

