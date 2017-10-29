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

val kafkaVersion = "1.0.0"

val commonSettings = Seq(
  name := "example-kafkastreams",
  version := "1.0",
  organization := "http://mkuthan.github.io/",
  scalaVersion := "2.12.4"
)

val customScalacOptions = Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked",
  "-Xfatal-warnings",
  "-Xfuture",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-unused-import"
)

val customResolvers = Seq(
  "Apache Staging" at "https://repository.apache.org/content/groups/staging/"
)

val customLibraryDependencies = Seq(
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.kafka" % "kafka-streams" % kafkaVersion,

  "net.manub" %% "scalatest-embedded-kafka" % "0.16.0",

  "com.twitter" %% "chill" % "0.9.2",

  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
  "org.slf4j" % "slf4j-api" % "1.7.22",
  "log4j" % "log4j" % "1.2.16"
)

val customJavaOptions = Seq(
  "-Xms1024m",
  "-Xmx1024m",
  "-XX:-MaxFDLimit"
)

lazy val root = (project in file("."))
  .settings(commonSettings)
  .settings(scalacOptions ++= customScalacOptions)
  .settings(resolvers ++= customResolvers)
  .settings(libraryDependencies ++= customLibraryDependencies)
  .settings(fork in run := true)
  .settings(connectInput in run := true)
  .settings(javaOptions in run ++= customJavaOptions)
