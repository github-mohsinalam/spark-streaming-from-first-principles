// build.sbt

ThisBuild / scalaVersion := "2.13.18"
ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.spark-streaming-fp"

val sparkVersion = "4.1.2"
val deltaVersion = "4.3.1"
val scalaTestVersion = "3.2.20"

lazy val root = (project in file("."))
  .settings(
    name := "spark-streaming-from-first-principles",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql"              % sparkVersion,
      "org.apache.spark" %% "spark-sql-kafka-0-10"   % sparkVersion,
      "io.delta"         %% "delta-spark"            % deltaVersion,
      "org.scalatest"    %% "scalatest"              % scalaTestVersion % Test
    ),

    // Spark 4 on JDK 17+ needs these to access internal JDK packages.
    // Without them: java.lang.reflect.InaccessibleObjectException at runtime.
    Compile / run / javaOptions ++= Seq(
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
    ),

    // `sbt run` should fork the JVM so the javaOptions above actually apply.
    // Without fork := true, javaOptions are silently ignored for `run`.
    Compile / run / fork := true
  )