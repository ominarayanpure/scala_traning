ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "Assessment1"
  )

// Define Spark version that has better compatibility with newer Java versions
val sparkVersion = "3.2.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalatest" %% "scalatest" % "3.2.18" % "test",
  "org.apache.hadoop" % "hadoop-aws" % "3.3.1",
  "com.amazonaws" % "aws-java-sdk-bundle" % "1.11.563",
  "software.amazon.awssdk" % "aws-sdk-java" % "2.17.32",
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.0.1",
  "org.scalatest" %% "scalatest" % "3.2.18" % "test",
  "com.github.jnr" % "jnr-posix" % "3.1.19",
  "joda-time" % "joda-time" % "2.12.7"
)


