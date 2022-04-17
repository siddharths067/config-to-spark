name := "config-to-spark"

version := "0.1"

scalaVersion := "2.12.15"

libraryDependencies += "io.circe" %% "circe-yaml" % "0.14.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1"