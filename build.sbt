import sbt.Keys._
import sbt._


name := "sbt_test"

version := "1.0"

scalaVersion := "2.11.5"


libraryDependencies +=  "org.scalaj" %% "scalaj-http" % "1.1.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.0"

resolvers += "justwrote" at "http://repo.justwrote.it/releases/"

libraryDependencies += "it.justwrote" %% "scala-faker" % "0.3"

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
  case PathList("javax", "pom.properties", xs@_*) => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".properties" => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".class" => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".RSA" => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith "mailcap" => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".xml" => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".html" => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".dtd" => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".default" => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".xsd" => MergeStrategy.first
  case "pom.properties" => MergeStrategy.first
  case "unwanted.txt" => MergeStrategy.discard
  case x => old(x)
}
}

assemblyJarName in assembly := "spark_hello_world.jar"
