name := "spark-example"

version := "0.1"

scalaVersion := "2.10.7"


libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.0"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.5"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"
libraryDependencies += "commons-net" % "commons-net" % "3.6"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.6.0"
libraryDependencies += "com.h2database" % "h2" % "1.4.192" % "test"
libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.10.7"

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}