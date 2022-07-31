name := "filter"

version := "1.0"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.5"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.scala-lang.modules" %% "scala-xml" % "1.3.0",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.5"
)