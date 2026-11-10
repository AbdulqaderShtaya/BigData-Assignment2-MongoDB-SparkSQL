name := "BigData_Assignment2"

version := "1.0"

scalaVersion := "2.13.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "4.2.0" % "provided",
  "org.mongodb" % "mongodb-driver-sync" % "5.3.1"
)
