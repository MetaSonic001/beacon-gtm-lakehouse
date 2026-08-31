name := "beacon-scala-jobs"
version := "0.1.0"
scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.1" % "provided",
  "org.apache.spark" %% "spark-sql"  % "3.5.1" % "provided"
)

// sbt assembly (add sbt-assembly to project/plugins.sbt) to build a fat jar:
//   spark-submit --class beacon.CustomerLtvJob target/scala-2.12/beacon-scala-jobs-assembly-0.1.0.jar
