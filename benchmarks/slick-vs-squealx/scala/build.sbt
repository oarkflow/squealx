name := "slick-vs-squealx-bench"
version := "0.1-SNAPSHOT"
scalaVersion := "2.12.21"

// Versions matched to clear/fulltext's build.sbt so the Slick side of this
// comparison reflects what that app actually runs in production.
libraryDependencies ++= Seq(
  "com.typesafe.slick" %% "slick" % "3.6.1",
  "com.mysql" % "mysql-connector-j" % "8.4.0",
  "ch.qos.logback" % "logback-classic" % "1.5.32"
)

run / fork := true
