name := "Spark Scala"
version := "0.1"
scalaVersion := "2.12.10"
libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-reflect" % "2.12.10",
  "org.apache.spark" %% "spark-sql" % "3.1.2" % "provided",
  "com.holdenkarau" %% "spark-testing-base" % "3.1.2_1.1.0" % "test"
)