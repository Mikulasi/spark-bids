name := "spark-bids"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0",
  "org.apache.spark" %% "spark-hive" % "2.1.0",
  "junit" % "junit" % "4.11" % "test",
  "com.holdenkarau" % "spark-testing-base_2.10" % "2.0.0_0.6.0" % "test"
)
        