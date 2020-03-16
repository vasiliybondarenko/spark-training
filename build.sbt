lazy val root = (project in file(".")).settings(
  inThisBuild(
    List(
      organization := "com.example",
      scalaVersion := "2.11.12",
      version := "0.1.0-SNAPSHOT"
    )
  ),
  name := "spark-training",
  resolvers += Resolver.mavenLocal,
  mainClass in assembly := Some("example.wiki.spark.SparkCluster"),
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
  },
  scalacOptions ++= Seq(
    "-language:implicitConversions",
  ),
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.4.5",
    "org.apache.spark" %% "spark-sql" % "2.4.5",
    "org.apache.spark" %% "spark-mllib" % "2.4.5",
    "org.apache.spark" %% "spark-hive" % "2.4.5",
    "org.apache.bahir" %% "spark-streaming-twitter" % "2.4.0",
    "org.scalatest" %% "scalatest" % "3.0.8" % "test"
  )
)
