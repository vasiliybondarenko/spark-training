lazy val root = (project in file(".")).settings(
  inThisBuild(
    List(
      organization := "com.example",
      scalaVersion := "2.12.10",
      version := "0.1.0-SNAPSHOT"
    )
  ),
  name := "spark-training",
  resolvers += Resolver.mavenLocal,
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.4.5" % "provided",
    "org.apache.spark" %% "spark-sql" % "2.4.5",
    "org.scalatest" %% "scalatest" % "3.0.8" % "test"
  )
)
