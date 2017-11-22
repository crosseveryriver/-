name := "spark"

version := "1.0"

//scalaVersion := "2.12.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.10
libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "2.1.0" % "provided"
// https://mvnrepository.com/artifact/org.apache.spark/spark-graphx_2.10
libraryDependencies += "org.apache.spark" % "spark-graphx_2.10" % "2.0.0" % "provided"

libraryDependencies += "org.mongodb" %% "casbah" % "3.1.1"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.0.0" % "provided"



assemblyMergeStrategy in assembly := {
  case PathList(ps @ _*) if ps.last endsWith ".properties" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}