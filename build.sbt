name := "table_checker"
organization := "ru.beeline"

version := "0.1"

scalaVersion := "2.11.8"

resolvers += "Nexus local" at "https://nexus-repo.dmp.vimpelcom.ru/repository/sbt_releases_/"

val sparkVersion = "2.3.2"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion  % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion  % "provided",
)

assembly / artifact := {
  val art = (assembly / artifact).value
  art.withClassifier(Some("assembly"))
}

addArtifact(assembly / artifact, assembly)

assemblyMergeStrategy in assembly := {
  case path if path.contains("META-INF/services") => MergeStrategy.concat
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}
