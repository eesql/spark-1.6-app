name := "spark-1.6-app"
version := "1.0"
scalaVersion := "2.10.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.3" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.6.3" % "provided",
  "org.apache.spark" %% "spark-hive" % "1.6.3" % "provided",
  "org.apache.spark" %% "spark-mllib" % "1.6.3" % "provided"
)

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
