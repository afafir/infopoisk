name := "crawler"

version := "0.1"

scalaVersion := "2.12.15"
resolvers += "jitpack" at "https://jitpack.io"

idePackagePrefix := Some("ru.itis")

libraryDependencies += "com.softwaremill.sttp.client3" %% "core" % "3.5.0"
libraryDependencies += "org.jsoup" % "jsoup" % "1.14.3"
libraryDependencies += "com.johnsnowlabs.nlp" %% "spark-nlp" % "3.4.2"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.1.2"
libraryDependencies += "com.github.demidko"%% "aot" % "2021.11.17"
