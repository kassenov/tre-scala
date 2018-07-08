scalaVersion := "2.12.4"

name := "hello-world"
organization := "ch.epfl.scala"
version := "1.0"

libraryDependencies += "org.apache.commons" % "commons-compress" % "1.17"
libraryDependencies += "org.apache.lucene" % "lucene-analyzers-common" % "7.4.0"
libraryDependencies += "org.apache.lucene" % "lucene-queryparser" % "7.4.0"
libraryDependencies += "org.typelevel" %% "cats-core" % "1.0.1"
libraryDependencies += "commons-io" % "commons-io" % "2.6"
libraryDependencies += "com.google.code.gson" % "gson" % "2.2.4"
libraryDependencies += "com.google.guava" % "guava" % "18.0"
libraryDependencies += "com.lihaoyi" %% "ujson" % "0.6.6"
