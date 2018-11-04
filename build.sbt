//scalaVersion := "2.12"
//crossScalaVersions := Seq("2.12.7", "2.13.0-M2")
//scalaVersion := crossScalaVersions.value.head
//
////scalacOptions in Compile += "-deprecation"
//scalacOptions ++=
//  (CrossVersion.partialVersion(scalaVersion.value) match {
//    case Some((2, n)) if n >= 12 => Seq("-Xsource:2.12", "-Xsource:2.12")
//    case _ => Seq("-Yno-adapted-args")
//  })
scalaVersion := "2.13.0-M3"

name := "tre-scala"
organization := "ca.uofa.scala"
version := "1.0"

libraryDependencies += "org.scala-lang" % "scala-library" % "2.13.0-M3"
libraryDependencies += "org.scala-lang.modules" % "scala-parallel-collections_2.13.0-M2" % "1.0.2"
libraryDependencies += "org.apache.commons" % "commons-compress" % "1.17"
libraryDependencies += "org.apache.commons" % "commons-text" % "1.4"
libraryDependencies += "org.apache.lucene" % "lucene-analyzers-common" % "7.4.0"
libraryDependencies += "org.apache.lucene" % "lucene-queryparser" % "7.4.0"
libraryDependencies += "org.typelevel" % "cats-core_2.12" % "1.0.1"
libraryDependencies += "commons-io" % "commons-io" % "2.6"
libraryDependencies += "com.google.code.gson" % "gson" % "2.2.4"
libraryDependencies += "com.google.guava" % "guava" % "18.0"
libraryDependencies += "com.lihaoyi" % "ujson_2.12" % "0.6.6"
libraryDependencies += "de.siegmar" % "fastcsv" % "1.0.2"
libraryDependencies += "net.liftweb" % "lift-json_2.12" % "3.3.0"

//fork in run := true
