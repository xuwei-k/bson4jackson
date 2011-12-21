scalaVersion := "2.9.1"

libraryDependencies ++= {
val (j,v) = ("org.codehaus.jackson","1.7.4")
Seq(
  j % "jackson-core-asl" % v,
  j % "jackson-mapper-asl" % v,
  "org.mongodb" % "mongo-java-driver" % "2.5.3" % "test",
  "junit" % "junit" % "4.10" % "test",
  "com.novocode" % "junit-interface" % "0.7" % "test->default"
)
}

name := "bson4jackson"

version := "1.2.0"

initialCommands in console := {
  "import de.undercouch.bson4jackson._"
}

scalacOptions ++= Seq(
  "-deprecation"
)
