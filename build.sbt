import scalariform.formatter.preferences._

name := """cbscheckaccounts"""

version := "1.1"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream-experimental" % "2.0.1",
  "org.scala-lang.modules" %% "scala-xml" % "1.0.6",
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4",
 // "org.scalaa-lang.modules" %% "scala-swing" % "1.0.2",
  "com.zaxxer" % "HikariCP" % "2.6.0",
  "oracle" % "ojdbc" % "6.0",
  "commons-dbutils" % "commons-dbutils" % "1.6",
  "org.apache.commons" % "commons-lang3" % "3.5",
  "commons-codec" % "commons-codec" % "1.10"
)

scalariformSettings

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(AlignSingleLineCaseStatements, true)
  .setPreference(AlignSingleLineCaseStatements.MaxArrowIndent, 100)
  .setPreference(DoubleIndentClassDeclaration, true)
  .setPreference(PreserveDanglingCloseParenthesis, true)

fork in run := true
