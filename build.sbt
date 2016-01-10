import sbt.Keys._

organization := "com.dudanov"
name := "dynamic-connectivity"
version := "1.0"


scalaVersion   := "2.11.7"

scalacOptions ++= List(
    "-deprecation",
    "-encoding", "UTF-8",
    "-feature",
    "-language:existentials",
    "-language:higherKinds",
    "-language:implicitConversions",
    "-unchecked",
    "-Xfatal-warnings",
    "-Xlint",
    "-Yno-adapted-args",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    //"-Ywarn-value-discard", // fails with @sp on Unit
    "-Xfuture"
)

crossScalaVersions := List(scalaVersion.value)




libraryDependencies ++= List(
   "org.apache.spark" %%  "spark-core"  % "1.6.0",
   "org.scalatest"    %%  "scalatest"   % "2.2.1"   % "test",
   "junit"            %   "junit"       % "4.11"    % "test"

)




