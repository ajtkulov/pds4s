name := "pds4s"

version := "0.1"

run / javaOptions += "-Xmx16G"

javaOptions in(Test, run) += "-Xmx16G"

parallelExecution in Test := false

fork := true

lazy val commonSettings = Seq(
  organization := "",
  scalaVersion := "2.12.10",
  sources in(Compile, doc) := Seq.empty,
  publishArtifact in(Compile, packageDoc) := false,
  publishArtifact in(Compile, packageSrc) := false
)

lazy val core = Project(id = "core", base = file("core"))
  .configs(ItTest).settings(inConfig(ItTest)(Defaults.testTasks): _*)
  .settings(
    testOptions in Test := Seq(Tests.Filter(unitFilter)),
    testOptions in ItTest := Seq(Tests.Filter(itFilter))
  )

lazy val root = Project(id = "pds4s", base = file("."))
  .aggregate(core)
  .configs(ItTest).settings(inConfig(ItTest)(Defaults.testTasks): _*)
  .settings(
    testOptions in Test := Seq(Tests.Filter(unitFilter)),
    testOptions in ItTest := Seq(Tests.Filter(itFilter))
  )

lazy val ItTest = config("integrationTest") extend (Test)
def itFilter(name: String): Boolean = name endsWith "ItTest"
def unitFilter(name: String): Boolean = !itFilter(name)

