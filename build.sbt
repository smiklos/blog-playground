
name := "blog-playground"

version := "0.1"

scalaVersion := "2.11.12"
val sparkVersion = "2.4.4"


val withCommonSettings: Seq[Setting[_]] = {
  inThisBuild(
    Seq(
      organization := "com.smiklos",
      scalaVersion := "2.11.12",
      credentials ++= Seq(
        Credentials(Path.userHome / ".ivy2" / ".credentials")
        ),
      parallelExecution := false,
      javaOptions ++= Seq(
        "-Xms512M",
        "-Xmx2048M",
        "-XX:MaxPermSize=2048M",
        "-XX:+CMSClassUnloadingEnabled"
        ),
      scalacOptions := Seq(
        "-Xexperimental",
        "-feature",
        "-deprecation",
        "-Ypatmat-exhaust-depth",
        "off",
        "-Xmax-classfile-name",
        "144"
        )
      )
    )
}

val withTesting: Seq[Setting[_]] = Seq(
  parallelExecution in Test := false,
  fork in Test := false,
  testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")
  )

val dependencies: Seq[ModuleID] = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.storm-enroute" %% "scalameter" % "0.19"
  )

val logging: Seq[ModuleID] = Seq(
  "com.typesafe.scala-logging" %% "scala-logging-api" % "2.1.2",
  "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2"
  )

val root = project.in(file("."))
  .settings(name := "blog-playground")
  .settings(withCommonSettings: _*)
  .settings(withTesting: _*)
  .settings(inThisBuild(libraryDependencies ++= dependencies ++ logging ))

