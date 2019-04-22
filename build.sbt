import com.typesafe.sbt.SbtMultiJvm
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm
import sbt.CrossVersion
import scalariform.formatter.preferences._

val akkaVersion = "2.5.22"

resolvers += Resolver.sonatypeRepo("snapshots")

val `akka-db` = project
  .in(file("."))
  .settings(SbtMultiJvm.multiJvmSettings: _*)
  .settings(
    name := "akka-db",
    version := "0.0.1",
    scalaVersion := "2.12.8",

    //scalacOptions in Compile ++= Seq("-deprecation", "-feature", "-unchecked", "-Xlog-reflective-calls", "-Xlint"),

    javacOptions in Compile ++= Seq("-Xlint:unchecked", "-Xlint:deprecation"),

    //javaOptions in run ++= Seq("-Xmx3G", "-XX:MaxMetaspaceSize=2500MB", "-XX:+UseG1GC"),

    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-cluster-typed" % akkaVersion,
      "com.typesafe.akka" %% "akka-http" % "10.1.7",
      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,

      "com.github.TanUkkii007" %% "akka-cluster-custom-downing" % "0.0.12",

      "com.github.mpilquist" %% "simulacrum" % "0.12.0",

      "org.rocksdb" % "rocksdbjni" % "5.17.2",

      "ch.qos.logback" % "logback-classic" % "1.2.3",

      "com.rbmhtechnology" %% "eventuate-crdt" % "0.10",
      
      //"org.hdrhistogram"  % "HdrHistogram" %  "2.1.10",
      ("com.lihaoyi" % "ammonite" % "1.6.0" % "test").cross(CrossVersion.full),

      "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaVersion),

    fork in run := true,

    // disable parallel tests
    parallelExecution in Test := false,

    javaOptions ++= Seq("-Xmx3G", "-XX:MaxMetaspaceSize=2G", "-XX:+UseG1GC")

  ) configs (MultiJvm)

//https://tpolecat.github.io/2017/04/25/scalac-flags.html


scalariformPreferences := scalariformPreferences.value
  .setPreference(AlignArguments, true)
  .setPreference(AlignSingleLineCaseStatements, true)
  .setPreference(DoubleIndentConstructorArguments, true)
  .setPreference(DanglingCloseParenthesis, Preserve)
  .setPreference(RewriteArrowSymbols, true)

//test:run test:console
sourceGenerators in Test += Def.task {
  val file = (sourceManaged in Test).value / "amm.scala"
  IO.write(file, """object amm extends App { ammonite.Main().run() }""")
  Seq(file)
}.taskValue

promptTheme := ScalapenosTheme

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)

// (optional) If you need scalapb/scalapb.proto or anything from google/protobuf/*.proto
libraryDependencies += "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)