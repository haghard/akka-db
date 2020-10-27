import com.typesafe.sbt.SbtMultiJvm
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm
import sbt.CrossVersion

val akkaVersion = "2.6.10"

resolvers += Resolver.sonatypeRepo("snapshots")

val `akka-db` = project
  .in(file("."))
  .settings(SbtMultiJvm.multiJvmSettings: _*)
  .settings(
    name := "akka-db",
    version := "0.0.1",
    scalaVersion := "2.13.3",

    //scalacOptions in Compile ++= Seq("-deprecation", "-feature", "-unchecked", "-Xlog-reflective-calls", "-Xlint"),

    javacOptions in Compile ++= Seq("-Xlint:unchecked", "-Xlint:deprecation"),

    //javaOptions in run ++= Seq("-Xmx3G", "-XX:MaxMetaspaceSize=2500MB", "-XX:+UseG1GC"),

    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-cluster-typed" % akkaVersion,
      //"com.typesafe.akka" %% "akka-http" % "10.1.12",

      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
      "ch.qos.logback" % "logback-classic" % "1.2.3",

      //"org.rocksdb" % "rocksdbjni" % "6.2.2",
      "org.rocksdb" % "rocksdbjni" %  "6.10.2",  //Jun, 2020

      //https://github.com/fusesource/rocksdbjni
      //"org.fusesource" % "rocksdbjni" % "99-master-SNAPSHOT",

      //https://github.com/wjglerum/IoT-collector.git
      //"io.waylay.influxdb" %% "influxdb-scala" % "2.0.1",
      //"com.github.mpilquist" %% "simulacrum" % "0.12.0",

      //com.rbmhtechnology" %% "eventuate-crdt" % "0.10",
      
      //"org.hdrhistogram"  % "HdrHistogram" %  "2.1.10",
      ("com.lihaoyi" % "ammonite" % "2.2.0" % "test").cross(CrossVersion.full),

      "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaVersion),

    fork in run := true,

    // disable parallel tests
    parallelExecution in Test := false,

    javaOptions ++= Seq("-Xmx3G", "-XX:MaxMetaspaceSize=2G", "-XX:+UseG1GC")

  ) configs MultiJvm

//https://tpolecat.github.io/2017/04/25/scalac-flags.html

scalafmtOnCompile := true

/*
scalariformPreferences := scalariformPreferences.value
  .setPreference(AlignArguments, true)
  .setPreference(AlignSingleLineCaseStatements, true)
  .setPreference(DoubleIndentConstructorArguments, true)
  .setPreference(DanglingCloseParenthesis, Preserve)
  .setPreference(RewriteArrowSymbols, true)
*/

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