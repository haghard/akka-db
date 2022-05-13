import com.typesafe.sbt.SbtMultiJvm
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm
import sbt.CrossVersion

val akkaVersion = "2.6.19"
val prometheusClient = "0.8.1"

resolvers += Resolver.sonatypeRepo("snapshots")

val `akka-db` = project
  .in(file("."))
  .settings(SbtMultiJvm.multiJvmSettings: _*)
  .settings(
    name := "akka-db",
    version := "0.0.1",
    scalaVersion := "2.13.8",

    //scalacOptions in Compile ++= Seq("-deprecation", "-feature", "-unchecked", "-Xlog-reflective-calls", "-Xlint"),

    Compile / javacOptions ++= Seq("-Xlint:unchecked", "-Xlint:deprecation"),

    //javaOptions in run ++= Seq("-Xmx3G", "-XX:MaxMetaspaceSize=2500MB", "-XX:+UseG1GC"),

    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-cluster-typed" % akkaVersion,

      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
      "ch.qos.logback" % "logback-classic" % "1.2.11",
      
      //"com.google.guava"  % "guava" % "30.1.1-jre",

      //"org.rocksdb" % "rocksdbjni" %  "6.10.2",  //Jun, 2020
      "org.rocksdb" % "rocksdbjni" % "7.1.2", //May 07, 2021


      //https://github.com/wjglerum/IoT-collector.git
      //"io.waylay.influxdb" %% "influxdb-scala" % "2.0.1"

      //com.rbmhtechnology" %% "eventuate-crdt" % "0.10",

      "io.prometheus" % "simpleclient"         % prometheusClient,
      "io.prometheus" % "simpleclient_common"  % prometheusClient,
      //"io.prometheus" % "simpleclient_hotspot" % prometheusClient,

      "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaVersion,

      //"org.hdrhistogram"  % "HdrHistogram" %  "2.1.10",
      "com.lihaoyi" % "ammonite" % "2.5.2" % "test" cross CrossVersion.full,
      
    ),

    run / fork := false, //true,

    // disable parallel tests
    Test / parallelExecution := false,

    //javaOptions ++= Seq("-Xmx3G", "-XX:MaxMetaspaceSize=2G", "-XX:+UseG1GC")

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
Test / sourceGenerators += Def.task {
  val file = (Test / sourceManaged).value / "amm.scala"
  IO.write(file, """object amm extends App { ammonite.Main().run() }""")
  Seq(file)
}.taskValue

promptTheme := ScalapenosTheme

Compile / PB.targets := Seq(scalapb.gen() -> (sourceManaged in Compile).value)

// (optional) If you need scalapb/scalapb.proto or anything from google/protobuf/*.proto
libraryDependencies += "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"