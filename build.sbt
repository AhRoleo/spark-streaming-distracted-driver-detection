// ─── Versions ────────────────────────────────────────────────────────────────
val sparkVersion = "3.3.2"
val scalaVer     = "2.12.17"

// ─── Options Java 17 ─────────────────────────────────────────────────────────
// Ces options sont requises pour que Spark 3.3.x fonctionne avec Java 17
// (Java 17 bloque l'accès aux modules internes par défaut)
val javaOpts = Seq(
  "--add-opens", "java.base/java.lang=ALL-UNNAMED",
  "--add-opens", "java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens", "java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens", "java.base/java.io=ALL-UNNAMED",
  "--add-opens", "java.base/java.net=ALL-UNNAMED",
  "--add-opens", "java.base/java.nio=ALL-UNNAMED",
  "--add-opens", "java.base/java.util=ALL-UNNAMED",
  "--add-opens", "java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens", "java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens", "java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens", "java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens", "java.base/sun.util.calendar=ALL-UNNAMED",
  "--add-opens", "java.security.jgss/sun.security.jgss=ALL-UNNAMED"
)

// ─── Dépendances Spark partagées ─────────────────────────────────────────────
val sparkDeps = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion
    exclude("org.apache.logging.log4j", "log4j-slf4j-impl"),
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.slf4j" % "slf4j-simple" % "1.7.32"
)

// ─── Paramètres communs à tous les modules ───────────────────────────────────
lazy val commonSettings = Seq(
  scalaVersion         := scalaVer,
  version              := "1.0",
  run / fork           := true,
  javaOptions         ++= javaOpts,
  libraryDependencies ++= sparkDeps,
  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", "services", _*)                     => MergeStrategy.concat
    case PathList("META-INF", "MANIFEST.MF")                      => MergeStrategy.discard
    case PathList("META-INF", "versions", _, "module-info.class") => MergeStrategy.first
    case PathList("META-INF", "io.netty.versions.properties")     => MergeStrategy.first
    case PathList("META-INF", "org", "apache", "logging", _*)    => MergeStrategy.first
    case PathList("META-INF", _*)                                 => MergeStrategy.discard
    case PathList("org", "apache", "commons", "logging", _*)      => MergeStrategy.first
    case PathList("org", "apache", "spark", "unused", _*)         => MergeStrategy.first
    case PathList("org", "apache", "logging", _*)                 => MergeStrategy.first
    case "module-info.class"                                      => MergeStrategy.discard
    case "reference.conf"                                         => MergeStrategy.concat
    case _                                                        => MergeStrategy.first
  }
)

// ─── Module common ───────────────────────────────────────────────────────────
// Contient : SparkSessionFactory (loan pattern) + configuration logback partagée
lazy val common = (project in file("modules/common"))
  .settings(
    commonSettings,
    name := "common"
  )

// ─── Module producer ─────────────────────────────────────────────────────────
// Contient : FileProducer (main), ProducerConfig, FileTransfer
// Dépend de common pour utiliser SparkSessionFactory
lazy val producer = (project in file("modules/producer"))
  .dependsOn(common)
  .settings(
    commonSettings,
    name                := "producer",
    Compile / mainClass := Some("producer.FileProducer"),
    // Le JVM forké s'exécute depuis la racine du projet pour trouver data/source et data/destination
    run / baseDirectory := (ThisBuild / baseDirectory).value
  )

// ─── Module consumer ─────────────────────────────────────────────────────────
// Contient : StructuredStreamingConsumer (main), ConsumerConfig
// Dépend de common pour utiliser SparkSessionFactory
lazy val consumer = (project in file("modules/consumer"))
  .dependsOn(common)
  .settings(
    commonSettings,
    name                := "consumer",
    Compile / mainClass := Some("consumer.StructuredStreamingConsumer"),
    // Le JVM forké s'exécute depuis la racine du projet pour trouver data/destination
    run / baseDirectory := (ThisBuild / baseDirectory).value,
    libraryDependencies ++= Seq(
      "com.sksamuel.scrimage" % "scrimage-core"   % "4.0.38",
      "com.microsoft.onnxruntime" % "onnxruntime" % "1.16.3"
    )
  )

// ─── Projet racine ───────────────────────────────────────────────────────────
// Agrège tous les modules : `sbt compile` compile tout, `sbt producer/run` lance le producer
lazy val root = (project in file("."))
  .aggregate(common, producer, consumer)
  .settings(
    name := "spark-file-streaming",
    // Le projet racine n'a pas de sources propres, il délègue aux modules
    Compile / sources                   := Seq.empty,
    Compile / unmanagedSourceDirectories := Seq.empty
  )

