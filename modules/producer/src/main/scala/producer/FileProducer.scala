package producer

import common.spark.SparkSessionFactory
import org.apache.hadoop.fs.{FileSystem, Path}
import producer.config.ProducerConfig
import common.helpers.FileTransfer

/**
 * Point d'entrée principal du module producer.
 *
 * Rôle : Lit les fichiers depuis data/source, les copie vers data/destination
 * par lots (batches) avec un temps d'attente entre chaque lot,
 * simulant ainsi un flux de données en streaming.
 */
object FileProducer {

  /**
   * Logique principale du producer : liste, découpe en lots et copie les fichiers.
   *
   * @param config La configuration chargée depuis application.conf.
   * @param sc     Le SparkContext actif, fourni par SparkSessionFactory.
   */
  def run(config: ProducerConfig, sc: org.apache.spark.SparkContext): Unit = {

    // Initialisation des FileSystem Hadoop pour la source et la destination
    val fs     = FileSystem.get(sc.hadoopConfiguration)
    val destFs = FileSystem.get(new Path(config.outputDir).toUri, sc.hadoopConfiguration)

    // Création du dossier de destination s'il n'existe pas encore
    if (!fs.exists(new Path(config.outputDir))) {
      fs.mkdirs(new Path(config.outputDir))
    }

    // Liste tous les fichiers présents dans le dossier source
    val files = fs.listStatus(new Path(config.inputDir))
      .filter(_.isFile)   // Exclure les sous-dossiers
      .map(_.getPath)     // Ne garder que le chemin Hadoop (Path) de chaque fichier

    if (files.nonEmpty) {
      println(s"\nEnvoi depuis '${config.inputDir}' vers '${config.outputDir}' " +
        s"par lot de ${config.batchSize} fichier(s), intervalle : ${config.intervalTime}s")

      // Découpe la liste en sous-groupes de taille batchSize pour simuler le streaming
      files.grouped(config.batchSize).foreach { batch =>

        // Copie chaque fichier du lot vers la destination via FileTransfer
        batch.foreach { file =>
          val destPath = new Path(config.outputDir, file.getName)
          FileTransfer.copyFile(fs, file, destFs, destPath, sc.hadoopConfiguration)
          println(s"  [OK] Fichier copié : ${file.getName}")
        }

        println(s"  Batch terminé. Attente de ${config.intervalTime}s avant le prochain lot...")
        // Pause simulant l'intervalle entre deux micro-batches de streaming
        Thread.sleep(config.intervalTime * 1000)
      }

      println("\n[FIN] Tous les lots ont été traités.")
    } else {
      println(s"Aucun fichier trouvé dans '${config.inputDir}'.")
    }
  }

  /**
   * Point d'entrée JVM du module producer.
   * Charge la config, initialise Spark via SparkSessionFactory et lance le traitement.
   */
  def main(args: Array[String]): Unit = {
    // Chargement de la configuration typée depuis application.conf
    val config = ProducerConfig.load()

    println("=== FileProducer (Simulation de streaming) ===")
    println(s"  input-dir     : ${config.inputDir}")
    println(s"  output-dir    : ${config.outputDir}")
    println(s"  batch-size    : ${config.batchSize} fichier(s) par batch")
    println(s"  interval-time : ${config.intervalTime}s")
    println("=============================================\n")

    // SparkSessionFactory.withSession garantit l'arrêt de Spark même en cas d'erreur
    SparkSessionFactory.withSession("FileProducer") { spark =>
      run(config, spark.sparkContext)
    }
  }
}
