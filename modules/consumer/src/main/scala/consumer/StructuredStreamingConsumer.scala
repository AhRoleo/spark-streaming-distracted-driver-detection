package consumer

import common.spark.SparkSessionFactory
import consumer.config.ConsumerConfig

import org.apache.spark.sql.DataFrame
import org.apache.hadoop.fs.{FileSystem, Path}
import common.helpers.FileTransfer

object StructuredStreamingConsumer {

  def run(config: ConsumerConfig, spark: org.apache.spark.sql.SparkSession): Unit = {

    import spark.implicits._

    // 1. Lecture en streaming (ne traiter que les fichiers texte)
    val stream = spark.readStream
      .format("text")
      .option("pathGlobFilter", "*.txt")
      .load(config.inputDir)

    // 2. Word count
    val wordCount = stream
      .as[String]                                    // DataFrame → Dataset[String]
      .flatMap(line => line.split("\\s+"))           // chaque ligne → liste de mots
      .filter(word => word.nonEmpty)                 // ignorer les mots vides
      .groupBy("value")                              // grouper par mot
      .count()                                       // compter les occurrences



    // 3. Sink vers fichier via foreachBatch pour le Streamlit
    val query = wordCount.writeStream
      .outputMode("complete")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        val conf = batchDF.sparkSession.sparkContext.hadoopConfiguration
        val fs = FileSystem.get(conf)
        val tempPath = new Path("data/temp_output")
        val finalFile = new Path(config.outputDir, "wordcount.csv")

        // S'assurer que le dossier final existe
        val finalDir = new Path(config.outputDir)
        if (!fs.exists(finalDir)) fs.mkdirs(finalDir)

        // Ecrire le batch dans un dossier temporaire
        batchDF.coalesce(1)
          .write
          .mode("overwrite")
          .option("header", "true")
          .csv(tempPath.toString)

        // Trouver le fichier CSV généré
        val files = fs.listStatus(tempPath)
        val csvFileOpt = Option(files).flatMap(_.find(status => {
          val name = status.getPath.getName
          name.endsWith(".csv") && name.startsWith("part-")
        }))
        
        csvFileOpt.foreach { fileStatus =>
          // Déplacer (copier avec écrasement) vers le fichier final en utilisant FileTransfer
          FileTransfer.copyFile(
            fs,
            fileStatus.getPath,
            fs,
            finalFile,
            conf
          )
        }

        // Nettoyer le dossier temp
        if (fs.exists(tempPath)) {
          fs.delete(tempPath, true)
        }
        ()
      }
      .option("checkpointLocation", config.checkpointDir)
      .start()

    println(s"[Consumer] Word count en écoute sur '${config.inputDir}'... (Ctrl+C pour arrêter)")

    query.awaitTermination()
  }

  /**
   * Point d'entrée JVM du module consumer.
   */
  def main(args: Array[String]): Unit = {
    val config = ConsumerConfig.load()

    println("=== StructuredStreamingConsumer ===")
    println(s"  input-dir      : ${config.inputDir}")
    println(s"  checkpoint-dir : ${config.checkpointDir}")
    println("===================================\n")

    SparkSessionFactory.withSession("StructuredStreamingConsumer") { spark =>
      run(config, spark)
    }
  }
}
