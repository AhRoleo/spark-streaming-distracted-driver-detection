package consumer

import ai.onnxruntime.{OnnxTensor, OrtEnvironment, OrtSession}
import com.sksamuel.scrimage.ImmutableImage
import common.helpers.FileTransfer
import common.spark.SparkSessionFactory
import consumer.config.ConsumerConfig
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, functions => F}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import scala.collection.JavaConverters._

object StructuredStreamingConsumer {

  // ─────────────────────────────────────────────
  // Resize + Flatten d'une image depuis ses bytes
  // ─────────────────────────────────────────────
  def resizeAndFlatten(bytes: Array[Byte], width: Int, height: Int): Array[Float] = {
    val image   = ImmutableImage.loader().fromBytes(bytes)
    val resized = image.scaleTo(width, height)
    resized.pixels().flatMap { p =>
      Array(p.red() / 255.0f, p.green() / 255.0f, p.blue() / 255.0f)
    }
  }

  // ─────────────────────────────────────────────
  // Prédiction ONNX depuis un vecteur de pixels
  // ─────────────────────────────────────────────
  /**
   * Effectue la prédiction ONNX à partir d'un vecteur plat de pixels normalisés.
   * 
   * MODIFICATIONS APPORTÉES :
   * - Passage d'un tenseur de rang 2 ([1, size]) à un tenseur de rang 4 ([1, height, width, 3]).
   *   Le modèle de détection de distractions (best_distracted_driver_cnn.onnx) attend un format
   *   d'entrée NHWC (Batch, Height, Width, Channels) correspondant à l'architecture Keras d'origine.
   * - Fermeture explicite des ressources ONNX Runtime (tensors, results, sessions) pour éviter
   *   les fuites de mémoire (memory leaks) dans la mémoire native C++ (off-heap) à chaque prédiction.
   */
  def predict(pixels: Array[Float], modelPath: String, width: Int, height: Int): String = {
    val env     = OrtEnvironment.getEnvironment()
    val session = env.createSession(modelPath)

    // 1. Reshape de la structure 1D plate vers un tableau 4D float[1][height][width][3]
    //    Le format attendu par le modèle ONNX est [Batch = 1, Height = 64, Width = 64, Channels = 3] (RGB).
    val input4D = Array.ofDim[Float](1, height, width, 3)
    var idx = 0
    for (r <- 0 until height) {
      for (c <- 0 until width) {
        if (idx + 2 < pixels.length) {
          input4D(0)(r)(c)(0) = pixels(idx)     // Canal Rouge (R)
          input4D(0)(r)(c)(1) = pixels(idx + 1) // Canal Vert (G)
          input4D(0)(r)(c)(2) = pixels(idx + 2) // Canal Bleu (B)
        }
        idx += 3
      }
    }

    // 2. Création du tenseur d'entrée et exécution de l'inférence
    val inputTensor = OnnxTensor.createTensor(env, input4D)
    val inputName = session.getInputNames.asScala.head
    val results = session.run(Map(inputName -> inputTensor).asJava)

    // 3. Extraction de la classe prédite (index de la probabilité maximale dans le vecteur de sortie de taille 10)
    val output      = results.get(0).getValue.asInstanceOf[Array[Array[Float]]](0)
    val predicted   = output.zipWithIndex.maxBy(_._1)._2.toString

    // 4. Nettoyage et libération des ressources natives
    inputTensor.close()
    results.close()
    session.close()
    predicted
  }

  // ─────────────────────────────────────────────
  // Run principal
  // ─────────────────────────────────────────────
  def run(config: ConsumerConfig, spark: org.apache.spark.sql.SparkSession): Unit = {

    val modelPath = config.modelPath  // à ajouter dans ConsumerConfig
    val imgWidth  = 64
    val imgHeight = 64

    // UDF resize + flatten
    val flattenUDF: UserDefinedFunction = F.udf(
      (bytes: Array[Byte]) => resizeAndFlatten(bytes, imgWidth, imgHeight)
    )

    // UDF prédiction ONNX
    val predictUDF: UserDefinedFunction = F.udf(
      (pixels: Seq[Float]) => predict(pixels.toArray, modelPath, imgWidth, imgHeight)
    )

    // Schéma obligatoire pour le format binaryFile en streaming
    // MODIFICATIONS APPORTÉES :
    // - Définition explicite d'un schéma structuré pour les sources en streaming de type binaryFile.
    //   Par défaut, Spark Structured Streaming requiert la spécification d'un schéma (ou l'activation 
    //   de l'inférence automatique globale) pour éviter une exception de type IllegalArgumentException.
    val imageSchema = StructType(Seq(
      StructField("path", StringType, nullable = true),
      StructField("modificationTime", TimestampType, nullable = true),
      StructField("length", LongType, nullable = true),
      StructField("content", BinaryType, nullable = true)
    ))

    // 1. Lecture en streaming des images
    val stream = spark.readStream
      .format("binaryFile")
      .schema(imageSchema)
      .option("pathGlobFilter", "*.{png,jpg,jpeg}")
      .load(config.inputDir)

    // 2. Resize + Flatten + Prédiction
    val predictions = stream
      .withColumn("pixels",     flattenUDF(F.col("content")))
      .withColumn("prediction", predictUDF(F.col("pixels")))
      .select(
        F.col("path").as("image"),
        F.col("prediction")
      )

    // 3. Sink vers CSV via foreachBatch
    val query = predictions.writeStream
      .outputMode("append")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        if (!batchDF.isEmpty) {
          val conf     = batchDF.sparkSession.sparkContext.hadoopConfiguration
          conf.set("fs.file.impl", classOf[org.apache.hadoop.fs.RawLocalFileSystem].getName) // Pour éviter les fichier .crc
          val fs       = FileSystem.get(conf)
          val tempPath = new Path("data/temp_output")
          val finalDir = new Path(config.outputDir)
          val finalFile = new Path(config.outputDir, "predictions.csv")

          if (!fs.exists(finalDir)) fs.mkdirs(finalDir)

          // Écriture temporaire
          batchDF.coalesce(1)
            .write
            .mode("overwrite")
            .option("header", "true")
            .csv(tempPath.toString)

          // Trouver le part-*.csv généré
          val csvFileOpt = Option(fs.listStatus(tempPath))
            .flatMap(_.find(s => s.getPath.getName.endsWith(".csv") && s.getPath.getName.startsWith("part-")))

          // Copier ou ajouter les prédictions au fichier final (sans écraser l'historique)
          csvFileOpt.foreach { fileStatus =>
            val srcPath = fileStatus.getPath
            if (!fs.exists(finalFile)) {
              // Si le fichier predictions.csv n'existe pas encore, on le copie directement (avec le header)
              FileTransfer.copyFile(fs, srcPath, fs, finalFile, conf)
            } else {
              // 1. Lecture de l'historique existant via Scala Source
              val existingLines = scala.io.Source
                .fromInputStream(fs.open(finalFile))
                .getLines()
                .toVector

              // 2. Lecture du nouveau batch — on saute le header (première ligne)
              val newLines = scala.io.Source
                .fromInputStream(fs.open(srcPath))
                .getLines()
                .drop(1)       // ← saute le header "image,prediction"
                .toVector

              // 3. Réécriture complète via l'API Hadoop (fs.create)
              val out = fs.create(finalFile, true)  // true = overwrite
              try {
                (existingLines ++ newLines).foreach { line =>
                  out.writeBytes(line + "\n")
                }
              } finally {
                out.close()
              }
              // Si le fichier existe, on lit l'historique et on y ajoute les nouvelles lignes (sans leur header)
            }
          }

          // Nettoyage temp
          if (fs.exists(tempPath)) fs.delete(tempPath, true)
        }
        ()
      }
      .option("checkpointLocation", config.checkpointDir)
      .start()

    println(s"[Consumer] Pipeline image en écoute sur '${config.inputDir}'... (Ctrl+C pour arrêter)")
    query.awaitTermination()
  }

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