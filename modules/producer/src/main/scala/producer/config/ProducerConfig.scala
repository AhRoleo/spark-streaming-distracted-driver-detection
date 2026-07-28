package producer.config

import java.util.Properties

/**
 * Représentation typée de la configuration du producer.
 * Chaque champ correspond à une clé dans application.conf.
 *
 * @param inputDir     Chemin du dossier source (fichiers à envoyer).
 * @param outputDir    Chemin du dossier de destination (contrat avec le consumer).
 * @param batchSize    Nombre de fichiers copiés par lot.
 * @param intervalTime Durée d'attente entre deux lots (en secondes).
 */
case class ProducerConfig(
  inputDir:     String,
  outputDir:    String,
  batchSize:    Int,
  intervalTime: Long
)

object ProducerConfig {

  /**
   * Charge la configuration depuis application.properties (classpath) et retourne
   * un objet ProducerConfig avec des valeurs typées.
   */
  def load(): ProducerConfig = {
    // Chargement du fichier de config depuis les ressources du classpath
    val props  = new Properties()
    val stream = getClass.getClassLoader.getResourceAsStream("application.properties")
    if (stream == null) throw new RuntimeException("Config introuvable : application.properties")
    props.load(stream)
    ProducerConfig(
      inputDir     = props.getProperty("producer.input-dir").trim,
      outputDir    = props.getProperty("producer.output-dir").trim,
      batchSize    = props.getProperty("producer.batch-size").trim.toInt,
      intervalTime = props.getProperty("producer.interval-time").trim.toLong
    )
  }
}


//package producer.config
//
//import scala.io.Source
//
//case class ProducerConfig(
//                           inputDir:     String,
//                           outputDir:    String,
//                           batchSize:    Int,
//                           intervalTime: Long
//                         )
//
//object ProducerConfig {
//
//
//  // Valeurs par défaut si le fichier est absent
//
//  private val defaults = Map(
//    "producer.input-dir"    -> "data/source",
//    "producer.output-dir"   -> "data/destination",
//    "producer.batch-size"   -> "1",
//    "producer.interval-time"-> "5"
//  )
//
//  def load(): ProducerConfig = {
//    val props = Option(getClass.getClassLoader.getResourceAsStream("application.properties"))
//      .map { stream =>
//        // Fichier trouvé → parser les lignes clé=valeur
//        Source.fromInputStream(stream)
//          .getLines()
//          .filter(line => line.contains("=") && !line.startsWith("#"))
//          .map { line =>
//            val Array(key, value) = line.split("=", 2)
//            key.trim -> value.trim
//          }
//          .toMap
//      }
//      .getOrElse {
//        // Fichier absent → avertissement + valeurs par défaut
//        println("[WARN] application.properties introuvable — utilisation des valeurs par défaut.")
//        Map.empty[String, String]
//      }
//
//    // Pour chaque clé, on prend la valeur du fichier si elle existe, sinon le défaut
//    def get(key: String): String = props.getOrElse(key, defaults(key))
//
//    ProducerConfig(
//      inputDir     = get("producer.input-dir"),
//      outputDir    = get("producer.output-dir"),
//      batchSize    = get("producer.batch-size").toInt,
//      intervalTime = get("producer.interval-time").toLong
//    )
//  }
//}
