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
