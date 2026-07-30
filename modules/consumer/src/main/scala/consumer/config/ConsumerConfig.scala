package consumer.config

import java.util.Properties

/**
 * Représentation typée de la configuration du consumer.
 *
 * @param inputDir      Dossier surveillé en streaming (= data/destination du producer).
 * @param checkpointDir Dossier de checkpoint Spark pour la tolérance aux pannes.
 */
case class ConsumerConfig(
  inputDir:      String,
  outputDir:     String,
  checkpointDir: String,
  modelPath:     String
)

object ConsumerConfig {

  /**
   * Charge la configuration depuis application.properties (classpath).
   */
  def load(): ConsumerConfig = {
    val props  = new Properties()
    val stream = getClass.getClassLoader.getResourceAsStream("application.properties")
    if (stream == null) throw new RuntimeException("Config introuvable : application.properties")
    props.load(stream)
    ConsumerConfig(
      inputDir      = props.getProperty("consumer.input-dir").trim,
      outputDir     = props.getProperty("consumer.output-dir").trim,
      checkpointDir = props.getProperty("consumer.checkpoint-dir").trim,
      modelPath     = props.getProperty("consumer.model-path").trim
    )
  }
}
