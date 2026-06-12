package common.spark

import org.apache.spark.sql.SparkSession

/**
 * Factory centralisée pour créer et gérer le cycle de vie d'une SparkSession.
 *
 * Utilise le "loan pattern" : la session est prêtée au bloc de code,
 * puis automatiquement arrêtée à la fin (même en cas d'erreur).
 */
object SparkSessionFactory {

  /**
   * Crée une SparkSession, exécute le bloc de code fourni, puis l'arrête proprement.
   *
   * @param appName Le nom de l'application affiché dans l'UI Spark.
   * @param block   Le code à exécuter avec la session Spark (reçoit la session en paramètre).
   * @tparam A      Le type de retour du bloc de code.
   * @return        La valeur retournée par le bloc de code.
   */
  def withSession[A](appName: String)(block: SparkSession => A): A = {
    // Force le niveau de log par défaut de SLF4J SimpleLogger à WARN avant l'initialisation de Spark
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn")

    // Création et configuration de la SparkSession
    // local[*] = utilise tous les cœurs CPU disponibles sur la machine
    val spark = SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()

    // Configure le niveau de log de Spark à WARN pour éviter de polluer la console
    spark.sparkContext.setLogLevel("WARN")

    try {
      // On passe la session au bloc de code fourni par l'appelant
      block(spark)
    } finally {
      // Arrêt garanti de Spark même si une exception est levée dans le bloc
      spark.stop()
    }
  }
}
