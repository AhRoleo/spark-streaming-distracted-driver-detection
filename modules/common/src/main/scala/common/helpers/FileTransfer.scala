package common.helpers

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

/**
 * Wrapper autour de l'utilitaire Hadoop FileUtil.
 * Centralise la logique de copie de fichiers pour éviter la duplication.
 */
object FileTransfer {

  /**
   * Copie un fichier d'un FileSystem source vers un FileSystem de destination.
   *
   * @param srcFs FileSystem du dossier source.
   * @param src   Chemin Hadoop du fichier à copier.
   * @param dstFs FileSystem du dossier de destination.
   * @param dst   Chemin Hadoop de destination du fichier.
   * @param conf  Configuration Hadoop utilisée pour l'opération.
   */
  def copyFile(
    srcFs: FileSystem,
    src:   Path,
    dstFs: FileSystem,
    dst:   Path,
    conf:  Configuration
  ): Unit = {
    // deleteSource = false → le fichier original est conservé (pas de déplacement)
    // overwrite    = true  → si le fichier existe déjà à destination, il est écrasé
    conf.set("fs.file.impl", classOf[org.apache.hadoop.fs.RawLocalFileSystem].getName)
    FileUtil.copy(srcFs, src, dstFs, dst, false, true, conf)
  }
}
