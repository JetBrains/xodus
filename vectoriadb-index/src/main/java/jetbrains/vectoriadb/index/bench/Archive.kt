package jetbrains.vectoriadb.index.bench

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.commons.compress.utils.IOUtils
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.extension

data class Archive(
    val archiveName: String,
    // fileInside lets us easily check whether the archive was extracted or not
    val fileInside: String
)

data class DownloadedArchive(val archiveFile: File, val fileInside: String) {
    fun extractTo(targetDir: Path) {
        val extractedFileName = targetDir.resolve(fileInside)
        if (Files.exists(extractedFileName)) {
            println("the archive $archiveFile is already extracted to ${targetDir}, so extracting is not required")
            return
        }

        extractToImpl(targetDir)
    }

    private fun extractToImpl(targetDirectory: Path) {
        val archive = archiveFile
        when {
            archive.name.endsWith(".tar.gz") -> extractTarGz(archive, targetDirectory)
            archive.name.endsWith(".gz") -> extractGz(archive, targetDirectory.resolve(fileInside).toFile())
            else -> throw IllegalArgumentException("Archive format $archive is not supported")
        }
    }

    /**
     * Extracts .tar.gz files
     * */
    private fun extractTarGz(archive: File, targetDirectory: Path) {
        println("Extracting $archive into $targetDirectory")
        FileInputStream(archive).use { fis ->
            GzipCompressorInputStream(fis).use { giz ->
                TarArchiveInputStream(giz).use { tar ->
                    var entry = tar.nextTarEntry
                    while (entry != null) {
                        val entryName = entry.name
                        val fileName = Path.of(entryName).fileName

                        if (fileName.extension.isNotBlank()) {
                            println("Extracting $entryName")

                            val file = targetDirectory.resolve(fileName).toFile()

                            FileOutputStream(file).use { fos ->
                                IOUtils.copy(tar, fos)
                            }
                        }

                        entry = tar.nextTarEntry
                    }
                }
            }
        }
        println("$archive extracted")
    }

    /**
     * Extracts .gz files
     * */
    private fun extractGz(archive: File, targetFile: File) {
        println("Extracting $archive into $targetFile")

        FileInputStream(archive).use { fis ->
            GzipCompressorInputStream(fis).use { giz ->
                Files.copy(giz, targetFile.toPath())
            }
        }
        println("$archive extracted")
    }
}