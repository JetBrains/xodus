package jetbrains.vectoriadb.index.bench

import jetbrains.vectoriadb.index.Distance
import java.nio.file.Files
import java.nio.file.Path

@Suppress("unused")
class DownloadDataset {
    fun main() {
        val datasetName = requireParam("dataset")
        val benchPathStr = requireParam("benchPath")
        println("""
            Provided params:
                dataset: $datasetName
                benchPath: $benchPathStr
                
        """.trimIndent())

        val datasetContext = datasetName.toDatasetContext()
        val benchDir = Path.of(benchPathStr)

        Files.createDirectories(benchDir)

        println("""
            Effective benchmark params:
                dataset: $datasetName
                benchPath: ${benchDir.toAbsolutePath()}
                
        """.trimIndent())

        val archives = datasetContext.downloadDatasetArchives(benchDir)
        archives.forEach { archive ->
            archive.extractTo(benchDir)
        }
    }
}

fun VectorDatasetInfo.downloadDatasetArchives(targetDir: Path): List<DownloadedArchive> = buildList {
    archives.forEach { (archiveName, fileInside) ->
        val archiveFile = datasetSource.downloadArchive(archiveName, targetDir)
        add(DownloadedArchive(archiveFile, fileInside))
    }
}

fun requireParam(name: String): String {
    val value = System.getProperty(name)
    require(!value.isNullOrBlank()) { "The required param '$name' is missing" }
    return value
}

fun String.toDistance(): Distance = when (this.lowercase().trim()) {
    "l2" -> Distance.L2
    "ip" -> Distance.DOT
    else -> throw IllegalArgumentException("$this distance is not supported")
}
