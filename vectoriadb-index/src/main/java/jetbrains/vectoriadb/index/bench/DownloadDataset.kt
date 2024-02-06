/*
 * Copyright ${inceptionYear} - ${year} ${owner}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
