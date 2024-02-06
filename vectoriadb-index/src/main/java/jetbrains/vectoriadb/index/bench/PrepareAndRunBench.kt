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

import java.nio.file.Files
import java.nio.file.Path

@Suppress("unused")
class PrepareAndRunBench {
    fun main() {
        val datasetName = requireParam("dataset")
        val benchPathStr = requireParam("benchPath")
        val distanceStr = requireParam("distance")
        var indexName = System.getProperty("indexName")
        val neighbourCountStr = System.getProperty("neighbourCount")
        val graphPartitionMemoryConsumptionGbStr = System.getProperty("graphPartitionMemoryConsumptionGb")
        val cacheSizeGbStr = System.getProperty("cacheSizeGb")
        val doWarmingUpStr = System.getProperty("doWarmingUp")
        val repeatTimesStr = System.getProperty("repeatTimes")
        val recallCountStr = System.getProperty("recallCount")

        println("""
            Provided params:
                dataset: $datasetName
                benchPath: $benchPathStr
                distance: $distanceStr
                indexName: $indexName
                graphPartitionMemoryConsumptionGb: $graphPartitionMemoryConsumptionGbStr
                neighbourCount: $neighbourCountStr
                cacheSizeGb: $cacheSizeGbStr
                doWarmingUp: $doWarmingUpStr
                repeatTimes: $repeatTimesStr
                recallCount: $recallCountStr
                               
        """.trimIndent())

        val distance = distanceStr.toDistance()
        val datasetContext = datasetName.toDatasetContext()
        val benchPath = Path.of(benchPathStr)
        val neighbourCount = neighbourCountStr.toIntOrNull() ?: VectorDatasetInfo.DEFAULT_NEIGHBOURS_COUNT
        val cacheSizeGb = cacheSizeGbStr.toDoubleOrNull() ?: 1.0
        val doWarmingUp = doWarmingUpStr.toBooleanStrictOrNull() ?: false
        val repeatTimes = repeatTimesStr.toIntOrNull() ?: 20
        indexName = if (indexName.isNullOrBlank()) datasetContext.defaultIndexName(distance) else indexName
        val indexPath = benchPath.resolve(indexName)
        val graphPartitionMemoryConsumptionGb = graphPartitionMemoryConsumptionGbStr.toDoubleOrNull() ?: 1.0
        val recallCount = recallCountStr?.toIntOrNull() ?: neighbourCount

        println("""
            Effective benchmark params:
                dataset: $datasetName
                benchPath: ${benchPath.toAbsolutePath()}
                distance: $distanceStr
                indexName: $indexName
                indexPath: ${indexPath.toAbsolutePath()}
                graphPartitionMemoryConsumptionGb: $graphPartitionMemoryConsumptionGb
                neighbourCount: $neighbourCount
                cacheSizeGb: $cacheSizeGb
                doWarmingUp: $doWarmingUp
                repeatTimes: $repeatTimes
                recallCount: $recallCount
                               
        """.trimIndent())

        require(recallCount <= neighbourCount) { "recallCount: $recallCount must be less or equal to $neighbourCount" }

        val cacheSize = (cacheSizeGb * 1024 * 1024 * 1024).toLong()
        val graphPartitionMemoryConsumption = (graphPartitionMemoryConsumptionGb * 1024 * 1024 * 1024).toLong()

        with(datasetContext) {
            Files.createDirectories(benchPath)

            downloadDatasetArchives(benchPath).forEach { archive ->
                archive.extractTo(benchPath)
            }
            println()

            calculateGroundTruth(benchPath, distance, neighbourCount)
            println()

            buildIndex(benchPath, distance, indexName, indexPath, graphPartitionMemoryConsumption)
            println()

            runBench(benchPath, distance, indexName, indexPath, neighbourCount, cacheSize, doWarmingUp, repeatTimes, recallCount)
        }
    }
}