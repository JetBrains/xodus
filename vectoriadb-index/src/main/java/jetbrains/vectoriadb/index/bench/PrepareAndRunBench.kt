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
                               
        """.trimIndent())

        val distance = distanceStr.toDistance()
        val datasetContext = datasetName.toDatasetContext()
        val benchPath = Path.of(benchPathStr)
        val neighbourCount = neighbourCountStr.toIntOrNull() ?: GenerateGroundTruthBigANNBench.NEIGHBOURS_COUNT
        val cacheSizeGb = cacheSizeGbStr.toDoubleOrNull() ?: 1.0
        val doWarmingUp = doWarmingUpStr.toBooleanStrictOrNull() ?: false
        val repeatTimes = repeatTimesStr.toIntOrNull() ?: 20
        indexName = if (indexName.isNullOrBlank()) datasetContext.defaultIndexName(distance) else indexName
        val indexPath = benchPath.resolve(indexName)
        val graphPartitionMemoryConsumptionGb = graphPartitionMemoryConsumptionGbStr.toDoubleOrNull() ?: 1.0

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
                               
        """.trimIndent())

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

            runBench(benchPath, distance, indexName, indexPath, neighbourCount, cacheSize, doWarmingUp, repeatTimes)
        }
    }
}