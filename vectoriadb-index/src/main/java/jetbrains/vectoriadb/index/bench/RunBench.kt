package jetbrains.vectoriadb.index.bench

import jetbrains.vectoriadb.index.*
import jetbrains.vectoriadb.index.diskcache.DiskCache
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.file.Files
import java.nio.file.Path
import kotlin.time.measureTime

@Suppress("unused")
class RunBench {
    fun main() {
        val datasetName = requireParam("dataset")
        val benchPathStr = requireParam("benchPath")
        val distanceStr = requireParam("distance")
        var indexName = System.getProperty("indexName")
        val neighbourCountStr = System.getProperty("neighbourCount")
        val cacheSizeGbStr = System.getProperty("cacheSizeGb")
        val doWarmingUpStr = System.getProperty("doWarmingUp")
        val repeatTimesStr = System.getProperty("repeatTimes")

        println("""
            Provided params:
                dataset: $datasetName
                benchPath: $benchPathStr
                distance: $distanceStr
                indexName: $indexName
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

        println("""
            Effective benchmark params:
                dataset: $datasetName
                benchPath: ${benchPath.toAbsolutePath()}
                distance: $distanceStr
                indexName: $indexName
                indexPath: ${indexPath.toAbsolutePath()}
                neighbourCount: $neighbourCount
                cacheSizeGb: $cacheSizeGb
                doWarmingUp: $doWarmingUp
                repeatTimes: $repeatTimes
                               
        """.trimIndent())

        datasetContext.runBench(
            benchPath,
            distance,
            indexName,
            indexPath,
            neighbourCount,
            cacheSize = (cacheSizeGb * 1024 * 1024 * 1024).toLong(),
            doWarmingUp,
            repeatTimes
        )
    }
}

fun VectorDatasetInfo.runBench(
    benchPath: Path,
    distance: Distance,
    indexName: String,
    indexPath: Path,
    neighbourCount: Int,
    cacheSize: Long,
    doWarmingUp: Boolean,
    repeatTimes: Int
) {
    check(Files.exists(indexPath)) { "Index directory $indexPath not found" }

    val queryFilePath = benchPath.resolve(queryFile)
    check(Files.exists(queryFilePath)) { "Query file $queryFilePath not found" }

    val groundTruthFilePath = benchPath.resolve(groundTruthFile(distance))
    check(Files.exists(groundTruthFilePath)) { "Ground truth $groundTruthFilePath not found" }

    println("Reading queries...")
    val queryVectors = readVectors(queryFilePath, vectorDimensions)
    println("${queryVectors.size} queries are read")

    println("Reading ground truth...")
    val groundTruth = readGroundTruth(groundTruthFilePath, neighbourCount)
    println("${groundTruth.size} ground truth records are read")

    DiskCache(cacheSize, vectorDimensions, IndexBuilder.DEFAULT_MAX_CONNECTIONS_PER_VERTEX).use { diskCache ->

        IndexReader(
            indexName,
            vectorDimensions,
            indexPath,
            distance,
            diskCache
        ).use { indexReader ->

            println("Searching...")

            if (doWarmingUp) {
                println("Warming up ...")

                //give GC chance to collect garbage
                Thread.sleep((5 * 1000).toLong())
                repeat(1) {
                    for (vector in queryVectors) {
                        indexReader.nearest(vector, 1)
                    }
                }
            }

            println("Benchmark ...")

            val pid = ProcessHandle.current().pid()
            println("PID: $pid")
            repeat(repeatTimes) {
                var errorsCount = 0
                val duration = measureTime {
                    for (queryIdx in queryVectors.indices) {
                        val queryVector = queryVectors[queryIdx]
                        val foundNearestVectorId = indexReader.nearest(queryVector, 1)[0].toVectorId()
                        val groundTruthNearestVectorId = groundTruth[queryIdx][0]

                        if (foundNearestVectorId != groundTruthNearestVectorId) {
                            errorsCount++
                        }
                    }
                }
                val errorPercentage = errorsCount * 100.0 / queryVectors.size

                println("Avg. query time : ${duration / queryVectors.size}, errors: ${errorPercentage}%, cache hits ${indexReader.hits()}%")
            }
        }
    }
}

fun ByteArray.toVectorId(): Int = ByteBuffer.wrap(this).order(ByteOrder.LITTLE_ENDIAN).getInt()