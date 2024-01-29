package jetbrains.vectoriadb.index.bench

import jetbrains.vectoriadb.index.Distance
import jetbrains.vectoriadb.index.IndexBuilder
import jetbrains.vectoriadb.index.IndexReader
import jetbrains.vectoriadb.index.diskcache.DiskCache
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.file.Files
import java.nio.file.Path
import kotlin.time.Duration
import kotlin.time.measureTimedValue

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
        val recallCountStr = System.getProperty("recallCount")

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
        val recallCount = recallCountStr?.toIntOrNull() ?: neighbourCount

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
                recallCount: $recallCount
                               
        """.trimIndent())

        require(recallCount <= neighbourCount) { "recallCount: $recallCount must be less or equal to $neighbourCount" }

        datasetContext.runBench(
            benchPath,
            distance,
            indexName,
            indexPath,
            neighbourCount,
            cacheSize = (cacheSizeGb * 1024 * 1024 * 1024).toLong(),
            doWarmingUp,
            repeatTimes,
            recallCount
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
    repeatTimes: Int,
    recallCount: Int
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
            var avgTimeOverall = Duration.ZERO
            repeat(repeatTimes) {
                val recall = IntArray(recallCount)
                var totalDuration = Duration.ZERO

                for (queryIdx in queryVectors.indices) {
                    val queryVector = queryVectors[queryIdx]
                    val (foundNearestVectors, duration) = measureTimedValue {
                        indexReader.nearest(queryVector, recallCount)
                    }
                    totalDuration += duration

                    val foundNearestVectorsId = foundNearestVectors.map { it.toVectorId() }
                    val groundTruthNearestVectorId = groundTruth[queryIdx][0]
                    var found = false
                    repeat(recallCount) { i ->
                        if (foundNearestVectorsId[i] == groundTruthNearestVectorId) {
                            found = true
                        }
                        if (found) {
                            recall[i]++
                        }
                    }
                }

                val recallPercentage = recall.map { it * 100.0 / queryVectors.size }
                val avgTime = totalDuration / queryVectors.size
                avgTimeOverall += avgTime

                val recallStr = buildString {
                    repeat(recallCount) { i ->
                        if (i > 0) {
                            append(", ")
                        }
                        append("recall@${i + 1}: ${recallPercentage[i]}%")
                    }
                }
                println("Avg. query time : $avgTime, $recallStr, cache hits ${indexReader.hits()}%")
            }
            println("Avg. query time overall: ${avgTimeOverall / repeatTimes}")
        }
    }
}

fun ByteArray.toVectorId(): Int = ByteBuffer.wrap(this).order(ByteOrder.LITTLE_ENDIAN).getInt()