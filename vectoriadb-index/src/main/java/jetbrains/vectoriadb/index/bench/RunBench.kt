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
            println()

            val queryCount = queryVectors.size
            val avgLatency = Array(6) { Duration.ZERO }
            repeat(repeatTimes) { iteration ->
                val recall = Recall(recallCount, queryCount)
                var totalDuration = Duration.ZERO
                val latency = Latency(queryCount)
                for (queryIdx in queryVectors.indices) {
                    val queryVector = queryVectors[queryIdx]
                    val (foundNearestVectors, duration) = measureTimedValue {
                        indexReader.nearest(queryVector, recallCount)
                    }
                    latency.set(queryIdx, duration)
                    totalDuration += duration

                    val foundNearestVectorsId = foundNearestVectors.map { it.toVectorId() }
                    val groundTruthNearestVectorId = groundTruth[queryIdx][0]

                    for (i in 0 until recallCount) {
                        if (foundNearestVectorsId[i] == groundTruthNearestVectorId) {
                            recall.match(i)
                            break
                        }
                    }
                }

                latency.sort()
                val report = buildString {
                    appendLine("Iteration ${iteration + 1}")
                    appendLine("Cache hits: ${indexReader.hits()}%")
                    appendLine("Latency:")
                    appendLine("    50%  : ${latency.get(0.5)}")
                    appendLine("    90%  : ${latency.get(0.9)}")
                    appendLine("    95%  : ${latency.get(0.95)}")
                    appendLine("    99%  : ${latency.get(0.99)}")
                    appendLine("    99.9%: ${latency.get(0.999)}")
                    appendLine("    100% : ${latency.get(1.0)}")
                    recall.printResult(this)
                }
                println(report)
                avgLatency[0] += latency.get(0.5)
                avgLatency[1] += latency.get(0.9)
                avgLatency[2] += latency.get(0.95)
                avgLatency[3] += latency.get(0.99)
                avgLatency[4] += latency.get(0.999)
                avgLatency[5] += latency.get(1.0)
            }
            println("""
                Average latency:
                    50%  : ${avgLatency[0] / repeatTimes}
                    90%  : ${avgLatency[1] / repeatTimes}
                    95%  : ${avgLatency[2] / repeatTimes}
                    99%  : ${avgLatency[3] / repeatTimes}
                    99.9%: ${avgLatency[4] / repeatTimes}
                    100% : ${avgLatency[5] / repeatTimes}
            """.trimIndent())
        }
    }
}

class Latency(
    private val queryCount: Int
) {
    private val durations = Array(queryCount) { Duration.ZERO }

    fun get(percentile: Double): Duration {
        return durations[minOf((queryCount * percentile).toInt(), queryCount - 1)]
    }

    fun set(i: Int, duration: Duration) {
        durations[i] = duration
    }

    fun sort() {
        durations.sort()
    }
}

class Recall(
    val recallCount: Int,
    val totalRequestCount: Int
) {
    private val nums = IntArray(recallCount)

    fun match(idx: Int) {
        for (i in idx until recallCount) {
            nums[i]++
        }
    }

    fun printResult(builder: StringBuilder) {
        builder.appendLine("Recall:")
        repeat(recallCount) { i ->
            builder.appendLine("    @${i + 1}: ${nums[i] * 100.0 / totalRequestCount}%")
        }
    }
}

fun ByteArray.toVectorId(): Int = ByteBuffer.wrap(this).order(ByteOrder.LITTLE_ENDIAN).getInt()