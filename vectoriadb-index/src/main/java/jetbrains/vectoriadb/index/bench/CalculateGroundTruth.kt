package jetbrains.vectoriadb.index.bench

import jetbrains.vectoriadb.index.*
import jetbrains.vectoriadb.index.util.collections.BoundedGreedyVertexPriorityQueue
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import kotlin.math.min

@Suppress("unused")
class CalculateGroundTruth {
    fun main() {
        val datasetName = requireParam("dataset")
        val benchPathStr = requireParam("benchPath")
        val distanceStr = requireParam("distance")
        val neighbourCountStr = System.getProperty("neighbourCount")

        println("""
            Provided params:
                dataset: $datasetName
                benchPath: $benchPathStr
                distance: $distanceStr
                neighbourCount: $neighbourCountStr
                
        """.trimIndent())

        val distance = distanceStr.toDistance()
        val datasetInfo = datasetName.toDatasetContext()
        val benchPath = Path.of(benchPathStr)
        val neighbourCount = neighbourCountStr.toIntOrNull() ?: GenerateGroundTruthBigANNBench.NEIGHBOURS_COUNT

        println("""
            Effective benchmark params:
                dataset: $datasetName
                benchPath: ${benchPath.toAbsolutePath()}
                distance: $distanceStr
                neighbourCount: $neighbourCount
                
        """.trimIndent())

        datasetInfo.calculateGroundTruth(benchPath, distance, neighbourCount)
    }
}

fun VectorDatasetContext.calculateGroundTruth(benchPath: Path, distance: Distance, neighbourCount: Int) {
    val distanceFun = distance.buildDistanceFunction()
    val groundTruthFile = groundTruthFile(distance)
    val groundTruthFilePath = benchPath.resolve(groundTruthFile)
    if (Files.exists(groundTruthFilePath)) {
        println("$groundTruthFilePath is already calculated")
        return
    }

    val dataFilePath = benchPath.resolve(baseFile)
    check(Files.exists(dataFilePath)) { "$dataFilePath not found" }

    val queryFilePath = benchPath.resolve(queryFile)
    check(Files.exists(queryFilePath)) { "$queryFilePath not found" }

    val queryVectors = readVectors(queryFilePath, vectorDimensions)

    val groundTruth = Array(queryVectors.size) { IntArray(neighbourCount) }

    val progressTracker = ConsolePeriodicProgressTracker(5)
    progressTracker.start("calculate-ground-truth")

    FileChannel.open(dataFilePath, StandardOpenOption.READ).use { channel ->
        ParallelBuddy("calculate-ground-truth").use { pBuddy ->
            val queryVectorsPerWorker = ParallelExecution.assignmentSize(queryVectors.size, pBuddy.numWorkers())
            val assignmentPerWorker = List(pBuddy.numWorkers()) { workerIdx ->
                val from = workerIdx * queryVectorsPerWorker
                val to = min(from + queryVectorsPerWorker, queryVectors.size)
                Pair(from, to)
            }
            val vectorBufferPerWorker = List(pBuddy.numWorkers()) { FloatArray(vectorDimensions) }
            val nearestVectorsByQueryIdx = List(queryVectors.size) { BoundedGreedyVertexPriorityQueue(neighbourCount) }
            val fileReaderPerWorker = List(pBuddy.numWorkers()) { VectorFileReader.wrapWithFileReader(channel, dataFilePath, vectorDimensions, vectorCount) }

            println("Calculating ground truth for $vectorCount vectors and ${queryVectors.size} query vectors using ${pBuddy.numWorkers()} workers...")
            pBuddy.run(
                "Calculate ground truth",
                vectorCount,
                progressTracker
            ) { workerIdx, vectorIdx ->
                val (from, to) = assignmentPerWorker[workerIdx]
                val vector = vectorBufferPerWorker[workerIdx]
                val vectorReader = fileReaderPerWorker[workerIdx]
                vectorReader.read(vectorIdx, vector)

                for (queryIdx in from until to) {
                    val q = queryVectors[queryIdx]
                    val nearestVectors = nearestVectorsByQueryIdx[queryIdx]

                    distanceFun.preProcess(vector, vector)
                    distanceFun.preProcess(q, q)

                    val dist = distanceFun.computeDistance(vector, 0, q, 0, vectorDimensions)
                    nearestVectors.add(vectorIdx, dist, false, false)
                }
            }

            for (queryIdx in queryVectors.indices) {
                nearestVectorsByQueryIdx[queryIdx].vertexIndices(groundTruth[queryIdx], neighbourCount)
            }
        }
    }

    println("Writing ground truth...")
    IvecsFileWriter(groundTruthFilePath, neighbourCount).use { fileWriter ->
        for (neighbours in groundTruth) {
            fileWriter.write(neighbours)
        }
    }

    println("Done. Ground truth stored in $groundTruthFilePath")
}