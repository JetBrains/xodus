package jetbrains.vectoriadb.index.bench

import jetbrains.vectoriadb.index.DataStore
import jetbrains.vectoriadb.index.Distance
import jetbrains.vectoriadb.index.IndexBuilder
import jetbrains.vectoriadb.index.Slf4jPeriodicProgressTracker
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.name
import kotlin.time.measureTime

@Suppress("unused")
class BuildIndex {
    fun main() {
        val datasetName = requireParam("dataset")
        val benchPathStr = requireParam("benchPath")
        val distanceStr = requireParam("distance")
        var indexName = System.getProperty("indexName")
        val graphPartitionMemoryConsumptionGbStr = System.getProperty("graphPartitionMemoryConsumptionGb")

        println("""
            Provided params:
                dataset: $datasetName
                benchPath: $benchPathStr
                distance: $distanceStr
                indexName: $indexName
                graphPartitionMemoryConsumptionGb: $graphPartitionMemoryConsumptionGbStr
                
        """.trimIndent())

        val distance = distanceStr.toDistance()
        val datasetContext = datasetName.toDatasetContext()
        val benchPath = Path.of(benchPathStr)
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
                
        """.trimIndent())

        datasetContext.buildIndex(benchPath, distance, indexName, indexPath, (graphPartitionMemoryConsumptionGb * 1024 * 1024 * 1024).toLong())
    }
}

fun VectorDatasetInfo.buildIndex(
    benchPath: Path,
    distance: Distance,
    indexName: String,
    indexPath: Path,
    graphPartitionMemoryConsumption: Long
) {
    val distanceFun = distance.buildDistanceFunction()

    val vectorFilePath = benchPath.resolve(baseFile)
    check(Files.exists(vectorFilePath)) { "$vectorFilePath not found" }

    if (Files.exists(indexPath)) {
        println("$indexPath already exists")
        return
    }
    Files.createDirectory(indexPath)

    val dataFilePath = DataStore.dataLocation(dataFile, benchPath)
    if (!Files.exists(dataFilePath)) {
        val progressTracker = Slf4jPeriodicProgressTracker(1)
        progressTracker.start("prepare vector file")
        progressTracker.pushPhase(dataFilePath.name)
        VectorFileReader.openFileReader(vectorFilePath, vectorDimensions, vectorCount).use { vectorReader ->
            DataStore.create(dataFile, vectorDimensions, distanceFun, benchPath).use { dataBuilder ->
                for (vectorIdx in 0 until vectorCount) {
                    val vector = vectorReader.read(vectorIdx)
                    val vectorId = vectorReader.readId(vectorIdx)
                    dataBuilder.add(vector, vectorId)
                    progressTracker.progress((vectorIdx.toDouble() / vectorCount) * 100)
                }
            }
        }
        progressTracker.pullPhase()
        progressTracker.finish()
    }

    val indexBuiltIn = measureTime {
        IndexBuilder.buildIndex(
            indexName,
            vectorDimensions,
            indexPath,
            dataFilePath,
            graphPartitionMemoryConsumption,
            distance,
            Slf4jPeriodicProgressTracker(5)
        )
    }

    println("Index built in $indexBuiltIn")
}