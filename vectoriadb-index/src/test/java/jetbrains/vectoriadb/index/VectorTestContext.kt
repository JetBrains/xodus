package jetbrains.vectoriadb.index

import java.lang.foreign.Arena

class ParallelVectorTestContext(
    datasetContext: VectorDatasetContext,
    val pBuddy: ParallelBuddy,
    val arena: Arena,
    val progressTracker: ProgressTracker,
): VectorDatasetContext(
    datasetContext.vectors,
    datasetContext.vectorReader,
    datasetContext.numVectors,
    datasetContext.dimensions,
)

class VectorTestContext(
    datasetContext: VectorDatasetContext,
    val progressTracker: ProgressTracker,
): VectorDatasetContext(
    datasetContext.vectors,
    datasetContext.vectorReader,
    datasetContext.numVectors,
    datasetContext.dimensions,
)

open class VectorDatasetContext(
    val vectors: Array<FloatArray>,
    val vectorReader: VectorReader,
    val numVectors: Int = vectorReader.size(),
    val dimensions: Int = vectors[0].size
)

fun vectorTest(datasetBuilder: VectorDataset, test: VectorTestContext.() -> Unit) {
    val progressTracker = ConsolePeriodicProgressTracker(1)
    progressTracker.start("test")

    val datasetContext = datasetBuilder.build()
    val testContext = VectorTestContext(datasetContext, progressTracker)
    testContext.test()
}

fun parallelVectorTest(datasetBuilder: VectorDataset, numWorkers: Int = ParallelExecution.availableCores(), test: ParallelVectorTestContext.() -> Unit) {
    val progressTracker = ConsolePeriodicProgressTracker(1)
    progressTracker.start("test")

    val datasetContext = datasetBuilder.build()
    ParallelBuddy(numWorkers, "test").use { pBuddy ->
        Arena.ofShared().use { arena ->
            val testContext = ParallelVectorTestContext(
                datasetContext,
                pBuddy,
                arena,
                progressTracker
            )
            testContext.test()
        }
    }
}
