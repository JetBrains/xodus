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
    datasetContext.maxInnerProduct
)

class VectorTestContext(
    datasetContext: VectorDatasetContext,
    val progressTracker: ProgressTracker,
): VectorDatasetContext(
    datasetContext.vectors,
    datasetContext.vectorReader,
    datasetContext.numVectors,
    datasetContext.dimensions,
    datasetContext.maxInnerProduct
)

open class VectorDatasetContext(
    val vectors: Array<FloatArray>,
    val vectorReader: VectorReader,
    val numVectors: Int = vectorReader.size(),
    val dimensions: Int = vectors[0].size,
    // we use it to calculate the Silhouette Coefficient using DotDistanceFunction
    val maxInnerProduct: Float
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
