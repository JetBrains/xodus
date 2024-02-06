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

import org.junit.Test
import kotlin.math.min

class ParallelBuddyTest {

    @Test
    fun run() {
        val numWorkers = 10
        ParallelBuddy(numWorkers, "test").use { pBuddy ->
            val totalSize = 999

            val data = IntArray(totalSize * numWorkers)

            repeat(numWorkers) { workerIdx ->
                for (itemIdx in workerIdx * totalSize until (workerIdx + 1) * totalSize) {
                    data[itemIdx] = workerIdx + 1
                }
            }

            val resultPerWorker = IntArray(numWorkers)
            val progressTracker = NoOpProgressTracker()

            pBuddy.run(
                "test",
                totalSize,
                progressTracker
            ) { workerIdx, itemIdx ->
                resultPerWorker[workerIdx] += data[workerIdx * totalSize + itemIdx]
            }

            repeat(numWorkers) { workerIdx ->
                var expectedResult = 0
                for (itemIdx in workerIdx * totalSize until (workerIdx + 1) * totalSize) {
                    expectedResult += data[itemIdx]
                }
                assert(resultPerWorker[workerIdx] == expectedResult)
            }
        }
    }

    @Test
    fun runSplitEvenly() {
        val numWorkers = 10
        ParallelBuddy(numWorkers, "test").use { pBuddy ->
            val totalSize = 999
            val assignmentSize = ParallelExecution.assignmentSize(totalSize, numWorkers)

            val data = IntArray(totalSize)

            repeat(numWorkers) { workerIdx ->
                for (itemIdx in workerIdx * assignmentSize until min((workerIdx + 1) * assignmentSize, totalSize)) {
                    data[itemIdx] = workerIdx + 1
                }
            }

            val resultPerWorker = IntArray(numWorkers)
            val progressTracker = NoOpProgressTracker()

            pBuddy.runSplitEvenly(
                "test",
                totalSize,
                progressTracker
            ) { workerIdx, itemIdx ->
                resultPerWorker[workerIdx] += data[itemIdx]
            }

            repeat(numWorkers) { workerIdx ->
                val multiplier = if (workerIdx == numWorkers - 1) {
                    min(assignmentSize, totalSize - (numWorkers - 1) * assignmentSize)
                } else assignmentSize
                assert(resultPerWorker[workerIdx] == (workerIdx + 1) * multiplier)
            }

            pBuddy.runSplitEvenly(
                "test",
                totalSize,
                progressTracker,
                // clean dirty array
                { workerIdx ->
                    resultPerWorker[workerIdx] = 0
                },
                { workerIdx, itemIdx ->
                    resultPerWorker[workerIdx] += data[itemIdx]
                }
            )

            repeat(numWorkers) { workerIdx ->
                val multiplier = if (workerIdx == numWorkers - 1) {
                    min(assignmentSize, totalSize - (numWorkers - 1) * assignmentSize)
                } else assignmentSize
                assert(resultPerWorker[workerIdx] == (workerIdx + 1) * multiplier)
            }
        }
    }
}