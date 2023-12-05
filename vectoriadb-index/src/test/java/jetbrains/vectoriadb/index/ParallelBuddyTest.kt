package jetbrains.vectoriadb.index

import org.junit.Test
import kotlin.math.min

class ParallelBuddyTest {

    @Test
    fun run() {
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

            pBuddy.run(
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

            pBuddy.run(
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