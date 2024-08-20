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
package jetbrains.exodus.entitystore

import jetbrains.exodus.testutil.TimeProfiler
import mu.KLogging
import org.junit.Ignore
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit.MINUTES
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

class EntityIterableCacheTest : EntityStoreTestBase() {

    companion object : KLogging() {

        init {
            // Use for local experiments to change cache params
            //System.setProperty(ENTITY_ITERABLE_CACHE_SIZE, "4096")
            //System.setProperty(ENTITY_ITERABLE_CACHE_MEMORY_PERCENTAGE, "50")
            //System.setProperty(PersistentEntityStoreConfig.ENTITY_ITERABLE_CACHE_THREAD_COUNT, "20")
            //System.setProperty(PersistentEntityStoreConfig.ENTITY_ITERABLE_CACHE_COUNTS_THREAD_COUNT, "1")
        }
    }

    override fun casesThatDontNeedExplicitTxn() = arrayOf(
        "testTotalHits",
        "testCacheTransactionIsolation",
        "testStressReadPerformance",
        "testStressWritePerformance",
    )

    fun testTotalHits() {
        // Given
        val test = IssueTrackerTestCase(entityStore, projectCount = 1, userCount = 1, issueCount = 1)

        // When
        test.queryAssignedIssues() // Miss
        entityStore.waitForCachingJobs()
        test.changeIssueTitle()
        test.queryAssignedIssues() // Hit
        entityStore.waitForCachingJobs()

        // Then
        reportInLogEntityIterableCacheStats()
        assertEquals(1, entityStore.entityIterableCache.stats.totalHits)
    }

    fun testCacheTransactionIsolation() {
        // Given
        // Helpers
        fun StoreTransaction.countAllIssues() = getAll("Issue").size()
        fun StoreTransaction.createNewIssue() = newEntity("Issue")

        val store = entityStore
        store.executeInTransaction {
            it.createNewIssue()
        }

        // When
        store.executeInTransaction {
            // Store issues in cache
            it.countAllIssues()
        }
        store.waitForCachingJobs()

        val read1Start = CountDownLatch(1)
        val read2Start = CountDownLatch(1)
        val write1Finish = CountDownLatch(1)
        val write2Finish = CountDownLatch(1)

        var readCount1 = 0L
        val readThread1 = thread {
            store.executeInTransaction {
                read1Start.countDown()
                // Wait for write transaction to finish
                write1Finish.await()
                readCount1 = it.countAllIssues()
            }
        }

        var writeCount1 = 0L
        // Wait for read transaction to start
        read1Start.await()
        store.executeInTransaction {
            it.createNewIssue()
            writeCount1 = it.countAllIssues()
        }
        write1Finish.countDown()

        var readCount2 = 0L
        val readThread2 = thread {
            store.executeInTransaction {
                read2Start.countDown()
                write2Finish.await()
                readCount2 = it.countAllIssues()
            }
        }

        var writeCount2 = 0L
        read2Start.await()
        store.executeInTransaction {
            it.createNewIssue()
            it.createNewIssue()
            writeCount2 = it.countAllIssues()
        }
        write2Finish.countDown()

        readThread1.join()
        readThread2.join()

        // Then
        // Assert that number of issues in cache did not change with concurrent writes
        assertEquals(1, readCount1)
        assertEquals(2, writeCount1)
        assertEquals(2, readCount2)
        assertEquals(4, writeCount2)

        reportInLogEntityIterableCacheStats()
    }

    fun testStressReadPerformance() {
        // Given
        val testCase = IssueTrackerTestCase(entityStore, projectCount = 2, userCount = 20, issueCount = 200)
        // Uncomment to run heavy test with profiler
        //val testCase = IssueTrackerTestCase(entityStore, projectCount = 10, userCount = 100, issueCount = 10000)

        val queryCount = 1000
        val queryConcurrencyLevel = 10
        val queryDelayMillis = 1L
        val updateDelayMillis = 100L

        // When
        logger.info("Running $queryCount queries...")
        val finishedRef = AtomicBoolean(false)
        val writeThread = thread {
            while (!finishedRef.get()) {
                testCase.changeIssueAssignee()
                Thread.sleep(updateDelayMillis)
            }
        }
        val executor = Executors.newFixedThreadPool(queryConcurrencyLevel)
        val profiler = TimeProfiler("Read performance")
        repeat(queryCount) {
            executor.submit {
                profiler.profile { testCase.queryComplexList() }
                Thread.sleep(queryDelayMillis)
                profiler.profile { testCase.queryComplexRoughSize() }
            }
        }
        executor.shutdown()
        executor.awaitTermination(10, MINUTES)
        finishedRef.set(true)
        writeThread.join()

        // Then
        reportInLogEntityIterableCacheStats()
        profiler.report()

        assertHitRateToBeNotLessThan(0.5)
    }

    fun testStressWritePerformance() {
        // Given
        val testCase = IssueTrackerTestCase(entityStore, projectCount = 2, userCount = 20, issueCount = 200)
        // Uncomment to run heavy test with profiler
        //val testCase = IssueTrackerTestCase(entityStore, projectCount = 10, userCount = 100, issueCount = 10000)

        val writeCount = 1000
        val writeConcurrencyLevel = 4
        val queryDelayMillis = 10L

        // When
        logger.info("Running $writeCount writes...")
        val finishedRef = AtomicBoolean(false)
        val queryThread = thread {
            while (!finishedRef.get()) {
                testCase.queryComplexList()
                testCase.queryAssignedIssues()
                Thread.sleep(queryDelayMillis)
            }
        }
        val executor = Executors.newFixedThreadPool(writeConcurrencyLevel)
        val profiler = TimeProfiler("Write performance")
        repeat(writeCount) {
            executor.submit {
                profiler.profile {
                    entityStore.executeInTransaction {
                        val issue = testCase.queryRandomIssue()
                        testCase.changeIssueAssignee(issue)
                        testCase.changeIssueTitle(issue)
                        // Uncomment for heavier test
                        //testCase.createIssue()
                        //testCase.queryComplexList()
                    }
                }
            }
        }
        executor.shutdown()
        executor.awaitTermination(10, MINUTES)
        finishedRef.set(true)
        queryThread.join()
        entityStore.waitForCachingJobs()

        // Then
        reportInLogEntityIterableCacheStats()
        profiler.report()

        // Expected hit rate is low because of intensive concurrent writes
        assertHitRateToBeNotLessThan(0.2)
    }

    fun testStressReadWritePerformance() {
        // Given
        val testCase = IssueTrackerTestCase(entityStore, projectCount = 2, userCount = 20, issueCount = 200)
        // Uncomment to run heavy test with profiler
        // val testCase = IssueTrackerTestCase(entityStore, projectCount = 10, userCount = 100, issueCount = 10000)

        val queryCount = 1000
        val queryConcurrencyLevel = 4
        val readWriteRatio = 100

        val writeCount = queryCount / readWriteRatio
        val writeConcurrencyLevel = 4
        val writeDelayMillis = 100L

        // When
        logger.info("Running $writeCount writes and $queryCount reads...")

        val queryExecutor = Executors.newFixedThreadPool(queryConcurrencyLevel)
        val readProfiler = TimeProfiler("Read performance")
        repeat(queryCount) { index ->
            queryExecutor.submit {
                readProfiler.profile {
                    if (index % 2 == 0) {
                        testCase.queryComplexList()
                    } else {
                        testCase.queryAssignedIssues()
                    }
                }
            }
        }

        val writeExecutor = Executors.newFixedThreadPool(writeConcurrencyLevel)
        val writeProfiler = TimeProfiler("Write performance")
        repeat(writeCount) {
            writeExecutor.submit {
                Thread.sleep(writeDelayMillis)
                writeProfiler.profile {
                    entityStore.executeInTransaction {
                        val issue = testCase.queryRandomIssue()
                        testCase.changeIssueAssignee(issue)
                        testCase.changeIssueTitle(issue)
                    }
                }
            }
        }
        writeExecutor.shutdown()
        writeExecutor.awaitTermination(10, MINUTES)
        queryExecutor.shutdown()
        queryExecutor.awaitTermination(10, MINUTES)
        entityStore.waitForCachingJobs()

        // Then
        reportInLogEntityIterableCacheStats()
        readProfiler.report()
        writeProfiler.report()

        // Expected hit rate is low because of intensive concurrent writes
        assertHitRateToBeNotLessThan(0.3)
    }

    // Helpers
    private fun assertHitRateToBeNotLessThan(expectedHitRate: Double) {
        val actualHitRate = entityStore.entityIterableCache.stats.hitRate
        println("Actual hitRate: $actualHitRate")
        assertTrue(
            "hitRate should be more or equal to $expectedHitRate, but was $actualHitRate",
            actualHitRate >= expectedHitRate
        )
    }

    private fun PersistentEntityStoreImpl.waitForCachingJobs() {
        entityIterableCache.processor.waitForJobs(5)
        entityIterableCache.countsProcessor.waitForJobs(5)
    }
}