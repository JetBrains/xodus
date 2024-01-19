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

import mu.KLogger
import mu.KLogging
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit.MINUTES
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

class EntityIterableCacheTest : EntityStoreTestBase() {

    companion object : KLogging() {
        private val kRandom = kotlin.random.Random(0)
        private val random = java.util.Random(0)
        private val testData = TestData(kRandom)

        // Settings for local dev testing
        private val logIssueCreated = false
        private val logQueryResult = false

        init {
            // Use for local experiments to change cache params
            //System.setProperty(ENTITY_ITERABLE_CACHE_SIZE, "8096")
            //System.setProperty(ENTITY_ITERABLE_CACHE_MEMORY_PERCENTAGE, "50")
        }
    }

    override fun casesThatDontNeedExplicitTxn() = arrayOf(
        "testHitCount",
        "testHitRate",
        "testCacheTransactionIsolation"
    )

    fun testTotalHits() {
        // Given
        val store = entityStore
        val projects = Project.createMany(1, store)
        val users = User.createMany(1, store)
        val issues = Issue.createMany(1, projects, users, store)
        val test = TestCase(store, projects, users, issues)

        // When
        test.queryAssignedIssues() // Miss
        store.waitForCacheJobs()
        store.executeInTransaction { Issue.changeTitle(issues[0]) }
        test.queryAssignedIssues() // Hit
        store.waitForCacheJobs()

        // Then
        reportInLogEntityIterableCacheStats()
        assertEquals(1, store.entityIterableCache.stats.totalHits)
    }

    fun testStressReadPerformance() {
        // Given
        val projectCount = 2
        val userCount = 20
        val issueCount = 1000
        val queryCount = 10000
        val queryConcurrencyLevel = 10
        val queryDelayMillis = 1L

        val updateConcurrently = true
        val updateDelayMillis = 100L

        val store = entityStore
        val projects = Project.createMany(projectCount, store)
        val users = User.createMany(userCount, store)
        val issues = Issue.createMany(issueCount, projects, users, store)
        val test = TestCase(store, projects, users, issues)

        // When
        logger.info("Running $queryCount queries...")
        val finishedRef = AtomicBoolean(false)
        val updateProcess = if (updateConcurrently) {
            thread {
                while (!finishedRef.get()) {
                    test.changeIssueAssignee()
                    Thread.sleep(updateDelayMillis)
                }
            }
        } else null
        val executor = Executors.newFixedThreadPool(queryConcurrencyLevel)
        repeat(queryCount) {
            executor.submit {
                test.queryComplexList()
                Thread.sleep(queryDelayMillis)
                test.queryComplexRoughSize()
            }
        }
        executor.shutdown()
        executor.awaitTermination(10, MINUTES)
        finishedRef.set(true)
        updateProcess?.join()

        // Then
        reportInLogEntityIterableCacheStats()
        assertHitRateToBeNotLessThan(0.5)
    }

    fun testStressWritePerformance() {
        // Given
        // Use these params to experiment with cache locally
        val projectCount = 2
        val userCount = 20
        val issueCount = 1000
        val writeCount = 10000

        val queryConcurrently = true
        val queryDelayMillis = 100L

        val store = entityStore
        val projects = Project.createMany(projectCount, store)
        val users = User.createMany(userCount, store)
        val issues = Issue.createMany(issueCount, projects, users, store)
        val test = TestCase(store, projects, users, issues)

        // When
        logger.info("Running $writeCount writes...")
        val finishedRef = AtomicBoolean(false)
        val queryThread = if (queryConcurrently) {
            thread {
                while (!finishedRef.get()) {
                    test.queryComplexList()
                    test.queryAssignedIssues()
                    Thread.sleep(queryDelayMillis)
                }
            }
        } else null
        repeat(writeCount) {
            test.changeIssueAssignee()
            test.changeIssueTitle()
            test.queryComplexList()
            if (it % 1000 == 0) {
                println("Progress: $it/$writeCount")
            }
        }
        finishedRef.set(true)
        queryThread?.join()

        // Then
        reportInLogEntityIterableCacheStats()
        // Expected hit rate is low because of intensive concurrent writes
        assertHitRateToBeNotLessThan(0.3)
    }

    fun testCacheTransactionIsolation() {
        // Given
        val store = entityStore
        store.executeInTransaction {
            it.createNewIssue()
        }

        // When
        store.executeInTransaction {
            // Store issues in cache
            it.countAllIssues()
        }
        store.waitForCacheJobs()

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
        assertHitRateToBeNotLessThan(0.5)
    }

    private fun assertHitRateToBeNotLessThan(expectedHitRate: Double) {
        val actualHitRate = entityStore.entityIterableCache.stats.hitRate
        println("Actual hitRate: $actualHitRate")
        assertTrue(
            "hitRate should be more or equal to $expectedHitRate, but was $actualHitRate",
            actualHitRate >= expectedHitRate
        )
    }

    data class TestCase(
        val store: PersistentEntityStoreImpl,
        val projects: List<Entity>,
        val users: List<Entity>,
        val issues: List<Entity>
    ) {

        companion object : KLogging()

        fun queryAssignedIssues() {
            val assignee = users.random(kRandom)
            store.executeInTransaction { tx ->
                val result = tx.findLinks("Issue", assignee, "assignee").toList()
                logger.logQueryResult("[1] Found ${result.size} issues assigned to ${assignee.getProperty("name")}")
            }
        }

        fun queryComplexList() {
            val project = projects.randomGaussian(random)
            val assignee = users.randomGaussian(random)
            val state = IssueState.entries.randomGaussian(random)
            val sortAscending = testData.boolean()
            store.executeInTransaction { tx ->
                val result = tx.findLinks("Issue", project, "project")
                    .intersect(tx.findLinks("Issue", assignee, "assignee"))
                    .intersect(tx.find("Issue", "state", state.name))
                    .intersect(tx.sort("Issue", "createdAt", sortAscending))
                    .toList()
                logger.logQueryResult(
                    "Found ${result.size} issues" +
                            "\n in project ${project.getProperty("name")}" +
                            "\n assigned to ${assignee.getProperty("name")}" +
                            "\n in state $state" +
                            "\n sorted by createdAt ${if (sortAscending) "ascending" else "descending"}"
                )
            }
        }

        fun queryComplexRoughSize() {
            val project = projects.random(kRandom)
            val assignee = users.random(kRandom)
            val state = IssueState.entries.random(kRandom)
            val sortAscending = testData.boolean()
            store.executeInTransaction { tx ->
                val roughSize = tx.findLinks("Issue", project, "project")
                    .intersect(tx.findLinks("Issue", assignee, "assignee"))
                    .intersect(tx.find("Issue", "state", state.name))
                    .intersect(tx.sort("Issue", "createdAt", sortAscending))
                    .roughSize
                logger.logQueryResult(
                    "Found $roughSize issues roughly" +
                            "\n in project ${project.getProperty("name")}" +
                            "\n assigned to ${assignee.getProperty("name")}" +
                            "\n in state $state" +
                            "\n sorted by createdAt ${if (sortAscending) "ascending" else "descending"}"
                )
            }
        }


        fun changeIssueAssignee() {
            val issue = issues.randomGaussian(random)
            val assignee = users.randomGaussian(random)
            store.executeInTransaction { tx ->
                issue.setLink("assignee", assignee)
            }
        }

        fun changeIssueTitle() {
            val issue = issues.random(kRandom)
            val title = testData.chuckNorrisFact()
            store.executeInTransaction { tx ->
                issue.setProperty("title", title)
            }
        }

        fun KLogger.logQueryResult(message: String) {
            if (logQueryResult) info(message)
        }


    }

    // Projects
    data class Project(
        val name: String = testData.programmingLanguageCreator()
    ) {

        companion object {
            fun createMany(count: Int, store: PersistentEntityStore): List<Entity> {
                logger.info("Creating $count projects...")
                return (1..count).map {
                    Project().create(store)
                }
            }
        }

        fun create(store: PersistentEntityStore): Entity = store.computeInTransaction { tx ->
            val project = tx.newEntity("Project")
            project.setProperty("name", name)
            project
        }
    }

    // Users
    data class User(
        val name: String = testData.programmingLanguageCreator()
    ) {

        companion object {

            fun createMany(count: Int, store: PersistentEntityStore): List<Entity> {
                logger.info("Creating $count users...")
                return (1..count).map {
                    User().create(store)
                }
            }
        }

        fun create(store: PersistentEntityStore): Entity = store.computeInTransaction { tx ->
            val user = tx.newEntity("User")
            user.setProperty("name", name)
            user
        }
    }

    // Issues
    enum class IssueState { Open, InProgress, Resolved, Verified, Closed }

    enum class IssuePriority { Low, Normal, High, Critical, Blocker }

    data class Issue(
        // Data
        val title: String = testData.chuckNorrisFact(),
        val summary: String = testData.rickAndMortyQuote(),
        val state: IssueState = IssueState.entries.random(kRandom),
        val priority: IssuePriority = IssuePriority.entries.random(kRandom),
        val createdAt: Long = testData.pastDateUpToDays(100).time,

        // Links
        val project: Entity,
        val reporter: Entity,
        val assignee: Entity
    ) {

        companion object {

            fun createMany(
                count: Int, projects: List<Entity>,
                users: List<Entity>, store: PersistentEntityStore
            ): List<Entity> {
                logger.info("Creating $count issues...")
                return (1..count).map {
                    val project = projects.random(kRandom)
                    val reporter = users.random(kRandom)
                    val assignee = users.random(kRandom)

                    val issue = Issue(project = project, reporter = reporter, assignee = assignee)
                    issue.create(store)
                }
            }

            fun changeTitle(issue: Entity) {
                issue.setProperty("title", testData.chuckNorrisFact())
            }
        }

        fun create(store: PersistentEntityStore): Entity = store.computeInTransaction { tx ->
            val issue = tx.newEntity("Issue")
            issue.setProperty("title", title)
            issue.setProperty("summary", summary)
            issue.setProperty("state", state.name)
            issue.setProperty("priority", priority.name)
            issue.setLink("project", project)
            issue.setLink("reporter", reporter)
            issue.setLink("assignee", assignee)
            if (logIssueCreated) logger.info("Created issue: $issue, $title")
            issue
        }
    }

    // Helpers
    fun StoreTransaction.countAllIssues(): Long {
        return getAll("Issue").size()
    }

    fun StoreTransaction.createNewIssue(): Entity {
        return newEntity("Issue")
    }

    private fun PersistentEntityStoreImpl.waitForCacheJobs() {
        entityIterableCache.processor.waitForJobs(5)
    }
}