package jetbrains.exodus.entitystore

import mu.KLogger
import mu.KLogging
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit.MINUTES
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread
import kotlin.random.Random

class EntityIterableCacheTest : EntityStoreTestBase() {

    companion object : KLogging() {
        private val rnd = Random(0)
        private val testData = TestData(rnd)

        // Settings for local dev testing
        private val logIssueCreated = false
        private val logQueryResult = false
    }

    fun testHistCount() {
        // Given
        val store = getEntityStore()
        val projects = Project.createMany(1, store)
        val users = User.createMany(1, store)
        val issues = Issue.createMany(1, projects, users, store)
        val test = TestCase(store, projects, users, issues, 1)

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

    fun testHistRate() {
        // Given
        val projectCount = 2
        val userCount = 20
        val issueCount = 1000
        val queryCount = 10000
        val queryConcurrencyLevel = 10
        val queryDelayMillis = 1L

        val updateConcurrently = false
        val updateDelayMillis = 10L

        val store = getEntityStore()
        val projects = Project.createMany(projectCount, store)
        val users = User.createMany(userCount, store)
        val issues = Issue.createMany(issueCount, projects, users, store)
        val test = TestCase(store, projects, users, issues, queryCount)

        // When
        logger.info("Running $queryCount queries...")
        val finishedRef = AtomicBoolean(false)
        val updateProcess = thread {
            while (!finishedRef.get()) {
                if (updateConcurrently) {
                    test.changeIssueAssignee()
                }
                Thread.sleep(updateDelayMillis)
            }
        }
        val executor = Executors.newFixedThreadPool(queryConcurrencyLevel)
        repeat(queryCount) {
            executor.submit {
                test.queryComplex()
                Thread.sleep(queryDelayMillis)
            }
        }
        executor.shutdown()
        executor.awaitTermination(10, MINUTES)
        finishedRef.set(true)
        updateProcess.join()

        // Then
        reportInLogEntityIterableCacheStats()
        assertTrue(test.store.entityIterableCache.stats.hitRate > 0.8)
    }

    fun testCacheTransactionIsolation() {
        // Given
        val store = getEntityStore()
        store.executeInTransaction {
            it.createNewIssue()
        }

        // When
        store.executeInTransaction {
            // Store issues in cache
            it.countAllIssues()
        }
        store.waitForCacheJobs()

        val write1Finish = CountDownLatch(1)
        val write2Finish = CountDownLatch(1)

        var readCount1 = 0L
        val readThread1 = thread {
            store.executeInTransaction {
                write1Finish.await()
                readCount1 = it.countAllIssues()
            }
        }

        var writeCount1 = 0L
        store.executeInTransaction {
            it.createNewIssue()
            writeCount1 = it.countAllIssues()
        }
        write1Finish.countDown()

        var readCount2 = 0L
        val readThread2 = thread {
            store.executeInTransaction {
                write2Finish.await()
                readCount2 = it.countAllIssues()
            }
        }

        var writeCount2 = 0L
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

    data class TestCase(
        val store: PersistentEntityStoreImpl,
        val projects: List<Entity>,
        val users: List<Entity>,
        val issues: List<Entity>,
        val queryCount: Int
    ) {

        companion object : KLogging()

        fun queryAssignedIssues() {
            val assignee = users.random(rnd)
            store.executeInTransaction { tx ->
                val result = tx.findLinks("Issue", assignee, "assignee").toList()
                logger.logQueryResult("[1] Found ${result.size} issues assigned to ${assignee.getProperty("name")}")
            }
        }

        fun queryComplex() {
            val project = projects.random(rnd)
            val assignee = users.random(rnd)
            val state = IssueState.entries.random(rnd)
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

        fun changeIssueAssignee() {
            val issue = issues.random(rnd)
            val assignee = users.random(rnd)
            store.executeInTransaction { tx ->
                issue.setLink("assignee", assignee)
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
        val state: IssueState = IssueState.entries.random(rnd),
        val priority: IssuePriority = IssuePriority.entries.random(rnd),
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
                    val project = projects.random(rnd)
                    val reporter = users.random(rnd)
                    val assignee = users.random(rnd)

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