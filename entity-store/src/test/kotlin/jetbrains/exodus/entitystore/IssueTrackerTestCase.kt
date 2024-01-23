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
import java.util.*

class IssueTrackerTestCase(
    // Required
    val store: PersistentEntityStoreImpl,
    projectCount: Int,
    userCount: Int,
    issueCount: Int,
    // Default
    randomSeed: Long = 0,
    val random: Random = Random(randomSeed),
    val kRandom: kotlin.random.Random = kotlin.random.Random(randomSeed),
    val testData: TestData = TestData(kRandom),
    val projects: List<Entity> = Project.createEntities(testData, projectCount, store),
    val users: List<Entity> = User.createEntities(testData, userCount, store),
    val issues: List<Entity> = Issue.createEntities(testData, issueCount, projects, users, store),
    val logQueryResult: Boolean = false,
) {

    companion object : KLogging()

    // Queries
    fun queryAssignedIssues() {
        val assignee = users.random(kRandom)
        store.executeInTransaction { tx ->
            val result = tx.findLinks("jetbrains.exodus.entitystore.Issue", assignee, "assignee").toList()
            logger.logQueryResult("[1] Found ${result.size} issues assigned to ${assignee.getProperty("name")}")
        }
    }

    fun queryComplexList() {
        val project = projects.randomGaussian(random)
        val assignee = users.randomGaussian(random)
        val state = IssueState.entries.randomGaussian(random)
        val sortAscending = testData.boolean()
        store.executeInTransaction { tx ->
            val result = tx.findLinks("jetbrains.exodus.entitystore.Issue", project, "project")
                .intersect(tx.findLinks("jetbrains.exodus.entitystore.Issue", assignee, "assignee"))
                .intersect(tx.find("jetbrains.exodus.entitystore.Issue", "state", state.name))
                .intersect(tx.sort("jetbrains.exodus.entitystore.Issue", "createdAt", sortAscending))
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
            val roughSize = tx.findLinks("jetbrains.exodus.entitystore.Issue", project, "project")
                .intersect(tx.findLinks("jetbrains.exodus.entitystore.Issue", assignee, "assignee"))
                .intersect(tx.find("jetbrains.exodus.entitystore.Issue", "state", state.name))
                .intersect(tx.sort("jetbrains.exodus.entitystore.Issue", "createdAt", sortAscending))
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

    // Writes
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

    private fun KLogger.logQueryResult(message: String) {
        if (logQueryResult) {
            info(message)
        }
    }
}

// Projects
data class Project(val name: String) {

    companion object {
        fun create(testData: TestData): Project {
            val name = testData.programmingLanguageCreator()
            return Project(name)
        }

        fun createEntities(testData: TestData, count: Int, store: PersistentEntityStore): List<Entity> {
            EntityIterableCacheTest.logger.info("Creating $count projects...")
            return store.computeInTransaction {
                (1..count).map {
                    val project = create(testData)
                    project.createNewEntity(store)
                }
            }
        }
    }

    fun createNewEntity(store: PersistentEntityStore): Entity {
        return store.computeInTransaction { tx ->
            val project = tx.newEntity("jetbrains.exodus.entitystore.Project")
            project.setProperty("name", name)
            project
        }
    }
}

// Users
data class User(val name: String) {

    companion object {

        fun createEntities(testData: TestData, count: Int, store: PersistentEntityStore): List<Entity> {
            EntityIterableCacheTest.logger.info("Creating $count users...")
            return store.computeInTransaction {
                (1..count).map {
                    val user = create(testData)
                    user.createNewEntity(store)
                }
            }
        }

        fun create(testData: TestData): User {
            val name = testData.programmingLanguageCreator()
            return User(name)
        }
    }

    fun createNewEntity(store: PersistentEntityStore): Entity {
        return store.computeInTransaction { tx ->
            val user = tx.newEntity("User")
            user.setProperty("name", name)
            user
        }
    }
}

// Issues
enum class IssueState { Open, InProgress, Resolved, Verified, Closed }

enum class IssuePriority { Low, Normal, High, Critical, Blocker }

data class Issue(
    // Data
    val title: String,
    val summary: String,
    val state: IssueState,
    val priority: IssuePriority,
    val createdAt: Long,

    // Links
    val project: Entity,
    val reporter: Entity,
    val assignee: Entity
) {

    companion object {

        fun create(
            testData: TestData,
            projects: List<Entity>,
            users: List<Entity>
        ): Issue {
            val kRandom = testData.kRandom

            val title: String = testData.chuckNorrisFact()
            val summary: String = testData.rickAndMortyQuote()
            val state: IssueState = IssueState.entries.random(kRandom)
            val priority: IssuePriority = IssuePriority.entries.random(kRandom)
            val createdAt: Long = testData.pastDateUpToDays(100).time

            val project = projects.random(kRandom)
            val reporter = users.random(kRandom)
            val assignee = users.random(kRandom)

            return Issue(title, summary, state, priority, createdAt, project, reporter, assignee)
        }

        fun createEntities(
            testData: TestData,
            count: Int,
            projects: List<Entity>,
            users: List<Entity>,
            store: PersistentEntityStore
        ): List<Entity> {
            EntityIterableCacheTest.logger.info("Creating $count issues...")
            return store.computeInTransaction {
                (1..count).map {
                    val issue = create(testData, projects, users)
                    issue.createNewEntity(store)
                }
            }
        }
    }

    fun createNewEntity(store: PersistentEntityStore): Entity {
        return store.computeInTransaction { tx ->
            val issue = tx.newEntity("jetbrains.exodus.entitystore.Issue")
            issue.setProperty("title", title)
            issue.setProperty("summary", summary)
            issue.setProperty("state", state.name)
            issue.setProperty("priority", priority.name)
            issue.setLink("project", project)
            issue.setLink("reporter", reporter)
            issue.setLink("assignee", assignee)
            issue
        }
    }
}