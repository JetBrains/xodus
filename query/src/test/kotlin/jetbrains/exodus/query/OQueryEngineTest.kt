package jetbrains.exodus.query

import com.google.common.truth.Truth.assertThat
import io.mockk.every
import io.mockk.mockk
import jetbrains.exodus.entitystore.PersistentEntityStoreImpl
import jetbrains.exodus.entitystore.PersistentStoreTransaction
import jetbrains.exodus.entitystore.orientdb.InMemoryOrientDB
import jetbrains.exodus.entitystore.orientdb.IssueClass
import jetbrains.exodus.entitystore.orientdb.ProjectIssues
import jetbrains.exodus.entitystore.orientdb.createIssue
import jetbrains.exodus.entitystore.orientdb.createProject
import jetbrains.exodus.entitystore.orientdb.linkIssueToProject
import jetbrains.exodus.query.metadata.ModelMetaData
import org.junit.Rule
import org.junit.Test

class OQueryEngineTest {

    @Rule
    @JvmField
    val orientDB = InMemoryOrientDB()

    @Test
    fun `should query property equal`() {
        // Given
        orientDB.createIssue("issue1")
        orientDB.createIssue("issue2")

        val engine = givenOQueryEngine()

        // When
        orientDB.withSession {
            val node = PropertyEqual("name", "issue2")
            val result = engine.query("Issue", node).toList()

            // Then
            assertThat(result.count()).isEqualTo(1)
            assertThat(result.first().getProperty("name")).isEqualTo("issue2")
        }
    }

    @Test
    fun `should query with or`() {
        // Given
        orientDB.createIssue("issue1")
        orientDB.createIssue("issue2")
        orientDB.createIssue("issue3")

        val engine = givenOQueryEngine()

        // When
        orientDB.withSession {
            val equal1 = PropertyEqual("name", "issue1")
            val equal3 = PropertyEqual("name", "issue3")
            val result = engine.query("Issue", Or(equal1, equal3))

            // Then
            assertThat(result.count()).isEqualTo(2)
            assertThat(result.first().getProperty("name")).isEqualTo("issue1")
            assertThat(result.last().getProperty("name")).isEqualTo("issue3")
        }
    }

    @Test
    fun `should query with and`() {
        // Given
        orientDB.createIssue("issue1", "normal")
        orientDB.createIssue("issue2", "normal")
        orientDB.createIssue("issue3", "high")

        val engine = givenOQueryEngine()

        // When
        orientDB.withSession {
            val nameEqual = PropertyEqual("name", "issue2")
            val projectEqual = PropertyEqual(IssueClass.PRIORITY_PROPERTY, "normal")
            val result = engine.query("Issue", And(nameEqual, projectEqual))

            // Then
            assertThat(result.count()).isEqualTo(1)
            assertThat(result.first().getProperty("name")).isEqualTo("issue2")
            assertThat(result.first().getProperty("priority")).isEqualTo("normal")
        }
    }

    @Test
    fun `should query links`() {
        // Given
        val project1 = orientDB.createProject("project1")
        val project2 = orientDB.createProject("project2")

        val issue1 = orientDB.createIssue("issue1")
        val issue2 = orientDB.createIssue("issue2")
        val issue3 = orientDB.createIssue("issue3")

        orientDB.linkIssueToProject(issue1, project1)
        orientDB.linkIssueToProject(issue2, project1)
        orientDB.linkIssueToProject(issue3, project2)


        val engine = givenOQueryEngine()

        // When
        orientDB.withSession {
            val link = LinkEqual(ProjectIssues.PROJECT_TO_ISSUES, project1)
            val issues = engine.query(IssueClass.NAME, link)

            // Then
            assertThat(issues.count()).isEqualTo(2)
            assertThat(issues.map { it.getProperty("name") }).containsExactly("issue1", "issue2")
        }
    }

    private fun givenOQueryEngine(): QueryEngine {
        val model = mockk<ModelMetaData>(relaxed = true)
        val store = mockk<PersistentEntityStoreImpl>(relaxed = true)
        every { store.getAndCheckCurrentTransaction() } returns PersistentStoreTransaction(store)
        return QueryEngine(model, store)
    }
}