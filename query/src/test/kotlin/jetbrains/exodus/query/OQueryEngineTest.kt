package jetbrains.exodus.query

import com.google.common.truth.Truth.assertThat
import io.mockk.every
import io.mockk.mockk
import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.PersistentEntityStoreImpl
import jetbrains.exodus.entitystore.PersistentStoreTransaction
import jetbrains.exodus.entitystore.orientdb.*
import jetbrains.exodus.query.metadata.ModelMetaData
import org.junit.Rule
import org.junit.Test
import kotlin.test.assertEquals

class OQueryEngineTest {

    @Rule
    @JvmField
    val orientDB = InMemoryOrientDB()

    @Test
    fun `should query all`() {
        // Given
        givenTestCase()
        val engine = givenOQueryEngine()

        // When
        orientDB.withSession {
            val issues = engine.queryGetAll("Issue")

            // Then
            assertNamesExactly(issues, "issue1", "issue2", "issue3")
        }
    }

    @Test
    fun `should query property equal`() {
        // Given
        givenTestCase()
        val engine = givenOQueryEngine()

        // When
        orientDB.withSession {
            val node = PropertyEqual("name", "issue2")
            val result = engine.query("Issue", node).toList()

            // Then
            assertNamesExactly(result, "issue2")
        }
    }

    @Test
    fun `should query property contains`() {
        // Given
        val test = givenTestCase()
        val engine = givenOQueryEngine()
        orientDB.withSession { test.issue2.setProperty("case", "Find me if YOU can") }

        // When
        orientDB.withSession {
            val issues = engine.query("Issue", PropertyContains("case", "YOU", true))
            val empty = engine.query("Issue", PropertyContains("case", "not", true))

            // Then
            assertNamesExactly(issues, "issue2")
            assertThat(empty).isEmpty()
        }
    }

    @Test
    fun `should query property starts with`() {
        // Given
        val test = givenTestCase()
        val engine = givenOQueryEngine()
        orientDB.withSession { test.issue2.setProperty("case", "Find me if YOU can") }

        // When
        orientDB.withSession {
            val issues = engine.query("Issue", PropertyStartsWith("case", "Find"))
            val empty = engine.query("Issue", PropertyStartsWith("case", "you"))

            // Then
            assertNamesExactly(issues, "issue2")
            assertThat(empty).isEmpty()
        }
    }

    @Test
    fun `should query property in range`() {
        // Given
        val test = givenTestCase()
        val engine = givenOQueryEngine()
        orientDB.withSession { test.issue2.setProperty("value", 3) }

        // When
        orientDB.withSession {
            val exclusive = engine.query("Issue", PropertyRange("value", 1, 5))
            val inclusiveMin = engine.query("Issue", PropertyRange("value", 3, 5))
            val inclusiveMax = engine.query("Issue", PropertyRange("value", 1, 3))
            val empty = engine.query("Issue", PropertyRange("value", 6, 12))

            // Then
            assertNamesExactly(exclusive, "issue2")
            assertNamesExactly(inclusiveMin, "issue2")
            assertNamesExactly(inclusiveMax, "issue2")
            assertThat(empty).isEmpty()
        }
    }

    @Test
    fun `should query property exists`() {
        // Given
        val test = givenTestCase()
        val engine = givenOQueryEngine()
        orientDB.withSession { test.issue2.setProperty("prop", "test") }

        // When
        orientDB.withSession {
            val issues = engine.query("Issue", PropertyNotNull("prop"))
            val empty = engine.query("Issue", PropertyNotNull("no_prop"))

            // Then
            assertNamesExactly(issues, "issue2")
            assertThat(empty).isEmpty()
        }
    }


    @Test
    fun `should query with or`() {
        // Given
        val test = givenTestCase()
        val engine = givenOQueryEngine()

        // When
        orientDB.withSession {
            val equal1 = PropertyEqual("name", test.issue1.name())
            val equal2 = PropertyEqual("name", test.issue2.name())
            val issues = engine.query(Issues.CLASS, Or(equal1, equal2))

            // Then
            assertNamesExactly(issues, "issue1", "issue2")
        }
    }

    @Test
    fun `should query with and`() {
        // Given
        val test = givenTestCase()
        val engine = givenOQueryEngine()

        // When
        orientDB.withSession {
            test.issue2.setProperty(Issues.Props.PRIORITY, "normal")

            val nameEqual = PropertyEqual("name", "issue2")
            val projectEqual = PropertyEqual(Issues.Props.PRIORITY, "normal")
            val issues = engine.query("Issue", And(nameEqual, projectEqual))

            // Then
            assertThat(issues.count()).isEqualTo(1)
            assertThat(issues.first().getProperty("name")).isEqualTo("issue2")
            assertThat(issues.first().getProperty("priority")).isEqualTo("normal")
        }
    }

    @Test
    fun `should query links`() {
        // Given
        val testCase = givenTestCase()
        orientDB.addIssueToProject(testCase.issue1, testCase.project1)
        orientDB.addIssueToProject(testCase.issue2, testCase.project1)
        orientDB.addIssueToProject(testCase.issue3, testCase.project2)

        val engine = givenOQueryEngine()

        // When
        orientDB.withSession {
            val issuesInProject = LinkEqual(Issues.Links.IN_PROJECT, testCase.project1)
            val issues = engine.query(Issues.CLASS, issuesInProject)

            // Then
            assertNamesExactly(issues, "issue1", "issue2")
        }
    }

    @Test
    fun `should query links with or`() {
        // Given
        val testCase = givenTestCase()
        orientDB.addIssueToProject(testCase.issue1, testCase.project1)
        orientDB.addIssueToProject(testCase.issue2, testCase.project1)
        orientDB.addIssueToProject(testCase.issue3, testCase.project2)
        val engine = givenOQueryEngine()

        // When
        orientDB.withSession {
            // Find all issues that in project1 or project2
            val issuesInProject1 = LinkEqual(Issues.Links.IN_PROJECT, testCase.project1)
            val issuesInProject2 = LinkEqual(Issues.Links.IN_PROJECT, testCase.project2)
            val issues = engine.query(Issues.CLASS, Or(issuesInProject1, issuesInProject2))

            // Then
            assertNamesExactly(issues, "issue1", "issue2", "issue3")
        }
    }

    @Test
    fun `should query links with and`() {
        // Given
        val test = givenTestCase()
        orientDB.addIssueToBoard(test.issue1, test.board1)
        orientDB.addIssueToBoard(test.issue2, test.board1)
        orientDB.addIssueToBoard(test.issue2, test.board2)
        orientDB.addIssueToBoard(test.issue3, test.board3)
        val engine = givenOQueryEngine()

        // When
        orientDB.withSession {
            // Find all issues that are on board1 and board2 at the same time
            val issuesOnBoard1 = LinkEqual(Issues.Links.ON_BOARD, test.board1)
            val issuesOnBoard2 = LinkEqual(Issues.Links.ON_BOARD, test.board2)
            val issues = engine.query(Issues.CLASS, And(issuesOnBoard1, issuesOnBoard2))

            // Then
            assertNamesExactly(issues, "issue2")
        }
    }

    @Test
    fun `should query different links with or`() {
        // Given
        val test = givenTestCase()
        orientDB.addIssueToProject(test.issue1, test.project1)
        orientDB.addIssueToBoard(test.issue2, test.board2)
        orientDB.addIssueToBoard(test.issue3, test.board3)
        val engine = givenOQueryEngine()

        // When
        orientDB.withSession {
            // Find all issues that are either in project1 or board2
            val issuesOnBoard1 = LinkEqual(Issues.Links.IN_PROJECT, test.project1)
            val issuesOnBoard2 = LinkEqual(Issues.Links.ON_BOARD, test.board2)
            val issues = engine.query(Issues.CLASS, Or(issuesOnBoard1, issuesOnBoard2))

            // Then
            assertNamesExactly(issues, "issue1", "issue2")
        }
    }

    @Test
    fun `should concat 2 queries and sum size`() {
        // Given
        val test = givenTestCase()
        orientDB.addIssueToBoard(test.issue1, test.board1)
        orientDB.addIssueToBoard(test.issue2, test.board1)
        orientDB.addIssueToBoard(test.issue1, test.board2)
        val engine = givenOQueryEngine()

        // When
        orientDB.withSession {
            val issuesOnBoard1 = engine.query(Issues.CLASS, LinkEqual(Issues.Links.ON_BOARD, test.board1))
            val issuesOnBoard2 = engine.query(Issues.CLASS, LinkEqual(Issues.Links.ON_BOARD, test.board2))
            val concat = engine.concat(issuesOnBoard1, issuesOnBoard2)

            // Then
            assertEquals(3, concat.count())
            assertEquals(2, concat.toSet().size)
        }
    }

    private fun assertNamesExactly(result: Iterable<Entity>, vararg names: String) {
        assertThat(result.map { it.getProperty("name") }).containsExactly(*names)
    }

    private fun givenOQueryEngine(): QueryEngine {
        val model = mockk<ModelMetaData>(relaxed = true)
        val store = mockk<PersistentEntityStoreImpl>(relaxed = true)
        every { store.getAndCheckCurrentTransaction() } returns PersistentStoreTransaction(store)
        return QueryEngine(model, store)
    }

    private fun givenTestCase() = TestCase(orientDB)

    private class TestCase(val orientDB: InMemoryOrientDB) {

        val project1 = orientDB.createProject("project1")
        val project2 = orientDB.createProject("project2")
        val project3 = orientDB.createProject("project3")

        val issue1 = orientDB.createIssue("issue1")
        val issue2 = orientDB.createIssue("issue2")
        val issue3 = orientDB.createIssue("issue3")

        val board1 = orientDB.createBoard("board1")
        val board2 = orientDB.createBoard("board2")
        val board3 = orientDB.createBoard("board3")
    }
}
