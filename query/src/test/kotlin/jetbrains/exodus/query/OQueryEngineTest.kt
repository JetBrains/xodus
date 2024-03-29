package jetbrains.exodus.query

import com.google.common.truth.Truth.assertThat
import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.db.document.ODatabaseDocument
import com.orientechnologies.orient.core.record.OElement
import com.orientechnologies.orient.core.record.OVertex
import com.orientechnologies.orient.core.tx.OTransaction
import io.mockk.every
import io.mockk.mockk
import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.PersistentEntityStore
import jetbrains.exodus.entitystore.PersistentEntityStoreImpl
import jetbrains.exodus.entitystore.PersistentStoreTransaction
import jetbrains.exodus.entitystore.orientdb.*
import jetbrains.exodus.query.metadata.EntityMetaData
import jetbrains.exodus.query.metadata.ModelMetaData
import jetbrains.exodus.query.metadata.PropertyMetaData
import jetbrains.exodus.query.metadata.PropertyType
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
    fun `should query when property exists`() {
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
    fun `should query when property exists sorted by value`() {
        // Given
        val test = givenTestCase()
        val engine = givenOQueryEngine()

        orientDB.withSession {
            test.issue1.setProperty("order", "1")
            test.issue2.setProperty("order", "2")
            test.issue3.setProperty("order", "3")
        }

        // When
        orientDB.withSession {
            val issuesAscending = engine.query(Issues.CLASS, SortByProperty(PropertyNotNull("order"), "order", false))
            val issuesDescending = engine.query(Issues.CLASS, SortByProperty(PropertyNotNull("order"), "order", true))

            // Then
            assertNamesExactly(issuesAscending, "issue1", "issue2", "issue3")
            assertNamesExactly(issuesDescending, "issue3", "issue2", "issue1")
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
    fun `should query by links`() {
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
    fun `should query with links not null`() {
        // Given
        val test = givenTestCase()
        orientDB.addIssueToBoard(test.issue1, test.board1)
        orientDB.addIssueToBoard(test.issue2, test.board2)
        val engine = givenOQueryEngine()

        // When
        orientDB.withSession {
            val issuesOnBoard = engine.query(Issues.CLASS, LinkNotNull(Issues.Links.ON_BOARD))
            val issuesInProject = engine.query(Issues.CLASS, LinkNotNull(Issues.Links.IN_PROJECT))

            // Then
            assertNamesExactly(issuesOnBoard, "issue1", "issue2")
            assertThat(issuesInProject).isEmpty()
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
    fun `should query by links sorted`() {
        // Given
        val test = givenTestCase()
        // Issues assigned to project in reverse order
        orientDB.addIssueToProject(test.issue1, test.project3)
        orientDB.addIssueToProject(test.issue2, test.project2)
        orientDB.addIssueToProject(test.issue3, test.project1)

        val metadata = givenModelMetadata().withEntityMetaData(Issues.CLASS)
        val engine = givenOQueryEngine(metadata)

        // When
        orientDB.withSession {
            val sortByLinkPropertyAsc = SortByLinkProperty(
                null, // child node
                Projects.CLASS, // link entity class
                "name", // link property name
                Issues.Links.IN_PROJECT, // link name
                true // ascending
            )
            val issueAsc = engine.query(Issues.CLASS, sortByLinkPropertyAsc)

            val sortByLinkPropertyDesc = SortByLinkProperty(
                null, // child node
                Projects.CLASS, // link entity class
                "name", // link property name
                Issues.Links.IN_PROJECT, // link name
                false // ascending
            )
            val issuesDesc = engine.query(Issues.CLASS, sortByLinkPropertyDesc)

            // Then
            // As sorted by project name
            assertNamesExactly(issueAsc, "issue3", "issue2", "issue1")
            assertNamesExactly(issuesDesc, "issue1", "issue2", "issue3")
        }
    }

    @Test
    fun `should query by links sorted distinct`() {
        // Given
        val test = givenTestCase()
        // Issues assigned to projects in reverse order
        orientDB.addIssueToProject(test.issue1, test.project3)
        orientDB.addIssueToProject(test.issue1, test.project2)

        orientDB.addIssueToProject(test.issue2, test.project2)
        orientDB.addIssueToProject(test.issue2, test.project1)

        orientDB.addIssueToProject(test.issue3, test.project1)
        orientDB.addIssueToProject(test.issue3, test.project2)

        val metadata = givenModelMetadata().withEntityMetaData(Issues.CLASS)
        val engine = givenOQueryEngine(metadata)

        // When
        orientDB.withSession {
            // Find all issues that are either in project1 or board2
            val sortByLinkProperty = SortByLinkProperty(
                null, // child node
                Projects.CLASS, // link entity class
                "name", // link property name
                Issues.Links.IN_PROJECT, // link name
                true // ascending
            )
            val issuesAsc = engine.query(Issues.CLASS, sortByLinkProperty)

            // Then
            assertNamesExactly(issuesAsc, "issue3", "issue2", "issue1")
        }
    }

    @Test
    fun `hasBlob should search for entity with blob`() {
        val test = givenTestCase()
        val metadata = givenModelMetadata {
            val issueMetaData = mockk<EntityMetaData>(relaxed = true)
            every { getEntityMetaData(Issues.CLASS) }.returns(issueMetaData)

            val blobMetaData = mockk<PropertyMetaData>(relaxed = true)
            every { issueMetaData.getPropertyMetaData("myBlob") }.returns(blobMetaData)
            every { blobMetaData.type }.returns(PropertyType.BLOB)
        }
        val engine = givenOQueryEngine(metadata)

        orientDB.withSession {
            //correct blob (can be found)
            test.issue1.setBlob("myBlob", "Hello".toByteArray().inputStream())
            //blob with content of size 0 (can be found)
            test.issue2.setBlob("myBlob", ByteArray(0).inputStream())
            //blob with removed content (cannot be found)
            test.issue3.setBlob("myBlob", "World".toByteArray().inputStream())
            val id = test.issue3.id.asOId()
            val vertex = it.load<OVertex>(id)
            val blobContainer = vertex.getProperty<OElement>("myBlob")
            blobContainer.removeProperty<ByteArray>(OVertexEntity.DATA_PROPERTY_NAME)
            blobContainer.save<OElement>()
        }

        orientDB.withSession {
            val issues = engine.query(Issues.CLASS, PropertyNotNull("myBlob")).toList()
            assertEquals(2, issues.size)
            assertEquals(test.issue1, issues.firstOrNull())
            assertEquals(test.issue2, issues.lastOrNull())
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

    private fun givenModelMetadata(mockingBlock: ((ModelMetaData).() -> Unit)? = null): ModelMetaData {
        return mockk<ModelMetaData>(relaxed = true) {
            mockingBlock?.invoke(this)
        }
    }

    private fun ModelMetaData.withEntityMetaData(
        entityType: String,
        mockingBlock: ((EntityMetaData).() -> Unit)? = null
    ): ModelMetaData {
        val entityMetaData = givenEntityMetaData {
            every { type }.returns(entityType)
            mockingBlock?.invoke(this)
        }
        every { getEntityMetaData(entityType) }.returns(entityMetaData)
        return this
    }

    private fun givenEntityMetaData(mockingBlock: ((EntityMetaData).() -> Unit)? = null): EntityMetaData {
        return mockk<EntityMetaData>(relaxed = true) {
            mockingBlock?.invoke(this)
        }
    }

    private fun givenOQueryEngine(metadataOrNull: ModelMetaData? = null): QueryEngine {
        val metadata = if (metadataOrNull != null) metadataOrNull else mockk<ModelMetaData>(relaxed = true)
        val store = mockk<PersistentEntityStore>(relaxed = true)
        val otx = mockk< OTransaction>(relaxed = true)
        every { store.getAndCheckCurrentTransaction() } returns OStoreTransactionImpl(ODatabaseSession.getActiveSession(), otx, store)
        val engine = QueryEngine(metadata, store)
        engine.sortEngine = SortEngine()
        return engine
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
