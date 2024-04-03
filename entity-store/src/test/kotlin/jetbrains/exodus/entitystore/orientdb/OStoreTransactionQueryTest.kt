package jetbrains.exodus.entitystore.orientdb

import com.google.common.truth.Truth.assertThat
import io.mockk.every
import io.mockk.mockk
import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.orientdb.testutil.InMemoryOrientDB
import jetbrains.exodus.entitystore.orientdb.testutil.Issues
import jetbrains.exodus.entitystore.orientdb.testutil.OTaskTrackerTestCase
import jetbrains.exodus.entitystore.orientdb.testutil.name
import org.junit.Rule
import org.junit.Test

class OStoreTransactionQueryTest {

    @Rule
    @JvmField
    val orientDB = InMemoryOrientDB()

    @Test
    fun `should query all`() {
        // Given
        givenTestCase()
        val tx = givenOTransaction()

        // When
        orientDB.withSession {
            val issues = tx.getAll(Issues.CLASS)

            // Then
            assertNamesExactly(issues, "issue1", "issue2", "issue3")
        }
    }

    @Test
    fun `should query property equal`() {
        // Given
        givenTestCase()
        val tx = givenOTransaction()

        // When
        orientDB.withSession {
            val result = tx.find(Issues.CLASS, "name", "issue2")

            // Then
            assertNamesExactly(result, "issue2")
        }
    }

    @Test
    fun `should query property contains`() {
        // Given
        val test = givenTestCase()
        val tx = givenOTransaction()

        orientDB.withSession { test.issue2.setProperty("case", "Find me if YOU can") }

        // When
        orientDB.withSession {
            val issues = tx.findContaining(Issues.CLASS, "case", "YOU", true)
            val empty = tx.findContaining(Issues.CLASS, "case", "not", true)

            // Then
            assertNamesExactly(issues, "issue2")
            assertThat(empty).isEmpty()
        }
    }

    @Test
    fun `should query property starts with`() {
        // Given
        val test = givenTestCase()
        val tx = givenOTransaction()

        orientDB.withSession { test.issue2.setProperty("case", "Find me if YOU can") }

        // When
        orientDB.withSession {
            val issues = tx.findStartingWith(Issues.CLASS, "case", "Find")
            val empty = tx.findStartingWith(Issues.CLASS, "case", "you")

            // Then
            assertNamesExactly(issues, "issue2")
            assertThat(empty).isEmpty()
        }
    }

    @Test
    fun `should query property in range`() {
        // Given
        val test = givenTestCase()
        val tx = givenOTransaction()

        orientDB.withSession { test.issue2.setProperty("value", 3) }

        // When
        orientDB.withSession {
            val exclusive = tx.find(Issues.CLASS, "value", 1, 5)
            val inclusiveMin = tx.find(Issues.CLASS, "value", 3, 5)
            val inclusiveMax = tx.find(Issues.CLASS, "value", 1, 3)
            val empty = tx.find(Issues.CLASS, "value", 6, 12)

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
        val tx = givenOTransaction()

        orientDB.withSession { test.issue2.setProperty("prop", "test") }

        // When
        orientDB.withSession {
            val issues = tx.findWithProp(Issues.CLASS, "prop")
            val empty = tx.findWithProp(Issues.CLASS, "no_prop")

            // Then
            assertNamesExactly(issues, "issue2")
            assertThat(empty).isEmpty()
        }
    }

    @Test
    fun `should query property sorted by value`() {
        // Given
        val test = givenTestCase()
        val tx = givenOTransaction()

        orientDB.withSession {
            test.issue1.setProperty("order", "1")
            test.issue2.setProperty("order", "2")
            test.issue3.setProperty("order", "3")
        }

        // When
        orientDB.withSession {
            val issuesAscending = tx.sort(Issues.CLASS, "order", true)
            val issuesDescending = tx.sort(Issues.CLASS, "order", false)

            // Then
            assertNamesExactly(issuesAscending, "issue1", "issue2", "issue3")
            assertNamesExactly(issuesDescending, "issue3", "issue2", "issue1")
        }
    }

    @Test
    fun `should query property exists and sorted by value`() {
        // Given
        val test = givenTestCase()
        val tx = givenOTransaction()

        orientDB.withSession {
            test.issue1.setProperty("order", "1")
            test.issue3.setProperty("order", "3")
        }

        // When
        orientDB.withSession {
            val issues = tx.findWithPropSortedByValue(Issues.CLASS, "order")
            val issuesAscending = tx.sort(Issues.CLASS, "order", issues, true)
            val issuesDescending = tx.sort(Issues.CLASS, "order", issues, false)

            // Then
            assertNamesExactly(issuesAscending, "issue1", "issue3")
            assertNamesExactly(issuesDescending, "issue3", "issue1")
        }
    }


    @Test
    fun `should query union`() {
        // Given
        val test = givenTestCase()
        val tx = givenOTransaction()

        // When
        orientDB.withSession {
            val equal1 = tx.find(Issues.CLASS, "name", test.issue1.name())
            val equal2 = tx.find(Issues.CLASS, "name", test.issue2.name())
            val issues = equal1.union(equal2)

            // Then
            assertNamesExactly(issues, "issue1", "issue2")
        }
    }

//    @Test
//    fun `should query with and`() {
//        // Given
//        val test = givenTestCase()
//        val tx = givenOTransaction()
//
//        // When
//        orientDB.withSession {
//            test.issue2.setProperty(PRIORITY, "normal")
//
//            val nameEqual = PropertyEqual("name", "issue2")
//            val projectEqual = PropertyEqual(PRIORITY, "normal")
//            val issues = engine.query("Issue", And(nameEqual, projectEqual))
//
//            // Then
//            assertThat(issues.count()).isEqualTo(1)
//            assertThat(issues.first().getProperty("name")).isEqualTo("issue2")
//            assertThat(issues.first().getProperty("priority")).isEqualTo("normal")
//        }
//    }
//
//    @Test
//    fun `should query by links`() {
//        // Given
//        val testCase = givenTestCase()
//        orientDB.addIssueToProject(testCase.issue1, testCase.project1)
//        orientDB.addIssueToProject(testCase.issue2, testCase.project1)
//        orientDB.addIssueToProject(testCase.issue3, testCase.project2)
//
//        val tx = givenOTransaction()
//
//        // When
//        orientDB.withSession {
//            val issuesInProject = LinkEqual(IN_PROJECT, testCase.project1)
//            val issues = engine.query(CLASS, issuesInProject)
//
//            // Then
//            assertNamesExactly(issues, "issue1", "issue2")
//        }
//    }
//
//    @Test
//    fun `should query with links not null`() {
//        // Given
//        val test = givenTestCase()
//        orientDB.addIssueToBoard(test.issue1, test.board1)
//        orientDB.addIssueToBoard(test.issue2, test.board2)
//        val tx = givenOTransaction()
//
//        // When
//        orientDB.withSession {
//            val issuesOnBoard = engine.query(CLASS, LinkNotNull(ON_BOARD))
//            val issuesInProject = engine.query(CLASS, LinkNotNull(IN_PROJECT))
//
//            // Then
//            assertNamesExactly(issuesOnBoard, "issue1", "issue2")
//            assertThat(issuesInProject).isEmpty()
//        }
//    }
//
//    @Test
//    fun `should query links with or`() {
//        // Given
//        val testCase = givenTestCase()
//        orientDB.addIssueToProject(testCase.issue1, testCase.project1)
//        orientDB.addIssueToProject(testCase.issue2, testCase.project1)
//        orientDB.addIssueToProject(testCase.issue3, testCase.project2)
//        val tx = givenOTransaction()
//
//        // When
//        orientDB.withSession {
//            // Find all issues that in project1 or project2
//            val issuesInProject1 = LinkEqual(IN_PROJECT, testCase.project1)
//            val issuesInProject2 = LinkEqual(IN_PROJECT, testCase.project2)
//            val issues = engine.query(CLASS, Or(issuesInProject1, issuesInProject2))
//
//            // Then
//            assertNamesExactly(issues, "issue1", "issue2", "issue3")
//        }
//    }
//
//    @Test
//    fun `should query links with and`() {
//        // Given
//        val test = givenTestCase()
//        orientDB.addIssueToBoard(test.issue1, test.board1)
//        orientDB.addIssueToBoard(test.issue2, test.board1)
//        orientDB.addIssueToBoard(test.issue2, test.board2)
//        orientDB.addIssueToBoard(test.issue3, test.board3)
//        val tx = givenOTransaction()
//
//        // When
//        orientDB.withSession {
//            // Find all issues that are on board1 and board2 at the same time
//            val issuesOnBoard1 = LinkEqual(ON_BOARD, test.board1)
//            val issuesOnBoard2 = LinkEqual(ON_BOARD, test.board2)
//            val issues = engine.query(CLASS, And(issuesOnBoard1, issuesOnBoard2))
//
//            // Then
//            assertNamesExactly(issues, "issue2")
//        }
//    }
//
//    @Test
//    fun `should query different links with or`() {
//        // Given
//        val test = givenTestCase()
//        orientDB.addIssueToProject(test.issue1, test.project1)
//        orientDB.addIssueToBoard(test.issue2, test.board2)
//        orientDB.addIssueToBoard(test.issue3, test.board3)
//        val tx = givenOTransaction()
//
//        // When
//        orientDB.withSession {
//            // Find all issues that are either in project1 or board2
//            val issuesOnBoard1 = LinkEqual(IN_PROJECT, test.project1)
//            val issuesOnBoard2 = LinkEqual(ON_BOARD, test.board2)
//            val issues = engine.query(CLASS, Or(issuesOnBoard1, issuesOnBoard2))
//
//            // Then
//            assertNamesExactly(issues, "issue1", "issue2")
//        }
//    }
//
//    @Test
//    fun `should query by links sorted`() {
//        // Given
//        val test = givenTestCase()
//        // Issues assigned to project in reverse order
//        orientDB.addIssueToProject(test.issue1, test.project3)
//        orientDB.addIssueToProject(test.issue2, test.project2)
//        orientDB.addIssueToProject(test.issue3, test.project1)
//
//        val metadata = givenModelMetadata().withEntityMetaData(CLASS)
//        val engine = givenOQueryEngine(metadata)
//
//        // When
//        orientDB.withSession {
//            val sortByLinkPropertyAsc = SortByLinkProperty(
//                null, // child node
//                Projects.CLASS, // link entity class
//                "name", // link property name
//                IN_PROJECT, // link name
//                true // ascending
//            )
//            val issueAsc = engine.query(CLASS, sortByLinkPropertyAsc)
//
//            val sortByLinkPropertyDesc = SortByLinkProperty(
//                null, // child node
//                Projects.CLASS, // link entity class
//                "name", // link property name
//                IN_PROJECT, // link name
//                false // ascending
//            )
//            val issuesDesc = engine.query(CLASS, sortByLinkPropertyDesc)
//
//            // Then
//            // As sorted by project name
//            assertNamesExactly(issueAsc, "issue3", "issue2", "issue1")
//            assertNamesExactly(issuesDesc, "issue1", "issue2", "issue3")
//        }
//    }
//
//    @Test
//    fun `should query by links sorted distinct`() {
//        // Given
//        val test = givenTestCase()
//        // Issues assigned to projects in reverse order
//        orientDB.addIssueToProject(test.issue1, test.project3)
//        orientDB.addIssueToProject(test.issue1, test.project2)
//
//        orientDB.addIssueToProject(test.issue2, test.project2)
//        orientDB.addIssueToProject(test.issue2, test.project1)
//
//        orientDB.addIssueToProject(test.issue3, test.project1)
//        orientDB.addIssueToProject(test.issue3, test.project2)
//
//        val metadata = givenModelMetadata().withEntityMetaData(CLASS)
//        val engine = givenOQueryEngine(metadata)
//
//        // When
//        orientDB.withSession {
//            // Find all issues that are either in project1 or board2
//            val sortByLinkProperty = SortByLinkProperty(
//                null, // child node
//                Projects.CLASS, // link entity class
//                "name", // link property name
//                IN_PROJECT, // link name
//                true // ascending
//            )
//            val issuesAsc = engine.query(CLASS, sortByLinkProperty)
//
//            // Then
//            assertNamesExactly(issuesAsc, "issue3", "issue2", "issue1")
//        }
//    }
//
//    @Test
//    fun `hasBlob should search for entity with blob`() {
//        val test = givenTestCase()
//        val metadata = givenModelMetadata {
//            val issueMetaData = mockk<EntityMetaData>(relaxed = true)
//            every { getEntityMetaData(CLASS) }.returns(issueMetaData)
//
//            val blobMetaData = mockk<PropertyMetaData>(relaxed = true)
//            every { issueMetaData.getPropertyMetaData("myBlob") }.returns(blobMetaData)
//            every { blobMetaData.type }.returns(PropertyType.BLOB)
//        }
//        val engine = givenOQueryEngine(metadata)
//
//        orientDB.withSession {
//            //correct blob (can be found)
//            test.issue1.setBlob("myBlob", "Hello".toByteArray().inputStream())
//            //blob with content of size 0 (can be found)
//            test.issue2.setBlob("myBlob", ByteArray(0).inputStream())
//            //blob with removed content (cannot be found)
//            test.issue3.setBlob("myBlob", "World".toByteArray().inputStream())
//            val id = test.issue3.id.asOId()
//            val vertex = it.load<OVertex>(id)
//            val blobContainer = vertex.getProperty<OElement>("myBlob")
//            blobContainer.removeProperty<ByteArray>(OVertexEntity.DATA_PROPERTY_NAME)
//            blobContainer.save<OElement>()
//        }
//
//        orientDB.withSession {
//            val issues = engine.query(CLASS, PropertyNotNull("myBlob")).toList()
//            assertEquals(2, issues.size)
//            assertEquals(test.issue1, issues.firstOrNull())
//            assertEquals(test.issue2, issues.lastOrNull())
//        }
//    }
//
//    @Test
//    fun `should concat 2 queries and sum size`() {
//        // Given
//        val test = givenTestCase()
//        orientDB.addIssueToBoard(test.issue1, test.board1)
//        orientDB.addIssueToBoard(test.issue2, test.board1)
//        orientDB.addIssueToBoard(test.issue1, test.board2)
//        val tx = givenOTransaction()
//
//        // When
//        orientDB.withSession {
//            val issuesOnBoard1 = engine.query(CLASS, LinkEqual(ON_BOARD, test.board1))
//            val issuesOnBoard2 = engine.query(CLASS, LinkEqual(ON_BOARD, test.board2))
//            val concat = engine.concat(issuesOnBoard1, issuesOnBoard2)
//
//            // Then
//            assertEquals(3, concat.count())
//            assertEquals(2, concat.toSet().size)
//        }
//    }
//
//
//    @Test
//    fun `should query distinct`() {
//        // Given
//        val test = givenTestCase()
//        orientDB.addIssueToBoard(test.issue1, test.board1)
//        orientDB.addIssueToBoard(test.issue1, test.board2)
//        orientDB.addIssueToBoard(test.issue2, test.board1)
//        orientDB.addIssueToBoard(test.issue3, test.board1)
//        val tx = givenOTransaction()
//
//        // When
//        orientDB.withSession {
//            val issues = engine.query(
//                CLASS,
//                Or(LinkEqual(ON_BOARD, test.board1), LinkEqual(ON_BOARD, test.board2))
//            ).instantiate() as EntityIterableBase
//
//            val issuesDistinct = issues.distinct()
//
//            // Then
//            assertEquals(4, issues.toList().count())
//            assertNamesExactly(issuesDistinct, "issue1", "issue2", "issue3")
//        }
//    }
//
//
//    @Test
//    fun `should query with minus`() {
//        // Given
//        val test = givenTestCase()
//        orientDB.addIssueToBoard(test.issue1, test.board1)
//        orientDB.addIssueToBoard(test.issue1, test.board2)
//        orientDB.addIssueToBoard(test.issue2, test.board1)
//        orientDB.addIssueToBoard(test.issue3, test.board1)
//        val tx = givenOTransaction()
//
//        // When
//        orientDB.withSession {
//            val issues = engine.query(
//                CLASS,
//                Minus(LinkEqual(ON_BOARD, test.board1), LinkEqual(ON_BOARD, test.board2))
//            )
//
//            // Then
//            assertNamesExactly(issues, "issue2", "issue3")
//        }
//    }
//
//    @Test
//    fun `should query links with select many`() {
//        // Given
//        val test = givenTestCase()
//        orientDB.addIssueToBoard(test.issue1, test.board1)
//        orientDB.addIssueToBoard(test.issue1, test.board2)
//        orientDB.addIssueToBoard(test.issue2, test.board1)
//        orientDB.addIssueToBoard(test.issue3, test.board1)
//        val tx = givenOTransaction()
//
//        // When
//        orientDB.withSession {
//            val issues = engine.queryGetAll(CLASS).instantiate() as EntityIterableBase
//            val boards = issues.selectMany(ON_BOARD)
//
//            // Then
//            assertNamesExactly(boards.sorted(), "board1", "board1", "board1", "board2")
//        }
//    }
//
//    @Test
//    fun `should query links with select many distinct`() {
//        // Given
//        val test = givenTestCase()
//        orientDB.addIssueToBoard(test.issue1, test.board1)
//        orientDB.addIssueToBoard(test.issue1, test.board2)
//        orientDB.addIssueToBoard(test.issue2, test.board1)
//        orientDB.addIssueToBoard(test.issue3, test.board1)
//        val tx = givenOQueryEngine()
//
//        // When
//        orientDB.withSession {
//            val issues = engine.queryGetAll(CLASS).instantiate() as EntityIterableBase
//            val boardsDistinct = engine.selectDistinct(issues, ON_BOARD)
//
//            // Then
//            assertNamesExactly(boardsDistinct, "board1", "board2")
//        }
//    }

    private fun assertNamesExactly(result: Iterable<Entity>, vararg names: String) {
        assertThat(result.map { it.getProperty("name") }).containsExactly(*names)
    }

    private fun givenOTransaction(): OStoreTransactionImpl {
        val store = mockk<OPersistentEntityStore>()
        val session = orientDB.openSession()
        val tx = session.begin().transaction
        val otx = OStoreTransactionImpl(session, tx, store)
        every { store.andCheckCurrentTransaction } returns otx
        return otx
    }


    private fun givenTestCase() = OTaskTrackerTestCase(orientDB)
}