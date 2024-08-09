package jetbrains.exodus.query

import com.google.common.truth.Truth.assertThat
import com.orientechnologies.orient.core.db.ODatabaseSession
import io.mockk.every
import io.mockk.mockk
import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.EntityIterable
import jetbrains.exodus.entitystore.orientdb.OStoreTransactionImpl
import jetbrains.exodus.entitystore.orientdb.testutil.*
import jetbrains.exodus.query.metadata.EntityMetaData
import jetbrains.exodus.query.metadata.ModelMetaData
import org.junit.Assert
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized


@RunWith(Parameterized::class)
class OQueryEngineTestWithMemorySequenceParametrized(
    val iterableGetter: ((QueryEngine, InMemoryOrientDB) -> (EntityIterable?)),
    val argName: String
) {
    @Rule
    @JvmField
    val orientDB = InMemoryOrientDB()

    companion object {
        @JvmStatic
        @Parameterized.Parameters(name = "{1}")
        fun data(): Collection<Array<Any>> {
            return listOf(
                arrayOf({ _: QueryEngine, _: InMemoryOrientDB -> null }, "Query"),
                arrayOf({ engine: QueryEngine, db: InMemoryOrientDB ->
                    val session = ODatabaseSession.getActiveSession() as ODatabaseSession
                    val txn = OStoreTransactionImpl(session, db.store, db.schemaBuddy, { db.openSession() })
                    val filteringSequence = engine.instantiateGetAll(Issues.CLASS).asSequence().filter {
                        it.id.typeId >= 0
                    }
                    InMemoryEntityIterable(filteringSequence.asIterable(), txn, engine)
                }, "InMemory")
            )
        }
    }

    @Test
    fun issueGetterShouldNotBeEmpty() {
        // Given
        givenTestCase()
        val engine = givenOQueryEngine()

        // When
        orientDB.withTxSession {
            val issues = iterableGetter(engine, orientDB)
            if (issues != null) {
                Assert.assertEquals(3, issues.count())
            }
        }

    }

    @Test
    fun `should query links with select many distinct`() {
        // Given
        val test = givenTestCase()
        orientDB.addIssueToBoard(test.issue1, test.board1)
        orientDB.addIssueToBoard(test.issue1, test.board2)
        orientDB.addIssueToBoard(test.issue2, test.board1)
        orientDB.addIssueToBoard(test.issue3, test.board1)
        val engine = givenOQueryEngine()

        // When
        orientDB.withTxSession {
            val issues = iterableGetter(engine, orientDB) ?: engine.queryGetAll(Issues.CLASS)
            val boardsDistinct = engine.selectManyDistinct(issues, Issues.Links.ON_BOARD)

            // Then
            assertNamesExactly(boardsDistinct, "board1", "board2")
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
        orientDB.withTxSession {
            // Find all issues that are either in project1 or board2
            val issuesOnBoard1 = LinkEqual(Issues.Links.IN_PROJECT, test.project1)
            val issuesOnBoard2 = LinkEqual(Issues.Links.ON_BOARD, test.board2)
            val issues =
                engine.query(iterableGetter(engine, orientDB), Issues.CLASS, Or(issuesOnBoard1, issuesOnBoard2))

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
        orientDB.withTxSession {
            // Find all issues that in project1 or project2
            val issuesInProject1 = LinkEqual(Issues.Links.IN_PROJECT, testCase.project1)
            val issuesInProject2 = LinkEqual(Issues.Links.IN_PROJECT, testCase.project2)
            val issues =
                engine.query(iterableGetter(engine, orientDB), Issues.CLASS, Or(issuesInProject1, issuesInProject2))

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
        orientDB.withTxSession {
            // Find all issues that are on board1 and board2 at the same time
            val issuesOnBoard1 = LinkEqual(Issues.Links.ON_BOARD, test.board1)
            val issuesOnBoard2 = LinkEqual(Issues.Links.ON_BOARD, test.board2)
            val issues =
                engine.query(iterableGetter(engine, orientDB), Issues.CLASS, And(issuesOnBoard1, issuesOnBoard2))

            // Then
            assertNamesExactly(issues, "issue2")
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
        orientDB.withTxSession {
            val issuesInProject = LinkEqual(Issues.Links.IN_PROJECT, testCase.project1)
            val issues = engine.query(iterableGetter(engine, orientDB), Issues.CLASS, issuesInProject)

            // Then
            assertNamesExactly(issues, "issue1", "issue2")
        }
    }

    @Test
    fun `should query by null`() {
        // Given
        val testCase = givenTestCase()
        orientDB.addIssueToProject(testCase.issue1, testCase.project1)

        val engine = givenOQueryEngine()

        // When
        orientDB.withTxSession {
            val issuesNotInProject = LinkEqual(Issues.Links.IN_PROJECT, null)
            val issues = engine.query(iterableGetter(engine, orientDB), Issues.CLASS, issuesNotInProject)

            // Then
            assertNamesExactly(issues, "issue2", "issue3")
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
        orientDB.withTxSession {
            val issuesOnBoard =
                engine.query(iterableGetter(engine, orientDB), Issues.CLASS, LinkNotNull(Issues.Links.ON_BOARD))
            val issuesInProject =
                engine.query(iterableGetter(engine, orientDB), Issues.CLASS, LinkNotNull(Issues.Links.IN_PROJECT))

            // Then
            assertNamesExactly(issuesOnBoard, "issue1", "issue2")
            assertThat(issuesInProject).isEmpty()
        }
    }

    @Test
    fun `should query by not null using unary not`() {
        // Given
        val testCase = givenTestCase()
        orientDB.addIssueToProject(testCase.issue1, testCase.project1)

        val engine = givenOQueryEngine()

        // When
        orientDB.withTxSession {
            val issuesInProject = UnaryNot(LinkEqual(Issues.Links.IN_PROJECT, null))
            val issues = engine.query(iterableGetter(engine, orientDB), Issues.CLASS, issuesInProject)

            // Then
            assertNamesExactly(issues, "issue1")
        }
    }

    @Test
    fun `should query with and`() {
        // Given
        val test = givenTestCase()
        val engine = givenOQueryEngine()

        // When
        orientDB.withTxSession {
            test.issue2.setProperty(Issues.Props.PRIORITY, "normal")

            val nameEqual = PropertyEqual("name", "issue2")
            val projectEqual = PropertyEqual(Issues.Props.PRIORITY, "normal")
            val issues = engine.query(iterableGetter(engine, orientDB), "Issue", And(nameEqual, projectEqual))

            // Then
            assertThat(issues.size()).isEqualTo(1)
            assertThat(issues.first().getProperty("name")).isEqualTo("issue2")
            assertThat(issues.first().getProperty("priority")).isEqualTo("normal")
        }
    }

    @Test
    fun `should query by links sorted`() {
        // Given
        val test = givenTestCase()

        // init association so Orient will know what to do
        orientDB.withSession { session ->
            val issue = session.getClass(Issues.CLASS)
            val project = session.getClass(Projects.CLASS)
            orientDB.addAssociation(issue, project, Issues.Links.IN_PROJECT, Projects.Links.HAS_ISSUE )
        }

        // Issues assigned to project in reverse order
        orientDB.addIssueToProject(test.issue1, test.project3)
        orientDB.addIssueToProject(test.issue2, test.project2)
        orientDB.addIssueToProject(test.issue3, test.project1)

        val metadata = givenModelMetadata().withEntityMetaData(Issues.CLASS)
        val engine = givenOQueryEngine(metadata)

        // When
        orientDB.withTxSession {
            val sortByLinkPropertyAsc = SortByLinkProperty(
                null, // child node
                Projects.CLASS, // link entity class
                "name", // link property name
                Issues.Links.IN_PROJECT, // link name
                true // ascending
            )
            val issueAsc = engine.query(iterableGetter(engine, orientDB), Issues.CLASS, sortByLinkPropertyAsc)

            val sortByLinkPropertyDesc = SortByLinkProperty(
                null, // child node
                Projects.CLASS, // link entity class
                "name", // link property name
                Issues.Links.IN_PROJECT, // link name
                false // descending
            )
            val issuesDesc = engine.query(iterableGetter(engine, orientDB), Issues.CLASS, sortByLinkPropertyDesc)

            // Then
            // As sorted by project name
            assertOrderedNamesExactly(issueAsc, "issue3", "issue2", "issue1")
            assertOrderedNamesExactly(issuesDesc, "issue1", "issue2", "issue3")
        }
    }


    private fun assertNamesExactly(result: Iterable<Entity>, vararg names: String) {
        assertThat(result.map { it.getProperty("name") }).containsExactly(*names)
    }

    private fun assertOrderedNamesExactly(result: Iterable<Entity>, vararg names: String) {
        assertThat(result.map { it.getProperty("name") }).containsExactly(*names).inOrder()
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
        // ToDo: return orientdb compatible query engine
        return QueryEngine(metadataOrNull, orientDB.store).apply {
            sortEngine = SortEngine(this)
        }
    }

    private fun givenTestCase() = OTaskTrackerTestCase(orientDB)
}
