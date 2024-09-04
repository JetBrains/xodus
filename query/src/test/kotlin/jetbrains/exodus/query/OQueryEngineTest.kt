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
package jetbrains.exodus.query

import com.google.common.truth.Truth.assertThat
import io.mockk.every
import io.mockk.mockk
import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.EntityIterable
import jetbrains.exodus.entitystore.orientdb.OStoreTransaction
import jetbrains.exodus.entitystore.orientdb.iterate.OEntityIterableBase
import jetbrains.exodus.entitystore.orientdb.testutil.*
import jetbrains.exodus.query.metadata.EntityMetaData
import jetbrains.exodus.query.metadata.ModelMetaData
import jetbrains.exodus.query.metadata.PropertyMetaData
import jetbrains.exodus.query.metadata.PropertyType
import org.junit.Assert
import org.junit.Ignore
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import kotlin.test.assertEquals


@RunWith(Parameterized::class)
class OQueryEngineTest(
    val iterableGetter: ((QueryEngine, OStoreTransaction) -> (EntityIterable?)),
    val argName: String
): OTestMixin {

    companion object {
        @JvmStatic
        @Parameterized.Parameters(name = "{1}")
        fun data(): Collection<Array<Any>> {
            return listOf(
                arrayOf({ _: QueryEngine, _: OStoreTransaction -> null }, "Query"),
                arrayOf({ engine: QueryEngine, currentTx: OStoreTransaction ->
                    val filteringSequence = engine.instantiateGetAll(Issues.CLASS).asSequence().filter {
                        it.id.typeId >= 0
                    }
                    InMemoryEntityIterable(filteringSequence.asIterable(), currentTx, engine)
                }, "InMemory")
            )
        }
    }


    @Rule
    @JvmField
    val orientDbRule = InMemoryOrientDB()

    override val orientDb = orientDbRule

    @Test
    fun `should query all`() {
        // Given
        givenTestCase()
        val engine = givenOQueryEngine()

        // When
        withStoreTx {
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
        withStoreTx { tx ->
            val node = PropertyEqual("name", "issue2")
            val result = engine.query(iterableGetter(engine, tx), "Issue", node).toList()

            // Then
            assertNamesExactly(result, "issue2")
        }
    }

    @Test
    fun `should query property null`() {
        // Given
        val test = givenTestCase()
        val engine = givenOQueryEngine()
        withStoreTx { test.issue1.setProperty("none", "n1") }

        // When
        withStoreTx { tx ->
            val node = PropertyEqual("none", null)
            val result = engine.query(iterableGetter(engine, tx), "Issue", node).toList()

            // Then
            assertNamesExactly(result, "issue2", "issue3")
        }
    }

    @Test
    fun `should query property contains`() {
        // Given
        val test = givenTestCase()
        val engine = givenOQueryEngine()
        withStoreTx { test.issue2.setProperty("case", "Find me if YOU can") }

        // When
        withStoreTx { tx ->
            val issues = engine.query(iterableGetter(engine, tx), "Issue", PropertyContains("case", "YOU", true))
            val empty = engine.query(iterableGetter(engine, tx), "Issue", PropertyContains("case", "not", true))

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
        withStoreTx { test.issue2.setProperty("case", "Find me if YOU can") }

        // When
        withStoreTx { tx ->
            val issues = engine.query(iterableGetter(engine, tx), "Issue", PropertyStartsWith("case", "Find"))
            val empty = engine.query(iterableGetter(engine, tx), "Issue", PropertyStartsWith("case", "you"))

            // Then
            assertNamesExactly(issues, "issue2")
            assertThat(empty).isEmpty()
        }
    }

    @Test
    fun `should query when property exists`() {
        // Given
        val test = givenTestCase()
        val engine = givenOQueryEngine()
        withStoreTx { test.issue2.setProperty("prop", "test") }

        // When
        withStoreTx { tx ->
            val issues = engine.query(iterableGetter(engine, tx), "Issue", PropertyNotNull("prop"))
            val empty = engine.query(iterableGetter(engine, tx), "Issue", PropertyNotNull("no_prop"))

            // Then
            assertNamesExactly(issues, "issue2")
            assertThat(empty).isEmpty()
        }
    }

    @Ignore
    @Test
    fun `should query when property exists sorted by value`() {
        // Given
        val test = givenTestCase()
        val engine = givenOQueryEngine()

        withStoreTx {
            test.issue1.setProperty("order", "1")
            test.issue2.setProperty("order", "2")
            test.issue3.setProperty("order", "3")
        }

        // When
        withStoreTx { tx ->
            val issuesAscending = engine.query(
                iterableGetter(engine, tx),
                Issues.CLASS,
                SortByProperty(PropertyNotNull("order"), "order", false)
            )
            val issuesDescending = engine.query(
                iterableGetter(engine, tx),
                Issues.CLASS,
                SortByProperty(PropertyNotNull("order"), "order", true)
            )

            // Then
            assertOrderedNamesExactly(issuesAscending, "issue1", "issue2", "issue3")
            assertOrderedNamesExactly(issuesDescending, "issue3", "issue2", "issue1")
        }
    }


    @Test
    fun `should query with or`() {
        // Given
        val test = givenTestCase()
        val engine = givenOQueryEngine()

        // When
        withStoreTx { tx ->
            val equal1 = PropertyEqual("name", test.issue1.name())
            val equal2 = PropertyEqual("name", test.issue2.name())
            val issues = engine.query(iterableGetter(engine, tx), Issues.CLASS, Or(equal1, equal2))

            // Then
            assertNamesExactly(issues, "issue1", "issue2")
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

        withStoreTx {
            //correct blob (can be found)
            test.issue1.setBlob("myBlob", "Hello".toByteArray().inputStream())
            //blob with content of size 0 (can be found)
            test.issue2.setBlob("myBlob", ByteArray(0).inputStream())
        }

        withStoreTx { tx ->
            val issues =
                engine.query(iterableGetter(engine, tx), Issues.CLASS, PropertyNotNull("myBlob")).sortedBy { it.getProperty("name") }
            assertEquals(2, issues.size)
            assertEquals(test.issue1, issues.firstOrNull())
            assertEquals(test.issue2, issues.lastOrNull())
        }
    }

    @Test
    fun `should concat 2 queries and sum size`() {
        // Given
        val test = givenTestCase()
        withStoreTx { tx ->
            tx.addIssueToBoard(test.issue1, test.board1)
            tx.addIssueToBoard(test.issue2, test.board1)
            tx.addIssueToBoard(test.issue1, test.board2)
        }
        val engine = givenOQueryEngine()

        // When
        withStoreTx { tx ->
            val issuesOnBoard1 = engine.query(
                iterableGetter(engine, tx),
                Issues.CLASS,
                LinkEqual(Issues.Links.ON_BOARD, test.board1)
            )
            val issuesOnBoard2 = engine.query(
                iterableGetter(engine, tx),
                Issues.CLASS,
                LinkEqual(Issues.Links.ON_BOARD, test.board2)
            )
            val concat = engine.concat(issuesOnBoard1, issuesOnBoard2)

            // Then
            assertEquals(3, concat.count())
            assertEquals(2, concat.toSet().size)
        }
    }


    @Test
    fun `should query distinct`() {
        // Given
        val test = givenTestCase()
        withStoreTx { tx ->
            tx.addIssueToBoard(test.issue1, test.board1)
            tx.addIssueToBoard(test.issue1, test.board2)
            tx.addIssueToBoard(test.issue2, test.board1)
            tx.addIssueToBoard(test.issue3, test.board1)
        }
        val engine = givenOQueryEngine()

        // When
        withStoreTx { tx ->
            val issues = engine.query(
                iterableGetter(engine, tx),
                Issues.CLASS,
                Or(LinkEqual(Issues.Links.ON_BOARD, test.board1), LinkEqual(Issues.Links.ON_BOARD, test.board2))
            )

            val issuesDistinct = issues.distinct()
            assertNamesExactly(issuesDistinct, "issue1", "issue2", "issue3")
        }
    }


    @Test
    fun `should query with minus`() {
        // Given
        val test = givenTestCase()
        withStoreTx { tx ->
            tx.addIssueToBoard(test.issue1, test.board1)
            tx.addIssueToBoard(test.issue1, test.board2)
            tx.addIssueToBoard(test.issue2, test.board1)
            tx.addIssueToBoard(test.issue3, test.board1)
        }
        val engine = givenOQueryEngine()

        // When
        withStoreTx { tx ->
            val issues = engine.query(
                iterableGetter(engine, tx),
                Issues.CLASS,
                Minus(LinkEqual(Issues.Links.ON_BOARD, test.board1), LinkEqual(Issues.Links.ON_BOARD, test.board2))
            )

            // Then
            assertNamesExactly(issues, "issue2", "issue3")
        }
    }

    @Test
    fun `should query links with select many`() {
        // Given
        val test = givenTestCase()
        withStoreTx { tx ->
            tx.addIssueToBoard(test.issue1, test.board1)
            tx.addIssueToBoard(test.issue1, test.board2)
            tx.addIssueToBoard(test.issue2, test.board1)
            tx.addIssueToBoard(test.issue3, test.board1)
        }
        val engine = givenOQueryEngine()

        // When
        withStoreTx {
            val issues = engine.queryGetAll(Issues.CLASS) as OEntityIterableBase
            val boards = issues.selectMany(Issues.Links.ON_BOARD)

            // Then
            assertNamesExactly(boards.sorted(), "board1", "board1", "board1", "board2")
        }
    }

    @Test
    fun issueGetterShouldNotBeEmpty() {
        // Given
        givenTestCase()
        val engine = givenOQueryEngine()

        // When
        withStoreTx { tx ->
            val issues = iterableGetter(engine, tx)
            if (issues != null) {
                Assert.assertEquals(3, issues.count())
            }
        }

    }

    @Test
    fun `should query links with select many distinct`() {
        // Given
        val test = givenTestCase()
        withStoreTx { tx ->
            tx.addIssueToBoard(test.issue1, test.board1)
            tx.addIssueToBoard(test.issue1, test.board2)
            tx.addIssueToBoard(test.issue2, test.board1)
            tx.addIssueToBoard(test.issue3, test.board1)
        }
        val engine = givenOQueryEngine()

        // When
        withStoreTx { tx ->
            val issues = iterableGetter(engine, tx) ?: engine.queryGetAll(Issues.CLASS)
            val boardsDistinct = engine.selectManyDistinct(issues, Issues.Links.ON_BOARD)

            // Then
            assertNamesExactly(boardsDistinct, "board1", "board2")
        }
    }

    @Test
    fun `should query different links with or`() {
        // Given
        val test = givenTestCase()
        withStoreTx { tx ->
            tx.addIssueToProject(test.issue1, test.project1)
            tx.addIssueToBoard(test.issue2, test.board2)
            tx.addIssueToBoard(test.issue3, test.board3)
        }
        val engine = givenOQueryEngine()

        // When
        withStoreTx { tx ->
            // Find all issues that are either in project1 or board2
            val issuesOnBoard1 = LinkEqual(Issues.Links.IN_PROJECT, test.project1)
            val issuesOnBoard2 = LinkEqual(Issues.Links.ON_BOARD, test.board2)
            val issues =
                engine.query(iterableGetter(engine, tx), Issues.CLASS, Or(issuesOnBoard1, issuesOnBoard2))

            // Then
            assertNamesExactly(issues, "issue1", "issue2")
        }
    }


    @Test
    fun `should query links with or`() {
        // Given
        val testCase = givenTestCase()
        withStoreTx { tx ->
            tx.addIssueToProject(testCase.issue1, testCase.project1)
            tx.addIssueToProject(testCase.issue2, testCase.project1)
            tx.addIssueToProject(testCase.issue3, testCase.project2)
        }
        val engine = givenOQueryEngine()

        // When
        withStoreTx { tx ->
            // Find all issues that in project1 or project2
            val issuesInProject1 = LinkEqual(Issues.Links.IN_PROJECT, testCase.project1)
            val issuesInProject2 = LinkEqual(Issues.Links.IN_PROJECT, testCase.project2)
            val issues =
                engine.query(iterableGetter(engine, tx), Issues.CLASS, Or(issuesInProject1, issuesInProject2))

            // Then
            assertNamesExactly(issues, "issue1", "issue2", "issue3")
        }
    }

    @Test
    fun `should query links with and`() {
        // Given
        val test = givenTestCase()
        withStoreTx { tx ->
            tx.addIssueToBoard(test.issue1, test.board1)
            tx.addIssueToBoard(test.issue2, test.board1)
            tx.addIssueToBoard(test.issue2, test.board2)
            tx.addIssueToBoard(test.issue3, test.board3)
        }
        val engine = givenOQueryEngine()

        // When
        withStoreTx { tx ->
            // Find all issues that are on board1 and board2 at the same time
            val issuesOnBoard1 = LinkEqual(Issues.Links.ON_BOARD, test.board1)
            val issuesOnBoard2 = LinkEqual(Issues.Links.ON_BOARD, test.board2)
            val issues =
                engine.query(iterableGetter(engine, tx), Issues.CLASS, And(issuesOnBoard1, issuesOnBoard2))

            // Then
            assertNamesExactly(issues, "issue2")
        }
    }

    @Test
    fun `should query by links`() {
        // Given
        val testCase = givenTestCase()
        withStoreTx { tx ->
            tx.addIssueToProject(testCase.issue1, testCase.project1)
            tx.addIssueToProject(testCase.issue2, testCase.project1)
            tx.addIssueToProject(testCase.issue3, testCase.project2)
        }

        val engine = givenOQueryEngine()

        // When
        withStoreTx { tx ->
            val issuesInProject = LinkEqual(Issues.Links.IN_PROJECT, testCase.project1)
            val issues = engine.query(iterableGetter(engine, tx), Issues.CLASS, issuesInProject)

            // Then
            assertNamesExactly(issues, "issue1", "issue2")
        }
    }

    @Test
    fun `should query by null`() {
        // Given
        val testCase = givenTestCase()
        withStoreTx { tx ->
            tx.addIssueToProject(testCase.issue1, testCase.project1)
        }

        val engine = givenOQueryEngine()

        // When
        withStoreTx { tx ->
            val issuesNotInProject = LinkEqual(Issues.Links.IN_PROJECT, null)
            val issues = engine.query(iterableGetter(engine, tx), Issues.CLASS, issuesNotInProject)

            // Then
            assertNamesExactly(issues, "issue2", "issue3")
        }
    }


    @Test
    fun `should query with links not null`() {
        // Given
        val test = givenTestCase()
        withStoreTx { tx ->
            tx.addIssueToBoard(test.issue1, test.board1)
            tx.addIssueToBoard(test.issue2, test.board2)
        }
        val engine = givenOQueryEngine()

        // When
        withStoreTx { tx ->
            val issuesOnBoard =
                engine.query(iterableGetter(engine, tx), Issues.CLASS, LinkNotNull(Issues.Links.ON_BOARD))
            val issuesInProject =
                engine.query(iterableGetter(engine, tx), Issues.CLASS, LinkNotNull(Issues.Links.IN_PROJECT))

            // Then
            assertNamesExactly(issuesOnBoard, "issue1", "issue2")
            assertThat(issuesInProject).isEmpty()
        }
    }

    @Test
    fun `should query by not null using unary not`() {
        // Given
        val testCase = givenTestCase()
        withStoreTx { tx ->
            tx.addIssueToProject(testCase.issue1, testCase.project1)
        }

        val engine = givenOQueryEngine()

        // When
        withStoreTx { tx ->
            val issuesInProject = UnaryNot(LinkEqual(Issues.Links.IN_PROJECT, null))
            val issues = engine.query(iterableGetter(engine, tx), Issues.CLASS, issuesInProject)

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
        withStoreTx { tx ->
            test.issue2.setProperty(Issues.Props.PRIORITY, "normal")

            val nameEqual = PropertyEqual("name", "issue2")
            val projectEqual = PropertyEqual(Issues.Props.PRIORITY, "normal")
            val issues = engine.query(iterableGetter(engine, tx), "Issue", And(nameEqual, projectEqual))

            // Then
            assertThat(issues.size()).isEqualTo(1)
            assertThat(issues.first().getProperty("name")).isEqualTo("issue2")
            assertThat(issues.first().getProperty("priority")).isEqualTo("normal")
        }
    }

    @Test
    fun `should query property in range`() {
        // Given
        val test = givenTestCase()
        val engine = givenOQueryEngine()
        withStoreTx { test.issue2.setProperty("value", 3) }

        // When
        withStoreTx {
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
    fun `should query by links sorted`() {
        // Given
        val test = givenTestCase()

        // Issues assigned to project in reverse order
        withStoreTx { tx ->
            tx.addIssueToProject(test.issue1, test.project3)
            tx.addIssueToProject(test.issue2, test.project2)
            tx.addIssueToProject(test.issue3, test.project1)
        }

        val metadata = givenModelMetadata().withEntityMetaData(Issues.CLASS)
        val engine = givenOQueryEngine(metadata)

        // When
        withStoreTx { tx ->
            val sortByLinkPropertyAsc = SortByLinkProperty(
                null, // child node
                Projects.CLASS, // link entity class
                "name", // link property name
                Issues.Links.IN_PROJECT, // link name
                true // ascending
            )
            val issueAsc = engine.query(iterableGetter(engine, tx), Issues.CLASS, sortByLinkPropertyAsc)

            val sortByLinkPropertyDesc = SortByLinkProperty(
                null, // child node
                Projects.CLASS, // link entity class
                "name", // link property name
                Issues.Links.IN_PROJECT, // link name
                false // descending
            )
            val issuesDesc = engine.query(iterableGetter(engine, tx), Issues.CLASS, sortByLinkPropertyDesc)

            // Then
            // As sorted by project name
            assertOrderedNamesExactly(issueAsc, "issue3", "issue2", "issue1")
            assertOrderedNamesExactly(issuesDesc, "issue1", "issue2", "issue3")
        }
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
        return QueryEngine(metadataOrNull, orientDb.store).apply {
            sortEngine = SortEngine(this)
        }
    }
}
