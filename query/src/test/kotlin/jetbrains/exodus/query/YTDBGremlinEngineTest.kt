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
import jetbrains.exodus.entitystore.youtrackdb.gremlin.GremlinEntityIterable
import jetbrains.exodus.entitystore.youtrackdb.gremlin.GremlinBlock.SortDirection
import jetbrains.exodus.entitystore.youtrackdb.testutil.InMemoryYouTrackDB
import jetbrains.exodus.entitystore.youtrackdb.testutil.Issues
import jetbrains.exodus.entitystore.youtrackdb.testutil.OTestMixin
import jetbrains.exodus.entitystore.youtrackdb.testutil.name
import jetbrains.exodus.query.metadata.EntityMetaData
import jetbrains.exodus.query.metadata.ModelMetaData
import jetbrains.exodus.query.metadata.PropertyMetaData
import jetbrains.exodus.query.metadata.PropertyType
import junit.framework.TestCase.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test


// todo: parameterized
//@RunWith(Parameterized::class)
class YTDBGremlinEngineTest(
//    val iterableGetter: ((QueryEngine, YTDBStoreTransaction) -> (EntityIterable?)),
//    val argName: String
) : OTestMixin {

//    companion object {
//        @JvmStatic
//        @Parameterized.Parameters(name = "{1}")
//        fun data(): Collection<Array<Any>> {
//            return listOf(
//                arrayOf({ _: QueryEngine, _: YTDBStoreTransaction -> null }, "Query"),
//                arrayOf({ engine: QueryEngine, currentTx: YTDBStoreTransaction ->
//                    val filteringSequence = engine.instantiateGetAll(Issues.CLASS).asSequence().filter {
//                        it.id.typeId >= 0
//                    }
//                    InMemoryEntityIterable(filteringSequence.asIterable(), currentTx, engine)
//                }, "InMemory"),
//                arrayOf({ engine: QueryEngine, currentTx: YTDBStoreTransaction ->
//                    val filteringSequence = engine.instantiateGetAll(Issues.CLASS).asSequence().filter {
//                        it.id.typeId >= 0
//                    }
//                    YTDBMultipleEntitiesIterable(currentTx, filteringSequence.toList())
//                }, "MultipleEntitiesIterable")
//            )
//        }
//    }


    @Rule
    @JvmField
    val orientDbRule = InMemoryYouTrackDB()

    override val youTrackDb = orientDbRule

    @Test
    fun `should query all`() {
        // Given
        givenTestCase()
        val engine = givenOQueryEngine()

        // When
        withStoreTx {
            val issues = engine.query("Issue", NodeFactory.all())

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
            val result = engine.query("Issue", NodeFactory.propEqual("name", "issue2"))

            // Then
            assertNamesExactly(result, "issue2")
        }
    }

    @Test
    fun `should query contains`() {
        // Given
        val testCase = givenTestCase()
        val engine = givenOQueryEngine()

        withStoreTx { tx ->
            assertTrue(engine.query("Issue", NodeFactory.all()).contains(testCase.issue1))
            assertTrue(engine.query("Issue", NodeFactory.all()).contains(testCase.issue2))
            assertTrue(engine.query("Issue", NodeFactory.all()).contains(testCase.issue3))

            val onlyIssue1 = engine.query("Issue", NodeFactory.propEqual("name", "issue1"))
            assertTrue(onlyIssue1.contains(testCase.issue1))
            assertFalse(onlyIssue1.contains(testCase.issue2))
            assertFalse(onlyIssue1.contains(testCase.issue3))
        }
    }


    @Test
    fun `should query property null`() {
        // Given
        val test = givenTestCase()
        val engine = givenOQueryEngine()
        withStoreTx {
            test.issue1.setProperty("none", "n1")
        }

        // When
        withStoreTx { tx ->
            val result = engine.query("Issue", NodeFactory.propEqual("none", null)).toList()

            // Then
            assertNamesExactly(result, "issue2", "issue3")
        }
    }

    @Test
    fun `should query property contains (string)`() {
        // Given
        val test = givenTestCase()
        val engine = givenOQueryEngine()
        withStoreTx { test.issue2.setProperty("case", "Find me if YOU can") }

        // When
        withStoreTx { tx ->
            val issues = engine.query("Issue", NodeFactory.hasSubstring("case", "YOU", false))
            val issuesIgnoreCase =
                engine.query("Issue", NodeFactory.hasSubstring("case", "yOu", true))
            val issuesIgnoreNotIgnoreCase = engine.query("Issue", NodeFactory.hasSubstring("case", "yOu", false))
            val empty = engine.query("Issue", NodeFactory.hasSubstring("case", "not", true))

            // Then
            assertNamesExactly(issues, "issue2")
            assertNamesExactly(issuesIgnoreCase, "issue2")
            //this may be subject to change if we want to support exact case search
            assertThat(issuesIgnoreNotIgnoreCase).isEmpty()
            assertThat(empty).isEmpty()
        }
    }

    @Test
    fun `should query property contains (collection)`() {
        givenTestCase()
        val engine = givenOQueryEngine()

        withStoreTx { tx ->
            val bugs = engine.query("Issue", NodeFactory.hasElement("tags", "bug"))
            val inProgress = engine.query("Issue", NodeFactory.hasElement("tags", "in_progress"))
            val abandoned = engine.query("Issue", NodeFactory.hasElement("tags", "abandoned"))

            assertNamesExactly(bugs, "issue1")
            assertNamesExactly(inProgress, "issue1", "issue3")
            assertThat(abandoned).isEmpty()
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
            val issues = engine.query("Issue", NodeFactory.hasPrefix("case", "Find"))
            val issuesOtherCase = engine.query("Issue", NodeFactory.hasPrefix("case", "find"))
            val empty = engine.query("Issue", NodeFactory.hasPrefix("case", "you"))

            // Then
            assertNamesExactly(issues, "issue2")
            assertNamesExactly(issuesOtherCase, "issue2")
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
            val issues = engine.query("Issue", NodeFactory.propNotNull("prop"))
            val empty = engine.query("Issue", NodeFactory.propNotNull("no_prop"))

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

        withStoreTx {
            test.issue1.setProperty("order", "1")
            test.issue2.setProperty("order", "2")
            test.issue3.setProperty("order", "3")
        }

        // When
        withStoreTx { tx ->
            val issuesAscending = engine.query(
                Issues.CLASS,
                NodeFactory.combine(
                    NodeFactory.propNotNull("order"),
                    NodeFactory.sortBy("order", SortDirection.ASC)
                )
            )
            val issuesDescending = engine.query(
                Issues.CLASS,
                NodeFactory.combine(
                    NodeFactory.propNotNull("order"),
                    NodeFactory.sortBy("order", SortDirection.DESC)
                )
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
            val issues = engine.query(
                Issues.CLASS, NodeFactory.or(
                    NodeFactory.propEqual("name", test.issue1.name()),
                    NodeFactory.propEqual("name", test.issue2.name())
                )
            )

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
                engine.query(Issues.CLASS, NodeFactory.propNotNull("myBlob"))
                    .toList()
                    .sortedBy { it.getProperty("name") }
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
                Issues.CLASS,
                NodeFactory.hasLinkTo(Issues.Links.ON_BOARD, test.board1)
            )
            val issuesOnBoard2 = engine.query(
                Issues.CLASS,
                NodeFactory.hasLinkTo(Issues.Links.ON_BOARD, test.board2)
            )
            val concat = engine.concat(issuesOnBoard1, issuesOnBoard2)

            // Then
            assertEquals(3, concat.count())
            assertEquals(2, concat.toSet().size)
        }
    }

    @Test
    fun `should query by link`() {
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

            val issuesFromBoard1 = engine.query(
                Issues.CLASS,
                NodeFactory.hasLinkTo(Issues.Links.ON_BOARD, test.board1)
            )

            val issuesFromBoard2 = engine.query(
                Issues.CLASS,
                NodeFactory.hasLinkTo(Issues.Links.ON_BOARD, test.board2)
            )

            assertNamesExactly(issuesFromBoard1, "issue1", "issue2", "issue3")
            assertNamesExactly(issuesFromBoard2, "issue1")
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

                Issues.CLASS,
                NodeFactory.or(
                    NodeFactory.hasLinkTo(Issues.Links.ON_BOARD, test.board1),
                    NodeFactory.hasLinkTo(Issues.Links.ON_BOARD, test.board2)
                )
            )

            val issuesDistinct = issues.distinct()
            assertNamesExactly(issuesDistinct, "issue1", "issue2", "issue3")
        }
    }

    @Test
    fun `should query with minus (and not)`() {
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
                Issues.CLASS,
                NodeFactory.and(
                    NodeFactory.hasLinkTo(Issues.Links.ON_BOARD, test.board1),
                    NodeFactory.not(NodeFactory.hasLinkTo(Issues.Links.ON_BOARD, test.board2))
                )
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
        withStoreTx { tx ->
            val issues = engine.query(Issues.CLASS, NodeFactory.all()) as GremlinEntityIterable
            val boards = issues.selectMany(Issues.Links.ON_BOARD)

            // Then
            assertNamesExactly(boards.sorted(), "board1", "board1", "board1", "board2")
        }
    }

//    @Test
//    fun issueGetterShouldNotBeEmpty() {
//        // Given
//        givenTestCase()
//        val engine = givenOQueryEngine()
//
//        // When
//        withStoreTx { tx ->
//            val issues = iterableGetter(engine, tx)
//            if (issues != null) {
//                Assert.assertEquals(3, issues.count())
//            }
//        }
//
//    }

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
            val issues = engine.query(Issues.CLASS, NodeFactory.all()) as GremlinEntityIterable
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
            val issues =
                engine.query(
                    Issues.CLASS,
                    NodeFactory.or(
                        NodeFactory.hasLinkTo(Issues.Links.IN_PROJECT, test.project1),
                        NodeFactory.hasLinkTo(Issues.Links.ON_BOARD, test.board2)
                    )
                )

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
            val issues = engine.query(
                Issues.CLASS, NodeFactory.or(
                    NodeFactory.hasLinkTo(Issues.Links.IN_PROJECT, testCase.project1),
                    NodeFactory.hasLinkTo(Issues.Links.IN_PROJECT, testCase.project2)
                )
            )

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
            val issues = engine.query(
                Issues.CLASS, NodeFactory.and(
                    NodeFactory.hasLinkTo(Issues.Links.ON_BOARD, test.board1),
                    NodeFactory.hasLinkTo(Issues.Links.ON_BOARD, test.board2)
                )
            )

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
            val issues = engine.query(
                Issues.CLASS,
                NodeFactory.hasLinkTo(Issues.Links.IN_PROJECT, testCase.project1)
            )

            // Then
            assertNamesExactly(issues, "issue1", "issue2")
        }
    }

    @Test
    fun `should query by null links`() {
        // Given
        val testCase = givenTestCase()
        withStoreTx { tx ->
            tx.addIssueToProject(testCase.issue1, testCase.project1)
        }

        val engine = givenOQueryEngine()

        // When
        withStoreTx { tx ->
            val issuesNotInProject = NodeFactory.hasLinkTo(Issues.Links.IN_PROJECT, null)
            val issues = engine.query(Issues.CLASS, issuesNotInProject)

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
                engine.query(Issues.CLASS, NodeFactory.hasLink(Issues.Links.ON_BOARD))
            val issuesInProject =
                engine.query(Issues.CLASS, NodeFactory.hasLink(Issues.Links.IN_PROJECT))

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
            val issues = engine.query(Issues.CLASS, NodeFactory.not(NodeFactory.hasNoLink(Issues.Links.IN_PROJECT)))

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

            val issues = engine.query(
                "Issue", NodeFactory.and(
                    NodeFactory.propEqual("name", "issue2"),
                    NodeFactory.propEqual(Issues.Props.PRIORITY, "normal")
                )
            )

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
            val exclusive = engine.query("Issue", NodeFactory.inRange("value", 1, 5))
            val inclusiveMin = engine.query("Issue", NodeFactory.inRange("value", 3, 5))
            val inclusiveMax = engine.query("Issue", NodeFactory.inRange("value", 1, 3))
            val empty = engine.query("Issue", NodeFactory.inRange("value", 6, 12))

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
            val sortByLinkPropertyAsc =
                NodeFactory.sortByLinked(Issues.Links.IN_PROJECT, "name", SortDirection.ASC)
            val sortByLinkPropertyDesc =
                NodeFactory.sortByLinked(Issues.Links.IN_PROJECT, "name", SortDirection.DESC)
            val issueAsc = engine.query(Issues.CLASS, sortByLinkPropertyAsc)
            val issuesDesc = engine.query(Issues.CLASS, sortByLinkPropertyDesc)

            // Then
            // As sorted by project name
            assertOrderedNamesExactly(issueAsc, "issue3", "issue2", "issue1")
            assertOrderedNamesExactly(issuesDesc, "issue1", "issue2", "issue3")
        }
    }

    @Test
    fun `should query by property sorted`() {
        // Given
        val test = givenTestCase()

        val metadata = givenModelMetadata().withEntityMetaData(Issues.CLASS)
        val engine = givenOQueryEngine(metadata)

        // When
        withStoreTx { tx ->

            val sortByPropertyAsc =
                NodeFactory.sortBy("name", SortDirection.ASC)
//
            val issuesAsc = engine.query(Issues.CLASS, sortByPropertyAsc)

            val sortByLinkPropertyDesc = NodeFactory.sortBy("name", SortDirection.DESC)
            val issuesDesc = engine.query(Issues.CLASS, sortByLinkPropertyDesc)

            // Then
            // As sorted by project name
            assertOrderedNamesExactly(issuesDesc, "issue3", "issue2", "issue1")
            assertOrderedNamesExactly(issuesAsc, "issue1", "issue2", "issue3")
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
//
    private fun givenOQueryEngine(metadataOrNull: ModelMetaData? = null): QueryEngine {
        // ToDo: return orientdb compatible query engine
        return QueryEngine(metadataOrNull, youTrackDb.store).apply {
            sortEngine = SortEngine(this)
        }
    }
}
