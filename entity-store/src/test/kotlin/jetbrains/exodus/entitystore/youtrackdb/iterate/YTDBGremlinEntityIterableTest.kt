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
package jetbrains.exodus.entitystore.youtrackdb.iterate

import com.google.common.truth.Truth.assertThat
import jetbrains.exodus.entitystore.youtrackdb.YTDBEntityIterable
import jetbrains.exodus.entitystore.youtrackdb.YTDBStoreTransaction
import jetbrains.exodus.entitystore.youtrackdb.getOrCreateVertexClass
import jetbrains.exodus.entitystore.youtrackdb.gremlin.GremlinBlock
import jetbrains.exodus.entitystore.youtrackdb.gremlin.GremlinEntityIterable
import jetbrains.exodus.entitystore.youtrackdb.iterate.property.YTDBInstanceOfIterable
import jetbrains.exodus.entitystore.youtrackdb.query.buildSql
import jetbrains.exodus.entitystore.youtrackdb.testutil.*
import org.apache.tinkerpop.gremlin.process.traversal.Order
import org.apache.tinkerpop.gremlin.process.traversal.P
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.junit.Rule
import org.junit.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class YTDBGremlinEntityIterableTest : OTestMixin {

    @Rule
    @JvmField
    val orientDbRule = InMemoryYouTrackDB()

    override val youTrackDb = orientDbRule

    @Test
    fun `property is null`() {
        // Given
        val test = givenTestCase()
        withStoreTx {
            test.issue1.setProperty("none", "n1")
        }

        // When
        withStoreTx { tx ->
            val issues = GremlinEntityIterable.where(
                Issues.CLASS, tx, GremlinBlock.PropNull("none")
            )

            // Then
            // todo:
//            tx.checkSql(
//                issues,
//                expectedSql = "SELECT FROM Issue WHERE none is null"
//            )

            assertNamesExactly(issues, "issue2", "issue3")
        }
    }

    @Test
    fun `no links`() {
        // Given
        val test = givenTestCase()
        withStoreTx { tx ->
            tx.addIssueToProject(test.issue1, test.project1)
        }

        // When
        withStoreTx { tx ->
            val issues = GremlinEntityIterable.where(
                Issues.CLASS, tx,
                GremlinBlock.HasNoLink(Issues.Links.IN_PROJECT)
            )

            // Then
            // todo
            // tx.checkSql(
            //     issues,
            //     expectedSql = "SELECT FROM Issue WHERE outE('InProject_link').size() == 0",
            //     expectedParams = mapOf()
            // )


            assertNamesExactly(issues, "issue2", "issue3")
        }
    }

    @Test
    fun `property equals`() {
        // Given
        val test = givenTestCase()
        withStoreTx { tx ->
            test.issue1.setProperty("opca", 300)
            test.issue2.setProperty("opca", 200)
            test.issue3.setProperty("opca", 300)
        }

        // When
        withStoreTx { tx ->
            val issues = GremlinEntityIterable.where(
                Issues.CLASS,
                tx,
                GremlinBlock.PropEqual("opca", 300)
            )

            // Then
            // todo
//            tx.checkSql(
//                issues,
//                expectedSql = "SELECT FROM Issue WHERE opca = :opca0",
//                expectedParams = mapOf("opca0" to 300)
//            )
            assertNamesExactly(issues, "issue1", "issue3")
        }
    }

    @Test
    fun `union two iterables`() {
        // Given
        val test = givenTestCase()

        // When
        withStoreTx { tx ->
            val equal1 = tx.find(Issues.CLASS, "name", test.issue1.name())
            val equal2 = tx.find(Issues.CLASS, "name", test.issue2.name())

            val issues = equal1.union(equal2)

            // Then
            // todo:
//            tx.checkSql(
//                issues,
//                expectedSql = "SELECT FROM Issue WHERE (name = :name0 OR name = :name1)",
//                expectedParams = mapOf("name0" to "issue1", "name1" to "issue2")
//            )
            assertNamesExactly(issues, "issue1", "issue2")
        }
    }

    @Test
    fun `union two iterables having the same issue`() {
        // Given
        val test = givenTestCase()

        // When
        withStoreTx { tx ->
            val equal1 = tx.find(Issues.CLASS, "name", test.issue1.name())
            val equal2 = tx.find(Issues.CLASS, "name", test.issue1.name())

            val issues = equal1.union(equal2)

            // Then
                // todo
//            tx.checkSql(
//                issues,
//                expectedSql = "SELECT FROM Issue WHERE (name = :name0 OR name = :name1)",
//                expectedParams = mapOf("name0" to "issue1", "name1" to "issue1")
//            )
            // Union operation can distinct result set if query is optimized to OR conditions
            assertNamesExactly(issues, "issue1")
        }
    }

    @Test
    fun `intersect two iterables`() {
        // Given
        val test = givenTestCase()
        withStoreTx {
            test.issue2.setProperty(Issues.Props.PRIORITY, "normal")
        }

        // When
        withStoreTx { tx ->
            val nameEqual = tx.find(Issues.CLASS, "name", test.issue2.name())
            val priorityEqual = tx.find(Issues.CLASS, Issues.Props.PRIORITY, "normal")
            val issues = nameEqual.intersect(priorityEqual)

            // Then
            // todo
//            tx.checkSql(
//                issues,
//                expectedSql = "SELECT FROM Issue WHERE (name = :name0 AND priority = :priority1)",
//                expectedParams = mapOf("name0" to "issue2", "priority1" to "normal")
//            )
            assertNamesExactly(issues, "issue2")
            assertThat(issues.first().getProperty("priority")).isEqualTo("normal")
        }
    }

    @Test
    fun `concat iterables selected by properties`() {
        // Given
        val test = givenTestCase()

        withStoreTx { tx ->
            tx.addIssueToBoard(test.issue1, test.board1)
            tx.addIssueToBoard(test.issue2, test.board1)
            tx.addIssueToBoard(test.issue1, test.board2)
        }

        // When
        withStoreTx { tx ->
            val issue1 = tx.find(Issues.CLASS, "name", "issue1")
            val issue2 = tx.find(Issues.CLASS, "name", "issue2")
            val concat = issue1.concat(issue2)

            // Then
            // todo
//            tx.checkSql(
//                concat,
//                expectedSql = "SELECT expand(unionall(\$a0, \$b0)) LET \$a0=(SELECT FROM Issue WHERE name = :name1), \$b0=(SELECT FROM Issue WHERE name = :name2)",
//                expectedParams = mapOf("name1" to "issue1", "name2" to "issue2")
//            )
            assertNamesExactlyInOrder(concat, "issue1", "issue2")

            val concatMore = concat.concat(issue1)
            assertNamesExactlyInOrder(concatMore, "issue1", "issue2", "issue1")


            val concatEvenMore = concatMore.concat(issue2)
            assertNamesExactlyInOrder(concatEvenMore, "issue1", "issue2", "issue1", "issue2")
        }
    }

    @Test
    fun `find links`() {
        // Given
        val test = givenTestCase()

        withStoreTx { tx ->
            tx.addIssueToBoard(test.issue1, test.board1)
            tx.addIssueToBoard(test.issue2, test.board1)
        }

        // When
        withStoreTx { tx ->
            val issues = tx.findLinks(
                Issues.CLASS,
                test.board1,
                Issues.Links.ON_BOARD
            ) as GremlinEntityIterable

            // Then
            // todo:
//            tx.checkSql(
//                issues,
//                expectedSql = "SELECT FROM (SELECT expand(in('OnBoard_link')) FROM :targetIds0) WHERE @class='${Issues.CLASS}'",
//                expectedParams = mapOf(
//                    "targetIds0" to listOf(test.board1.id.asOId()),
//                )
//            )
            assertNamesExactly(issues, "issue1", "issue2")
        }
    }

    @Test
    fun `concat iterables selected by links`() {
        // Given
        val test = givenTestCase()

        withStoreTx { tx ->
            tx.addIssueToBoard(test.issue1, test.board1)
            tx.addIssueToBoard(test.issue2, test.board1)
            tx.addIssueToBoard(test.issue1, test.board2)
        }

        // When
        withStoreTx { tx ->
            val issuesOnBoard1 = tx.findLinks(Issues.CLASS, test.board1, Issues.Links.ON_BOARD)
            val issuesOnBoard2 = tx.findLinks(Issues.CLASS, test.board2, Issues.Links.ON_BOARD)
            val concat = issuesOnBoard1.concat(issuesOnBoard2)

            // Then
            // todo
//            tx.checkSql(
//                concat,
//                expectedSql = "SELECT expand(unionall(\$a0, \$b0)) LET \$a0=(SELECT FROM (SELECT expand(in('OnBoard_link')) FROM :targetIds1) WHERE @class='${Issues.CLASS}'), \$b0=(SELECT FROM (SELECT expand(in('OnBoard_link')) FROM :targetIds2) WHERE @class='${Issues.CLASS}')",
//                expectedParams = mapOf(
//                    "targetIds1" to listOf(test.board1.id.asOId()),
//                    "targetIds2" to listOf(test.board2.id.asOId()),
//                )
//            )
            assertNamesExactly(concat, "issue1", "issue2", "issue1")
        }
    }

    @Test
    fun distinct() {
        // Given
        val test = givenTestCase()

        withStoreTx { tx ->
            tx.addIssueToBoard(test.issue1, test.board1)
            tx.addIssueToBoard(test.issue1, test.board2)
            tx.addIssueToBoard(test.issue2, test.board1)
            tx.addIssueToBoard(test.issue3, test.board1)
        }

        // When
        withStoreTx { tx ->
            val issuesOnBoard1 = tx.findLinks(Issues.CLASS, test.board1, Issues.Links.ON_BOARD)
            val issuesOnBoard2 = tx.findLinks(Issues.CLASS, test.board2, Issues.Links.ON_BOARD)
            val issues = issuesOnBoard1.union(issuesOnBoard2)
            val issuesDistinct = issues.distinct()

            // Then
            // todo
//            tx.checkSql(
//                issuesDistinct,
//                expectedSql = "SELECT DISTINCT * FROM (SELECT expand(unionall(\$a0, \$b0).asSet()) LET \$a0=(SELECT FROM (SELECT expand(in('OnBoard_link')) FROM :targetIds1) WHERE @class='${Issues.CLASS}'), \$b0=(SELECT FROM (SELECT expand(in('OnBoard_link')) FROM :targetIds2) WHERE @class='${Issues.CLASS}'))",
//                expectedParams = mapOf(
//                    "targetIds1" to listOf(test.board1.id.asOId()),
//                    "targetIds2" to listOf(test.board2.id.asOId()),
//                )
//            )
            assertThat(issuesDistinct).hasSize(3)
            assertNamesExactly(issuesDistinct, "issue1", "issue2", "issue3")
        }
    }

    @Test
    fun `all minus find by property`() {
        // Given
        val test = givenTestCase()
        withStoreTx {
            test.issue1.setProperty("complex", "true")
            test.issue2.setProperty("complex", "true")
        }

        // When
        withStoreTx { tx ->
            val issues = tx.getAll(Issues.CLASS)
            val complexIssues = tx.find(Issues.CLASS, "complex", "true")
            val simpleIssues = issues.minus(complexIssues)

            // Then
            // todo
//            tx.checkSql(
//                simpleIssues,
//                expectedSql = "SELECT FROM Issue WHERE NOT (complex = :complex0)",
//                expectedParams = mapOf("complex0" to "true")
//            )
            assertNamesExactly(simpleIssues, "issue3")
        }
    }

    @Test
    fun `find by property minus find by property`() {
        // Given
        val test = givenTestCase()
        withStoreTx {
            test.issue1.setProperty("complex", "true")
            test.issue1.setProperty("blocked", "true")

            test.issue2.setProperty("complex", "true")
            test.issue2.setProperty("blocked", "false")

            test.issue3.setProperty("complex", "false")
            test.issue3.setProperty("blocked", "true")

        }

        // When
        withStoreTx { tx ->
            val complexIssues = tx.find(Issues.CLASS, "complex", "true")
            val blockedIssues = tx.find(Issues.CLASS, "blocked", "true")
            val complexUnblockedIssues = complexIssues.minus(blockedIssues)

            // Then
            // todo
//            tx.checkSql(
//                complexUnblockedIssues,
//                expectedSql = "SELECT FROM Issue WHERE (complex = :complex0 AND NOT (blocked = :blocked1))",
//                expectedParams = mapOf("complex0" to "true", "blocked1" to "true")
//            )
            assertNamesExactly(complexUnblockedIssues, "issue2")
        }
    }

    @Test
    fun `find by link minus find by link`() {
        // Given
        val test = givenTestCase()

        withStoreTx { tx ->
            tx.addIssueToBoard(test.issue1, test.board1)
            tx.addIssueToBoard(test.issue1, test.board2)
            tx.addIssueToBoard(test.issue2, test.board1)
            tx.addIssueToBoard(test.issue3, test.board1)
        }

        // When
        withStoreTx { tx ->
            val issuesOnBoard1 = tx.findLinks(Issues.CLASS, test.board1, Issues.Links.ON_BOARD)
            val issuesOnBoard2 = tx.findLinks(Issues.CLASS, test.board2, Issues.Links.ON_BOARD)

            val res1 = tx.g()
                .V()
                .has("localEntityId", 1)
                .`in`("OnBoard_link")
                .hasLabel("Issue")
                .aggregate("result1")
                .V()
                .has("localEntityId", 0)
                .`in`("OnBoard_link")
                .hasLabel("Issue")
                .where(P.without("result1"))
                .toList()


            // Then
            // todo
//            tx.checkSql(
//                issues,
//                expectedSql = "SELECT expand(difference(\$a0, \$b0)) LET \$a0=(SELECT FROM (SELECT expand(in('OnBoard_link')) FROM :targetIds1) WHERE @class='${Issues.CLASS}'), \$b0=(SELECT FROM (SELECT expand(in('OnBoard_link')) FROM :targetIds2) WHERE @class='${Issues.CLASS}')",
//                expectedParams = mapOf(
//                    "targetIds1" to listOf(test.board1.id.asOId()),
//                    "targetIds2" to listOf(test.board2.id.asOId()),
//                ),
//            )
            val issues = issuesOnBoard1.minus(issuesOnBoard2)
            assertNamesExactly(issues, "issue2", "issue3")
        }
    }


    @Test
    fun `iterable skip 1`() {
        // Given
        givenTestCase()

        // When
        withStoreTx { tx ->
            val issues = tx.sort(Issues.CLASS, "name", true).skip(1)

            // Then
            // todo:
//            tx.checkSql(
//                issues,
//                expectedSql = "SELECT FROM Issue ORDER BY name ASC SKIP :skip0",
//                expectedParams = mapOf("skip0" to 1)
//            )
            assertNamesExactlyInOrder(issues, "issue2", "issue3")
        }
    }

    @Test
    fun `iterable take 2`() {
        // Given
        givenTestCase()

        // When
        withStoreTx { tx ->
            val issues = tx.sort(Issues.CLASS, "name", true).take(2)

            // Then
            // todo
//            tx.checkSql(
//                issues,
//                expectedSql = "SELECT FROM Issue ORDER BY name ASC LIMIT :limit0",
//                expectedParams = mapOf("limit0" to 2)
//            )
            assertNamesExactlyInOrder(issues, "issue1", "issue2")
        }
    }

    @Test
    fun `iterable skip 1 and take 2`() {
        // Given
        givenTestCase()

        // When
        withStoreTx { tx ->
            val issues = tx.sort(Issues.CLASS, "name", true).skip(1).take(2)

            // Then
            // todo
//            tx.checkSql(
//                issues,
//                expectedSql = "SELECT FROM Issue ORDER BY name ASC SKIP :skip0 LIMIT :limit1",
//                expectedParams = mapOf("skip0" to 1, "limit1" to 2)
//            )
            assertNamesExactlyInOrder(issues, "issue2", "issue3")
        }
    }

    @Test
    fun `iterable sort and reverse`() {
        // Given
        givenTestCase()

        // When
        withStoreTx { tx ->

            val reversedByName =
                tx.sort(Issues.CLASS, "name", true).reverse()
            val reversedTwice =
                reversedByName.reverse()

            // Then
            // todo
//            tx.checkSql(
//                reversedByName,
//                expectedSql = "SELECT FROM Issue ORDER BY name DESC"
//            )
            assertNamesExactlyInOrder(reversedByName, "issue3", "issue2", "issue1")
            assertNamesExactlyInOrder(reversedTwice, "issue1", "issue2", "issue3")
        }
    }

    @Test
    fun `find issues (as iterable) on boards (as iterable)`() {
        // Given
        val test = givenTestCase()

        withStoreTx { tx ->
            tx.addIssueToBoard(test.issue1, test.board1)
            tx.addIssueToBoard(test.issue1, test.board2)
            tx.addIssueToBoard(test.issue2, test.board1)
            tx.addIssueToBoard(test.issue3, test.board3)
        }

        // When
        withStoreTx { tx ->
            // boards 1 and 2
            val boards = tx.find(Boards.CLASS, "name", test.board1.name())
                .union(tx.find(Boards.CLASS, "name", test.board2.name()))
            val allIssues = tx.getAll(Issues.CLASS)
            val issuesOnBoards =
                allIssues.findLinks(boards, Issues.Links.ON_BOARD)

            // Then
            // todo
//            tx.checkSql(
//                issuesOnBoards,
//                expectedSql = "SELECT expand(intersect(\$a0, \$b0)) LET \$a0=(SELECT FROM Issue), \$b0=(SELECT expand(in('OnBoard_link')) FROM (SELECT FROM Board WHERE (name = :name1 OR name = :name2)))",
//                expectedParams = mapOf(
//                    "name1" to "board1",
//                    "name2" to "board2"
//                )
//            )
            assertNamesExactly(issuesOnBoards, "issue1", "issue2")
        }
    }

    @Test
    fun `should throw exception for iterable with skip and take while intersect`() {
        // Given
        givenTestCase()

        // When
        withStoreTx { tx ->
            val skippedIssues = tx.getAll(Issues.CLASS).skip(1).take(2)
            val limitIssues = tx.getAll(Issues.CLASS).take(1)
            val issues = skippedIssues.intersect(limitIssues)

            // Then
            assertFailsWith<IllegalStateException>("Skip can not be used for sub-query") { issues.toList() }
        }
    }

    @Test
    fun `should throw exception for iterable with skip and take while union`() {
        // Given
        givenTestCase()

        // When
        withStoreTx { tx ->
            val skippedIssues = tx.getAll(Issues.CLASS).skip(1).take(1)
            val limitIssues = tx.getAll(Issues.CLASS).take(2)
            val issues = skippedIssues.union(limitIssues)

            // Then
            assertFailsWith<IllegalStateException>("Skip can not be used for sub-query") { issues.toList() }
        }
    }

    @Test
    fun `sort iterable and get first`() {
        // Given
        givenTestCase()

        // When
        withStoreTx { tx ->
            val sortedIssues = tx.sort(Issues.CLASS, "name", true)
            val firstIssue = sortedIssues.first!!

            // Then
            // todo
//            tx.checkSql(
//                sortedIssues,
//                expectedSql = "SELECT FROM Issue ORDER BY name ASC"
//            )
            assertThat(firstIssue.getProperty("name")).isEqualTo("issue1")
        }
    }

    @Test
    fun `sort iterable and get last`() {
        // Given
        givenTestCase()

        // When
        withStoreTx { tx ->
            val issue = tx.sort(Issues.CLASS, "name", true).last!!

            // Then
            assertThat(issue.getProperty("name")).isEqualTo("issue3")
        }
    }

    @Test
    fun `an EntityIterable can be used in different transactions`() {
        givenTestCase()
        val allIssues = withStoreTx { it.getAll(Issues.CLASS) }
        val result = withStoreTx { allIssues.toList() }
        assertEquals(3, result.size)
    }

    @Test
    fun `roughCount() = roughSize(), count() = size()`() {
        // Given
        givenTestCase()

        // When
        withStoreTx { tx ->
            val allIssues = tx.getAll(Issues.CLASS)

            assertEquals(3, allIssues.roughCount)
            assertEquals(3, allIssues.roughSize)
            assertEquals(3, allIssues.count())
            assertEquals(3, allIssues.size())

            // one more issue
            tx.newEntity(Issues.CLASS)

            // uses previously calculated value
            assertEquals(3, allIssues.roughCount)
            assertEquals(3, allIssues.roughSize)
            // calculates the actual one
            assertEquals(4, allIssues.count())
            assertEquals(4, allIssues.size())
            assertEquals(4, allIssues.roughCount)
            assertEquals(4, allIssues.roughSize)
        }
    }

    @Test
    fun `instance of should work`() {
        // Create 10 Issue and 1 SubIssue and their classes
        youTrackDb.provider.acquireSession().use { session ->
            val subIssue = session.getOrCreateVertexClass("ChildIssue")
            val issueClass = session.getOrCreateVertexClass(Issues.CLASS)
            subIssue.addSuperClass(issueClass)
        }
        (1..10).forEach {
            youTrackDb.createIssue("issue$it")
        }
        withStoreTx { tx ->
            tx.newEntity("ChildIssue")
        }

        withStoreTx { txn ->
            val childIssues = YTDBInstanceOfIterable(txn, "Issue", "ChildIssue", false)
            val notChildIssues = YTDBInstanceOfIterable(txn, "Issue", "ChildIssue", true)
            assertEquals(10, notChildIssues.toList().size)
            assertEquals(1, childIssues.toList().size)
        }
    }

    @Test
    fun `count should select the number of records`() {
        givenTestCase()

        withStoreTx { tx ->
            val issue1 = tx.find(Issues.CLASS, "name", "issue1")
            val issue2 = tx.find(Issues.CLASS, "name", "issue2")
            val issue4 = tx.find(Issues.CLASS, "name", "issue4")

            assertThat(issue1.size()).isEqualTo(1)
            assertThat(issue2.count()).isEqualTo(1)
            assertThat(issue4.count()).isEqualTo(0)

            assertThat(issue1.union(issue2).union(issue4).count()).isEqualTo(2)
        }
    }

    @Test
    fun `count should select the number of records with distinct`() {
        // Given
        val test = givenTestCase()

        withStoreTx { tx ->
            tx.addIssueToBoard(test.issue1, test.board1)
            tx.addIssueToBoard(test.issue2, test.board1)
            tx.addIssueToBoard(test.issue1, test.board2)
        }

        withStoreTx { tx ->

            val boards =
                tx.find(Boards.CLASS, "name", test.board1.name())
                    .union(
                        tx.find(Boards.CLASS, "name", test.board2.name())
                    )

            val issues = boards.selectDistinct(Boards.Links.HAS_ISSUE)

            assertThat(issues.toList().size).isEqualTo(2)
            assertThat(issues.count()).isEqualTo(2)
        }
    }

    @Test
    fun `count should count links`() {
        val test = givenTestCase()

        withStoreTx { tx ->
            tx.addIssueToBoard(test.issue1, test.board1)
            tx.addIssueToBoard(test.issue2, test.board1)
        }

        withStoreTx { tx ->

            val board1 =
                tx.find(Boards.CLASS, "name", test.board1.name())

            val issues = tx.findLinks(Issues.CLASS, board1, Issues.Links.ON_BOARD)

            assertThat(issues.toList().count()).isEqualTo(2)
            assertThat(issues.count()).isEqualTo(2)
        }
    }

    private fun checkGremlin(
        iterable: GremlinEntityIterable,
    ) {
        //  todo:
//        iterable.query.traverse(tx)
//        val sql = this.buildSql(iterable.query())
//        assertEquals(expectedSql, sql.sql)
//        assertContentEquals(expectedParams.toList(), sql.params.toList())
    }

    private fun YTDBStoreTransaction.checkSql(
        iterable: YTDBEntityIterable,
        expectedSql: String,
        expectedParams: Map<String, Any> = mapOf()
    ) {
        val sql = this.buildSql(iterable.query())
        assertEquals(expectedSql, sql.sql)
        assertContentEquals(expectedParams.toList(), sql.params.toList())
    }
}
