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
package jetbrains.exodus.entitystore.orientdb.iterate

import com.google.common.truth.Truth.assertThat
import jetbrains.exodus.entitystore.orientdb.OEntityIterable
import jetbrains.exodus.entitystore.orientdb.OStoreTransaction
import jetbrains.exodus.entitystore.orientdb.getOrCreateVertexClass
import jetbrains.exodus.entitystore.orientdb.iterate.binop.OConcatEntityIterable
import jetbrains.exodus.entitystore.orientdb.iterate.binop.OIntersectionEntityIterable
import jetbrains.exodus.entitystore.orientdb.iterate.binop.OMinusEntityIterable
import jetbrains.exodus.entitystore.orientdb.iterate.binop.OUnionEntityIterable
import jetbrains.exodus.entitystore.orientdb.iterate.link.OLinkIsNullEntityIterable
import jetbrains.exodus.entitystore.orientdb.iterate.link.OLinkOfTypeToEntityIterable
import jetbrains.exodus.entitystore.orientdb.iterate.property.OInstanceOfIterable
import jetbrains.exodus.entitystore.orientdb.iterate.property.OPropertyEqualIterable
import jetbrains.exodus.entitystore.orientdb.iterate.property.OPropertyIsNullIterable
import jetbrains.exodus.entitystore.orientdb.query.buildSql
import jetbrains.exodus.entitystore.orientdb.testutil.*
import org.junit.Rule
import org.junit.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class OEntityIterableTest : OTestMixin {

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
            val issues = OPropertyIsNullIterable(tx, Issues.CLASS, "none")

            // Then
            tx.checkSql(
                issues,
                expectedSql = "SELECT FROM Issue WHERE none is null"
            )

            assertNamesExactlyInOrder(issues, "issue2", "issue3")
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
            val issues = OLinkIsNullEntityIterable(tx, Issues.CLASS, Issues.Links.IN_PROJECT)

            // Then
            tx.checkSql(
                issues,
                expectedSql = "SELECT FROM Issue WHERE outE('InProject_link').size() == 0"
            )

            assertNamesExactlyInOrder(issues, "issue2", "issue3")
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
            val issues = OPropertyEqualIterable(tx, Issues.CLASS, "opca", 300)

            // Then
            tx.checkSql(
                issues,
                expectedSql = "SELECT FROM Issue WHERE opca = :opca0",
                expectedParams = mapOf("opca0" to 300)
            )
            assertNamesExactlyInOrder(issues, "issue1", "issue3")
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

            val issues = equal1.union(equal2) as OUnionEntityIterable

            // Then
            tx.checkSql(
                issues,
                expectedSql = "SELECT FROM Issue WHERE (name = :name0 OR name = :name1)",
                expectedParams = mapOf("name0" to "issue1", "name1" to "issue2")
            )
            assertNamesExactlyInOrder(issues, "issue1", "issue2")
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

            val issues = equal1.union(equal2) as OUnionEntityIterable

            // Then
            tx.checkSql(
                issues,
                expectedSql = "SELECT FROM Issue WHERE (name = :name0 OR name = :name1)",
                expectedParams = mapOf("name0" to "issue1", "name1" to "issue1")
            )
            // Union operation can distinct result set if query is optimized to OR conditions
            assertNamesExactlyInOrder(issues, "issue1")
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
            val issues = nameEqual.intersect(priorityEqual) as OIntersectionEntityIterable

            // Then
            tx.checkSql(
                issues,
                expectedSql = "SELECT FROM Issue WHERE (name = :name0 AND priority = :priority1)",
                expectedParams = mapOf("name0" to "issue2", "priority1" to "normal")
            )
            assertNamesExactlyInOrder(issues, "issue2")
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
            val concat = issue1.concat(issue2) as OConcatEntityIterable

            // Then
            tx.checkSql(
                concat,
                expectedSql = "SELECT expand(unionall(\$a0, \$b0)) LET \$a0=(SELECT FROM Issue WHERE name = :name1), \$b0=(SELECT FROM Issue WHERE name = :name2)",
                expectedParams = mapOf("name1" to "issue1", "name2" to "issue2")
            )
            assertNamesExactlyInOrder(concat, "issue1", "issue2")

            val concatMore = concat.concat(issue1)
            assertNamesExactlyInOrder(concatMore, "issue1", "issue2", "issue1")
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
            ) as OLinkOfTypeToEntityIterable

            println(test.board1.id.asOId())
            // Then
            tx.checkSql(
                issues,
                expectedSql = "SELECT FROM (SELECT expand(in('OnBoard_link')) FROM [${test.board1.id.asOId()}]) WHERE @class='Issue'",
                expectedParams = mapOf()
            )
            assertNamesExactlyInOrder(issues, "issue1", "issue2")
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
            val concat = issuesOnBoard1.concat(issuesOnBoard2) as OConcatEntityIterable

            // Then
            tx.checkSql(
                concat,
                expectedSql = "SELECT expand(unionall(\$a0, \$b0)) LET \$a0=(SELECT FROM (SELECT expand(in('OnBoard_link')) FROM [${test.board1.id.asOId()}]) WHERE @class='Issue'), \$b0=(SELECT FROM (SELECT expand(in('OnBoard_link')) FROM [${test.board2.id.asOId()}]) WHERE @class='Issue')",
            )
            assertNamesExactlyInOrder(concat, "issue1", "issue2", "issue1")
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
            val issuesDistinct = issues.distinct() as ODistinctEntityIterable

            // Then
            tx.checkSql(
                issuesDistinct,
                expectedSql = "SELECT DISTINCT * FROM (SELECT expand(unionall(\$a0, \$b0).asSet()) LET \$a0=(SELECT FROM (SELECT expand(in('OnBoard_link')) FROM [${test.board1.id.asOId()}]) WHERE @class='Issue'), \$b0=(SELECT FROM (SELECT expand(in('OnBoard_link')) FROM [${test.board2.id.asOId()}]) WHERE @class='Issue'))"
            )
            assertThat(issuesDistinct).hasSize(3)
            assertNamesExactlyInOrder(issuesDistinct, "issue1", "issue2", "issue3")
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
            val simpleIssues = issues.minus(complexIssues) as OMinusEntityIterable

            // Then
            tx.checkSql(
                simpleIssues,
                expectedSql = "SELECT FROM Issue WHERE NOT (complex = :complex0)",
                expectedParams = mapOf("complex0" to "true")
            )
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
            val complexUnblockedIssues = complexIssues.minus(blockedIssues) as OMinusEntityIterable

            // Then
            tx.checkSql(
                complexUnblockedIssues,
                expectedSql = "SELECT FROM Issue WHERE (complex = :complex0 AND NOT (blocked = :blocked1))",
                expectedParams = mapOf("complex0" to "true", "blocked1" to "true")
            )
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
            val issues = issuesOnBoard1.minus(issuesOnBoard2) as OMinusEntityIterable

            // Then
            tx.checkSql(
                issues,
                expectedSql = "SELECT expand(difference(\$a0, \$b0)) LET \$a0=(SELECT FROM (SELECT expand(in('OnBoard_link')) FROM [${test.board1.id.asOId()}]) WHERE @class='Issue'), \$b0=(SELECT FROM (SELECT expand(in('OnBoard_link')) FROM [${test.board2.id.asOId()}]) WHERE @class='Issue')",
            )
            assertNamesExactly(issues, "issue2", "issue3")
        }
    }


    @Test
    fun `iterable skip 1`() {
        // Given
        givenTestCase()

        // When
        withStoreTx { tx ->
            val issues = tx.getAll(Issues.CLASS).skip(1) as OSkipEntityIterable

            // Then
            tx.checkSql(
                issues,
                expectedSql = "SELECT FROM Issue SKIP 1"
            )
            assertNamesExactlyInOrder(issues, "issue2", "issue3")
        }
    }

    @Test
    fun `iterable take 2`() {
        // Given
        givenTestCase()

        // When
        withStoreTx { tx ->
            val issues = tx.getAll(Issues.CLASS).take(2) as OTakeEntityIterable

            // Then
            tx.checkSql(
                issues,
                expectedSql = "SELECT FROM Issue LIMIT 2"
            )
            assertNamesExactlyInOrder(issues, "issue1", "issue2")
        }
    }

    @Test
    fun `iterable skip 1 and take 2`() {
        // Given
        givenTestCase()

        // When
        withStoreTx { tx ->
            val issues = tx.getAll(Issues.CLASS).skip(1).take(2) as OTakeEntityIterable

            // Then
            tx.checkSql(
                issues,
                expectedSql = "SELECT FROM Issue SKIP 1 LIMIT 2"
            )
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
                tx.sort(Issues.CLASS, "name", true).reverse() as OReversedEntityIterable

            // Then
            tx.checkSql(
                reversedByName,
                expectedSql = "SELECT FROM Issue ORDER BY name DESC"
            )
            assertNamesExactlyInOrder(reversedByName, "issue3", "issue2", "issue1")
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
            val allIssues = tx.getAll(Issues.CLASS) as OEntityIterableBase
            val issuesOnBoards =
                allIssues.findLinks(boards, Issues.Links.ON_BOARD) as OEntityIterable

            // Then
            tx.checkSql(
                issuesOnBoards,
                expectedSql = "SELECT expand(intersect(\$a0, \$b0)) LET \$a0=(SELECT FROM Issue), \$b0=(SELECT expand(in('OnBoard_link')) FROM (SELECT FROM Board WHERE (name = :name1 OR name = :name2)))",
                expectedParams = mapOf("name1" to "board1", "name2" to "board2")
            )
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
            val sortedIssues = tx.sort(Issues.CLASS, "name", true) as OEntityIterable
            val firstIssue = sortedIssues.first!!

            // Then
            tx.checkSql(
                sortedIssues,
                expectedSql = "SELECT FROM Issue ORDER BY name ASC"
            )
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
            val allIssues = tx.getAll(Issues.CLASS) as OEntityIterableBase

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
            subIssue.setSuperClasses(session, listOf(issueClass))
        }
        (1..10).forEach {
            youTrackDb.createIssue("issue$it")
        }
        withStoreTx { tx ->
            tx.newEntity("ChildIssue")
        }

        withStoreTx { txn ->
            val childIssues = OInstanceOfIterable(txn, "Issue", "ChildIssue", false)
            val notChildIssues = OInstanceOfIterable(txn, "Issue", "ChildIssue", true)
            assertEquals(10, notChildIssues.toList().size)
            assertEquals(1, childIssues.toList().size)
        }
    }

    private fun OStoreTransaction.checkSql(
        iterable: OEntityIterable,
        expectedSql: String,
        expectedParams: Map<String, Any> = mapOf()
    ) {
        val sql = this.buildSql(iterable.query())
        assertEquals(expectedSql, sql.sql)
        assertContentEquals(expectedParams.toList(), sql.params.toList())
    }
}
