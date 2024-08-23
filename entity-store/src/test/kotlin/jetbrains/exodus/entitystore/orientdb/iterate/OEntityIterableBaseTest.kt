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
import jetbrains.exodus.entitystore.orientdb.getOrCreateVertexClass
import jetbrains.exodus.entitystore.orientdb.iterate.link.OLinkIsNullEntityIterable
import jetbrains.exodus.entitystore.orientdb.iterate.property.OInstanceOfIterable
import jetbrains.exodus.entitystore.orientdb.iterate.property.OPropertyIsNullIterable
import jetbrains.exodus.entitystore.orientdb.testutil.*
import org.junit.Assert
import org.junit.Rule
import org.junit.Test
import kotlin.test.assertEquals

class OEntityIterableBaseTest : OTestMixin {

    @Rule
    @JvmField
    val orientDbRule = InMemoryOrientDB()

    override val orientDb = orientDbRule

    @Test
    fun `should iterable property is null`() {
        // Given
        val test = givenTestCase()
        withStoreTx {
            test.issue1.setProperty("none", "n1")
        }

        // When
        withStoreTx { tx ->
            val issues = OPropertyIsNullIterable(tx, Issues.CLASS, "none")

            // Then
            assertNamesExactlyInOrder(issues, "issue2", "issue3")
        }
    }

    @Test
    fun `should iterable link is null`() {
        // Given
        val test = givenTestCase()
        withStoreTx { tx ->
            tx.addIssueToProject(test.issue1, test.project1)
        }

        // When
        withStoreTx { tx ->
            val issues = OLinkIsNullEntityIterable(tx, Issues.CLASS, Issues.Links.IN_PROJECT)

            // Then
            assertNamesExactlyInOrder(issues, "issue2", "issue3")
        }
    }

    @Test
    fun `should iterable union different issues`() {
        // Given
        val test = givenTestCase()

        // When
        withStoreTx { tx ->
            val equal1 = tx.find(Issues.CLASS, "name", test.issue1.name())
            val equal2 = tx.find(Issues.CLASS, "name", test.issue2.name())

            val issues = equal1.union(equal2)

            // Then
            assertNamesExactlyInOrder(issues, "issue1", "issue2")
        }
    }

    @Test
    fun `should iterable union same issue`() {
        // Given
        val test = givenTestCase()

        // When
        withStoreTx { tx ->
            val equal1 = tx.find(Issues.CLASS, "name", test.issue1.name())
            val equal2 = tx.find(Issues.CLASS, "name", test.issue1.name())

            val issues = equal1.union(equal2)

            // Then
            // Union operation can distinct result set if query is optimized to OR conditions
            assertNamesExactlyInOrder(issues, "issue1")
        }
    }

    @Test
    fun `should iterable intersect`() {
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
            assertNamesExactlyInOrder(issues, "issue2")
            assertThat(issues.first().getProperty("priority")).isEqualTo("normal")
        }
    }

    @Test
    fun `should iterable concat with properties`() {
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
            val concat = issue1.concat(issue2).concat(issue1)

            // Then
            assertNamesExactlyInOrder(concat, "issue1", "issue2", "issue1")
        }
    }

    @Test
    fun `should iterable concat with links`() {
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
            assertNamesExactlyInOrder(concat, "issue1", "issue2", "issue1")
        }
    }

    @Test
    fun `should iterable distinct`() {
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
            assertThat(issuesDistinct).hasSize(3)
            assertNamesExactlyInOrder(issuesDistinct, "issue1", "issue2", "issue3")
        }
    }

    @Test
    fun `should iterable minus with properties and all`() {
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
            assertNamesExactly(simpleIssues, "issue3")
        }
    }

    @Test
    fun `should iterable minus with properties`() {
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
            assertNamesExactly(complexUnblockedIssues, "issue2")
        }
    }

    @Test
    fun `should iterable minus with links`() {
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
            val issues = issuesOnBoard1.minus(issuesOnBoard2)

            // Then
            assertNamesExactly(issues, "issue2", "issue3")
        }
    }


    @Test
    fun `should iterable skip`() {
        // Given
        givenTestCase()

        // When
        withStoreTx { tx ->
            val issues = tx.getAll(Issues.CLASS).skip(1)

            // Then
            assertNamesExactlyInOrder(issues, "issue2", "issue3")
        }
    }

    @Test
    fun `should iterable take`() {
        // Given
        givenTestCase()

        // When
        withStoreTx { tx ->
            val issues = tx.getAll(Issues.CLASS).take(2)

            // Then
            assertNamesExactlyInOrder(issues, "issue1", "issue2")
        }
    }

    @Test
    fun `should iterable skip and take`() {
        // Given
        givenTestCase()

        // When
        withStoreTx { tx ->
            val issues = tx.getAll(Issues.CLASS).skip(1).take(1)

            // Then
            assertNamesExactlyInOrder(issues, "issue2")
        }
    }

    @Test
    fun `should iterable reverse when with order`() {
        // Given
        givenTestCase()

        // When
        withStoreTx { tx ->
            val reversedByName = tx.sort(Issues.CLASS, "name", true).reverse()

            // Then
            assertNamesExactlyInOrder(reversedByName, "issue3", "issue2", "issue1")
        }
    }

    @Test
    fun `should iterable find links`() {
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
            val allIssues = tx.getAll(Issues.CLASS) as OQueryEntityIterableBase
            val issuesOnBoards = allIssues.findLinks(boards, Issues.Links.ON_BOARD)

            // Then
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
            val exception = Assert.assertThrows(IllegalStateException::class.java) { issues.toList() }
            assertThat(exception.message).contains("Skip can not be used for sub-query")
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
            val exception = Assert.assertThrows(IllegalStateException::class.java) { issues.toList() }
            assertThat(exception.message).contains("Skip can not be used for sub-query")
        }
    }

    @Test
    fun `should iterable get first`() {
        // Given
        givenTestCase()

        // When
        withStoreTx { tx ->
            val issue = tx.sort(Issues.CLASS, "name", true).first

            // Then
            assertThat(issue?.getProperty("name")).isEqualTo("issue1")
        }
    }

    @Test
    fun `should iterable get last`() {
        // Given
        givenTestCase()

        // When
        withStoreTx { tx ->
            val issue = tx.sort(Issues.CLASS, "name", true).last

            // Then
            assertThat(issue?.getProperty("name")).isEqualTo("issue3")
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
            val allIssues = tx.getAll(Issues.CLASS) as OQueryEntityIterableBase

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
        orientDb.provider.acquireSession().use { session ->
            val subIssue = session.getOrCreateVertexClass("ChildIssue")
            val issueClass = session.getOrCreateVertexClass(Issues.CLASS)
            subIssue.setSuperClasses(listOf(issueClass))
        }
        (1..10).forEach {
            orientDb.createIssue("issue$it")
        }
        withStoreTx { tx ->
            tx.newEntity("ChildIssue")
        }

        withStoreTx { txn ->
            val childIssues = OInstanceOfIterable(txn, "Issue", "ChildIssue", false)
            val notChildIssues = OInstanceOfIterable(txn, "Issue", "ChildIssue", true)
            Assert.assertEquals(10, notChildIssues.toList().size)
            Assert.assertEquals(1, childIssues.toList().size)
        }
    }


}
