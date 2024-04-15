package jetbrains.exodus.entitystore.orientdb.iterate

import com.google.common.truth.Truth.assertThat
import jetbrains.exodus.entitystore.orientdb.testutil.Boards
import jetbrains.exodus.entitystore.orientdb.testutil.InMemoryOrientDB
import jetbrains.exodus.entitystore.orientdb.testutil.Issues
import jetbrains.exodus.entitystore.orientdb.testutil.OTestMixin
import jetbrains.exodus.entitystore.orientdb.testutil.addIssueToBoard
import jetbrains.exodus.entitystore.orientdb.testutil.name
import jetbrains.exodus.testutil.eventually
import org.junit.Rule
import org.junit.Test

class OEntityIterableBaseTest : OTestMixin {

    @Rule
    @JvmField
    val orientDbRule = InMemoryOrientDB()

    override val orientDb = orientDbRule

    @Test
    fun `should iterable union different issues`() {
        // Given
        val test = givenTestCase()

        // When
        oTransactional { tx ->
            val equal1 = tx.find(Issues.CLASS, "name", test.issue1.name())
            val equal2 = tx.find(Issues.CLASS, "name", test.issue2.name())

            val issues = equal1.union(equal2)

            // Then
            assertNamesExactly(issues, "issue1", "issue2")
        }
    }

    @Test
    fun `should iterable union same issue`() {
        // Given
        val test = givenTestCase()

        // When
        oTransactional { tx ->
            val equal1 = tx.find(Issues.CLASS, "name", test.issue1.name())
            val equal2 = tx.find(Issues.CLASS, "name", test.issue1.name())

            val issues = equal1.union(equal2)

            // Then
            // Union operation can distinct result set if query is optimized to OR conditions
            assertNamesExactly(issues, "issue1")
        }
    }

    @Test
    fun `should iterable intersect`() {
        // Given
        val test = givenTestCase()
        orientDb.withSession {
            test.issue2.setProperty(Issues.Props.PRIORITY, "normal")
        }

        // When
        oTransactional { tx ->
            val nameEqual = tx.find(Issues.CLASS, "name", test.issue2.name())
            val priorityEqual = tx.find(Issues.CLASS, Issues.Props.PRIORITY, "normal")
            val issues = nameEqual.intersect(priorityEqual)

            // Then
            assertNamesExactly(issues, "issue2")
            assertThat(issues.first().getProperty("priority")).isEqualTo("normal")
        }
    }

    @Test
    fun `should iterable concat with properties`() {
        // Given
        val test = givenTestCase()

        orientDb.addIssueToBoard(test.issue1, test.board1)
        orientDb.addIssueToBoard(test.issue2, test.board1)
        orientDb.addIssueToBoard(test.issue1, test.board2)

        // When
        oTransactional { tx ->
            val issue1 = tx.find(Issues.CLASS, "name", "issue1")
            val issue2 = tx.find(Issues.CLASS, "name", "issue2")
            val concat = issue1.concat(issue2).concat(issue1)

            // Then
            assertNamesExactly(concat, "issue1", "issue2", "issue1")
        }
    }

    @Test
    fun `should iterable concat with links`() {
        // Given
        val test = givenTestCase()

        orientDb.addIssueToBoard(test.issue1, test.board1)
        orientDb.addIssueToBoard(test.issue2, test.board1)
        orientDb.addIssueToBoard(test.issue1, test.board2)

        // When
        oTransactional { tx ->
            val issuesOnBoard1 = tx.findLinks(Issues.CLASS, test.board1, Issues.Links.ON_BOARD)
            val issuesOnBoard2 = tx.findLinks(Issues.CLASS, test.board2, Issues.Links.ON_BOARD)
            val concat = issuesOnBoard1.concat(issuesOnBoard2)

            // Then
            assertNamesExactly(concat, "issue1", "issue2", "issue1")
        }
    }

    @Test
    fun `should iterable distinct`() {
        // Given
        val test = givenTestCase()

        orientDb.addIssueToBoard(test.issue1, test.board1)
        orientDb.addIssueToBoard(test.issue1, test.board2)
        orientDb.addIssueToBoard(test.issue2, test.board1)
        orientDb.addIssueToBoard(test.issue3, test.board1)

        // When
        oTransactional { tx ->
            val issuesOnBoard1 = tx.findLinks(Issues.CLASS, test.board1, Issues.Links.ON_BOARD)
            val issuesOnBoard2 = tx.findLinks(Issues.CLASS, test.board2, Issues.Links.ON_BOARD)
            val issues = issuesOnBoard1.union(issuesOnBoard2)
            val issuesDistinct = issues.distinct()

            // Then
            assertThat(issues).hasSize(4)
            assertNamesExactly(issuesDistinct, "issue1", "issue2", "issue3")
        }
    }

    @Test
    fun `should iterable minus`() {
        // Given
        val test = givenTestCase()

        orientDb.addIssueToBoard(test.issue1, test.board1)
        orientDb.addIssueToBoard(test.issue1, test.board2)
        orientDb.addIssueToBoard(test.issue2, test.board1)
        orientDb.addIssueToBoard(test.issue3, test.board1)

        // When
        oTransactional { tx ->
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
        oTransactional { tx ->
            val issues = tx.getAll(Issues.CLASS).skip(1)

            // Then
            assertNamesExactly(issues, "issue2", "issue3")
        }
    }

    @Test
    fun `should iterable take`() {
        // Given
        givenTestCase()

        // When
        oTransactional { tx ->
            val issues = tx.getAll(Issues.CLASS).take(2)

            // Then
            assertNamesExactly(issues, "issue1", "issue2")
        }
    }

    @Test
    fun `should iterable skip and take`() {
        // Given
        givenTestCase()

        // When
        oTransactional { tx ->
            val issues = tx.getAll(Issues.CLASS).skip(1).take(1)

            // Then
            assertNamesExactly(issues, "issue2")
        }
    }

    @Test
    fun `should iterable find links`() {
        // Given
        val test = givenTestCase()

        orientDb.addIssueToBoard(test.issue1, test.board1)
        orientDb.addIssueToBoard(test.issue1, test.board2)
        orientDb.addIssueToBoard(test.issue2, test.board1)
        orientDb.addIssueToBoard(test.issue3, test.board3)

        // When
        oTransactional { tx ->
            // boards 1 and 2
            val boards = tx.find(Boards.CLASS, "name", test.board1.name())
                .union(tx.find(Boards.CLASS, "name", test.board2.name()))
            val allIssues = tx.getAll(Issues.CLASS) as OQueryEntityIterableBase
            val issuesOnBoards = allIssues.findLinks(boards, Issues.Links.ON_BOARD)!!

            // Then
            assertNamesExactly(issuesOnBoards, "issue1", "issue2")
        }
    }

    @Test
    fun `should iterable size`() {
        // Given
        givenTestCase()

        // When
        oTransactional { tx ->
            // boards 1 and 2
            val allIssues = tx.getAll(Issues.CLASS) as OQueryEntityIterableBase

            // Then
            assertThat(allIssues.size()).isEqualTo(3)
        }
    }

    @Test
    fun `should iterable count`() {
        // Given
        givenTestCase()

        // When
        oTransactional { tx ->
            val allIssues = tx.getAll(Issues.CLASS) as OQueryEntityIterableBase

            // Then
            // Count is not calculated yet
            assertThat(allIssues.count()).isEqualTo(-1)
            // Wait until the count is updated asynchronously
            eventually { assertThat(allIssues.count()).isEqualTo(3) }
        }
    }

    @Test
    fun `should iterable rough size sync`() {
        // Given
        givenTestCase()

        // When
        oTransactional { tx ->
            val allIssues = tx.getAll(Issues.CLASS) as OQueryEntityIterableBase

            // Then
            assertThat(allIssues.roughSize).isEqualTo(3)
        }
    }

    @Test
    fun `should iterable rough count`() {
        // Given
        givenTestCase()

        // When
        oTransactional { tx ->
            val allIssues = tx.getAll(Issues.CLASS) as OQueryEntityIterableBase

            // Then
            assertThat(allIssues.roughCount).isEqualTo(-1)
            assertThat(allIssues.count()).isEqualTo(-1)
            // Wait until the count is updated asynchronously
            eventually { assertThat(allIssues.roughCount).isEqualTo(3) }
        }
    }
}