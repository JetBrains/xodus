package jetbrains.exodus.entitystore.orientdb

import com.google.common.truth.Truth.assertThat
import com.orientechnologies.orient.core.record.OElement
import com.orientechnologies.orient.core.record.OVertex
import jetbrains.exodus.entitystore.ComparableGetter
import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.orientdb.iterate.OQueryEntityIterableBase
import jetbrains.exodus.entitystore.orientdb.testutil.*
import org.junit.Rule
import org.junit.Test

class OStoreTransactionTest : OTestMixin {

    @Rule
    @JvmField
    val orientDbRule = InMemoryOrientDB()

    override val orientDb = orientDbRule

    @Test
    fun `should find all`() {
        // Given
        givenTestCase()

        // When
        oTransactional { tx ->
            val issues = tx.getAll(Issues.CLASS)

            // Then
            assertNamesExactly(issues, "issue1", "issue2", "issue3")
        }
    }

    @Test
    fun `should find property equal`() {
        // Given
        givenTestCase()

        // When
        oTransactional { tx ->
            val result = tx.find(Issues.CLASS, "name", "issue2")

            // Then
            assertNamesExactly(result, "issue2")
        }
    }

    @Test
    fun `should find property contains`() {
        // Given
        val test = givenTestCase()
        orientDb.withSession {
            test.issue2.setProperty("case", "Find me if YOU can")
        }

        // When
        oTransactional { tx ->
            val issues = tx.findContaining(Issues.CLASS, "case", "YOU", true)
            val empty = tx.findContaining(Issues.CLASS, "case", "not", true)

            // Then
            assertNamesExactly(issues, "issue2")
            assertThat(empty).isEmpty()
        }
    }

    @Test
    fun `should find property starts with`() {
        // Given
        val test = givenTestCase()
        orientDb.withSession { test.issue2.setProperty("case", "Find me if YOU can") }

        // When
        oTransactional { tx ->
            val issues = tx.findStartingWith(Issues.CLASS, "case", "Find")
            val empty = tx.findStartingWith(Issues.CLASS, "case", "you")

            // Then
            assertNamesExactly(issues, "issue2")
            assertThat(empty).isEmpty()
        }
    }

    @Test
    fun `should find property in range`() {
        // Given
        val test = givenTestCase()
        orientDb.withSession { test.issue2.setProperty("value", 3) }

        // When
        oTransactional { tx ->
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
    fun `should find property exists`() {
        // Given
        val test = givenTestCase()

        orientDb.withSession { test.issue2.setProperty("prop", "test") }

        // When
        oTransactional { tx ->
            val issues = tx.findWithProp(Issues.CLASS, "prop")
            val empty = tx.findWithProp(Issues.CLASS, "no_prop")

            // Then
            assertNamesExactly(issues, "issue2")
            assertThat(empty).isEmpty()
        }
    }

    @Test
    fun `should find entity with blob`() {
        // Given
        val test = givenTestCase()

        orientDb.withSession {
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

        // When
        oTransactional { tx ->
            val issues = tx.findWithBlob(Issues.CLASS, "myBlob")

            // Then
            assertNamesExactly(issues, "issue1", "issue2")
        }
    }

    @Test
    fun `should sorted by property`() {
        // Given
        val test = givenTestCase()

        orientDb.withSession {
            test.issue1.setProperty("order", "1")
            test.issue2.setProperty("order", "2")
            test.issue3.setProperty("order", "3")
        }

        // When
        oTransactional { tx ->
            val issuesAscending = tx.sort(Issues.CLASS, "order", true)
            val issuesDescending = tx.sort(Issues.CLASS, "order", false)

            // Then
            assertNamesExactly(issuesAscending, "issue1", "issue2", "issue3")
            assertNamesExactly(issuesDescending, "issue3", "issue2", "issue1")
        }
    }

    @Test
    fun `should sort iterable by property`() {
        // Given
        val test = givenTestCase()

        orientDb.withSession {
            test.issue1.setProperty("order", "1")
            test.issue3.setProperty("order", "3")
        }

        // When
        oTransactional { tx ->
            val issues = tx.findWithPropSortedByValue(Issues.CLASS, "order")
            val issuesAscending = tx.sort(Issues.CLASS, "order", issues, true)
            val issuesDescending = tx.sort(Issues.CLASS, "order", issues, false)

            // Then
            assertNamesExactly(issuesAscending, "issue1", "issue3")
            assertNamesExactly(issuesDescending, "issue3", "issue1")
        }
    }

    @Test
    fun `should find links by link entity id`() {
        // Given
        val testCase = givenTestCase()

        orientDb.addIssueToProject(testCase.issue1, testCase.project1)
        orientDb.addIssueToProject(testCase.issue2, testCase.project1)
        orientDb.addIssueToProject(testCase.issue3, testCase.project2)

        // When
        oTransactional { tx ->
            val issues = tx.findLinks(Issues.CLASS, testCase.project1, Issues.Links.IN_PROJECT)

            // Then
            assertNamesExactly(issues, "issue1", "issue2")
        }
    }

    @Test
    fun `should find links by link iterables`() {
        // Given
        val testCase = givenTestCase()

        orientDb.addIssueToProject(testCase.issue1, testCase.project1)
        orientDb.addIssueToProject(testCase.issue2, testCase.project1)
        orientDb.addIssueToProject(testCase.issue3, testCase.project2)

        // When
        oTransactional { tx ->
            val projects = tx.getAll(Projects.CLASS)
            val issues = tx.findLinks(Issues.CLASS, projects, Issues.Links.IN_PROJECT)

            // Then
            assertNamesExactly(issues, "issue1", "issue2", "issue3")
        }
    }

    @Test
    fun `should find with links`() {
        // Given
        val test = givenTestCase()

        orientDb.addIssueToBoard(test.issue1, test.board1)
        orientDb.addIssueToBoard(test.issue2, test.board2)

        // When
        oTransactional { tx ->
            val issuesOnBoard = tx.findWithLinks(Issues.CLASS, Issues.Links.ON_BOARD)
            val issuesInProject = tx.findWithLinks(Issues.CLASS, Issues.Links.IN_PROJECT)

            // Then
            assertNamesExactly(issuesOnBoard, "issue1", "issue2")
            assertThat(issuesInProject).isEmpty()
        }
    }

    @Test
    fun `should find links and iterable union`() {
        // Given
        val testCase = givenTestCase()

        orientDb.addIssueToProject(testCase.issue1, testCase.project1)
        orientDb.addIssueToProject(testCase.issue2, testCase.project1)
        orientDb.addIssueToProject(testCase.issue3, testCase.project2)

        // When
        oTransactional { tx ->
            // Find all issues that in project1 or project2
            val issuesInProject1 = tx.findLinks(Issues.CLASS, testCase.project1, Issues.Links.IN_PROJECT)
            val issuesInProject2 = tx.findLinks(Issues.CLASS, testCase.project2, Issues.Links.IN_PROJECT)
            val issues = issuesInProject1.union(issuesInProject2)

            // Then
            assertNamesExactly(issues, "issue1", "issue2", "issue3")
        }
    }

    @Test
    fun `should find links and iterable intersect`() {
        // Given
        val test = givenTestCase()

        orientDb.addIssueToBoard(test.issue1, test.board1)
        orientDb.addIssueToBoard(test.issue2, test.board1)
        orientDb.addIssueToBoard(test.issue2, test.board2)
        orientDb.addIssueToBoard(test.issue3, test.board3)

        // When
        oTransactional { tx ->
            // Find all issues that are on board1 and board2 at the same time
            val issuesOnBoard1 = tx.findLinks(Issues.CLASS, test.board1, Issues.Links.ON_BOARD)
            val issuesOnBoard2 = tx.findLinks(Issues.CLASS, test.board2, Issues.Links.ON_BOARD)
            val issues = issuesOnBoard1.intersect(issuesOnBoard2)

            // Then
            assertNamesExactly(issues, "issue2")
        }
    }

    @Test
    fun `should find different links and iterable union`() {
        // Given
        val test = givenTestCase()

        orientDb.addIssueToProject(test.issue1, test.project1)
        orientDb.addIssueToBoard(test.issue2, test.board2)
        orientDb.addIssueToBoard(test.issue3, test.board3)

        // When
        oTransactional { tx ->
            // Find all issues that are either in project1 or board2
            val issuesOnBoard1 = tx.findLinks(Issues.CLASS, test.project1, Issues.Links.IN_PROJECT)
            val issuesOnBoard2 = tx.findLinks(Issues.CLASS, test.board2, Issues.Links.ON_BOARD)
            val issues = issuesOnBoard1.union(issuesOnBoard2)

            // Then
            assertNamesExactly(issues, "issue1", "issue2")
        }
    }

    @Test
    fun `should sort links by property`() {
        // Given
        val test = givenTestCase()

        orientDb.addIssueToProject(test.issue1, test.project3)
        orientDb.addIssueToProject(test.issue2, test.project2)
        orientDb.addIssueToProject(test.issue3, test.project1)

        // When
        oTransactional { tx ->
            val links = tx.getAll(Projects.CLASS)
            val issues = tx.getAll(Issues.CLASS)

            val issueAsc = tx.sortLinks(
                Issues.CLASS, // entity class
                tx.sort(Projects.CLASS, "name", links, false), // links sorted asc by name
                false, // is multiple
                Issues.Links.IN_PROJECT, // link name
                issues // entities
            )
            val issuesDesc = tx.sortLinks(
                Issues.CLASS, // entity class
                tx.sort(Projects.CLASS, "name", links, false), // links sorted desc by name
                false, // is multiple
                Issues.Links.IN_PROJECT, // link name
                issues // entities
            )

            // Then
            // As sorted by project name
            assertNamesExactly(issueAsc, "issue3", "issue2", "issue1")
            assertNamesExactly(issuesDesc, "issue1", "issue2", "issue3")
        }
    }

    @Test
    fun `should sort links by property distinct`() {
        // Given
        val test = givenTestCase()

        // Issues assigned to projects in reverse order
        orientDb.addIssueToProject(test.issue1, test.project3)
        orientDb.addIssueToProject(test.issue1, test.project2)

        orientDb.addIssueToProject(test.issue2, test.project2)
        orientDb.addIssueToProject(test.issue2, test.project1)

        orientDb.addIssueToProject(test.issue3, test.project1)
        orientDb.addIssueToProject(test.issue3, test.project2)

        // When
        oTransactional { tx ->
            val links = tx.getAll(Projects.CLASS)
            val issues = tx.getAll(Issues.CLASS)

            // Find all issues that are either in project1 or board2
            val issuesAsc = tx.sortLinks(
                Issues.CLASS, // entity class
                tx.sort(Projects.CLASS, "name", links, false), // links sorted desc by name
                false, // is multiple
                Issues.Links.IN_PROJECT, // link name
                issues // entities
            ).distinct()

            // Then
            assertNamesExactly(issuesAsc, "issue3", "issue2", "issue1")
        }
    }

    @Test
    fun `should select many links`() {
        // Given
        val test = givenTestCase()
        orientDb.addIssueToBoard(test.issue1, test.board1)
        orientDb.addIssueToBoard(test.issue1, test.board2)
        orientDb.addIssueToBoard(test.issue2, test.board1)
        orientDb.addIssueToBoard(test.issue3, test.board1)

        // When
        oTransactional { tx ->
            val issues = tx.getAll(Issues.CLASS) as OQueryEntityIterableBase
            val boards = issues.selectMany(Issues.Links.ON_BOARD)

            // Then
            assertNamesExactly(boards.sorted(), "board1", "board1", "board1", "board2")
        }
    }

    @Test
    fun `should select many links distinct`() {
        // Given
        val test = givenTestCase()

        orientDb.addIssueToBoard(test.issue1, test.board1)
        orientDb.addIssueToBoard(test.issue1, test.board2)
        orientDb.addIssueToBoard(test.issue2, test.board1)
        orientDb.addIssueToBoard(test.issue3, test.board1)

        // When
        oTransactional { tx ->
            val issues = tx.getAll(Issues.CLASS) as OQueryEntityIterableBase
            val boards = issues.selectDistinct(Issues.Links.ON_BOARD)

            // Then
            assertNamesExactly(boards.sorted(), "board1", "board2")
        }
    }

    @Test
    fun `should merge sorted issues with comparable getter`() {
        // Given
        val test = givenTestCase()
        val anotherIssue = orientDb.createIssue("issue4")

        orientDb.addIssueToBoard(test.issue1, test.board1)
        orientDb.addIssueToBoard(test.issue2, test.board2)
        orientDb.addIssueToBoard(test.issue3, test.board3)
        orientDb.addIssueToBoard(anotherIssue, test.board3)


        orientDb.withSession {
            test.issue1.setProperty(Issues.Props.PRIORITY, "1")
            test.issue2.setProperty(Issues.Props.PRIORITY, "2")
            test.issue3.setProperty(Issues.Props.PRIORITY, "3")
            anotherIssue.setProperty(Issues.Props.PRIORITY, "0")
        }

        // When
        oTransactional { tx ->
            val issuesOnBoard1 = tx.findLinks(Issues.CLASS, test.board1, Issues.Links.ON_BOARD)
            val issuesOnBoard2 = tx.findLinks(Issues.CLASS, test.board2, Issues.Links.ON_BOARD)
            val issuesOnBoard3 = tx.sort(
                Issues.CLASS,
                Issues.Props.PRIORITY,
                tx.findLinks(Issues.CLASS, test.board3, Issues.Links.ON_BOARD),
                true
            )

            val comparableGetter = ComparableGetter { it.getProperty(Issues.Props.PRIORITY) }
            val comparator = Comparator<Comparable<Any>?> { o1, o2 -> o1?.compareTo(o2!!) ?: -1 }

            val mergeSorted =
                tx.mergeSorted(listOf(issuesOnBoard2, issuesOnBoard1, issuesOnBoard3), comparableGetter, comparator)

            // Then
            assertNamesExactly(mergeSorted, "issue4", "issue1", "issue2", "issue3")
        }
    }

    @Test
    fun `should merge sorted issues`() {
        // Given
        val test = givenTestCase()
        val anotherIssue = orientDb.createIssue("issue4")

        orientDb.addIssueToBoard(test.issue1, test.board1)
        orientDb.addIssueToBoard(test.issue2, test.board2)
        orientDb.addIssueToBoard(test.issue3, test.board3)
        orientDb.addIssueToBoard(anotherIssue, test.board3)


        orientDb.withSession {
            test.issue1.setProperty(Issues.Props.PRIORITY, "1")
            test.issue2.setProperty(Issues.Props.PRIORITY, "2")
            test.issue3.setProperty(Issues.Props.PRIORITY, "3")
            anotherIssue.setProperty(Issues.Props.PRIORITY, "0")
        }

        // When
        oTransactional { tx ->
            val issuesOnBoard1 = tx.findLinks(Issues.CLASS, test.board1, Issues.Links.ON_BOARD)
            val issuesOnBoard2 = tx.findLinks(Issues.CLASS, test.board2, Issues.Links.ON_BOARD)
            val issuesOnBoard3 = tx.sort(
                Issues.CLASS,
                Issues.Props.PRIORITY,
                tx.findLinks(Issues.CLASS, test.board3, Issues.Links.ON_BOARD),
                true
            )

            val comparator = Comparator<Entity> { o1, o2 ->
                o1.getProperty(Issues.Props.PRIORITY)?.compareTo(o2.getProperty(Issues.Props.PRIORITY)) ?: -1
            }

            val mergeSorted =
                tx.mergeSorted(arrayListOf(issuesOnBoard2, issuesOnBoard1, issuesOnBoard3), comparator)

            // Then
            assertNamesExactly(mergeSorted, "issue4", "issue1", "issue2", "issue3")
        }
    }
}