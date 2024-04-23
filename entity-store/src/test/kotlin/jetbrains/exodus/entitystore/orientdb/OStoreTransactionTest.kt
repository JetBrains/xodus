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
package jetbrains.exodus.entitystore.orientdb

import com.google.common.truth.Truth.assertThat
import com.orientechnologies.orient.core.record.OElement
import com.orientechnologies.orient.core.record.OVertex
import jetbrains.exodus.entitystore.ComparableGetter
import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.orientdb.iterate.OQueryEntityIterableBase
import jetbrains.exodus.entitystore.orientdb.testutil.*
import org.junit.Ignore
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
            assertNamesExactlyInOrder(issues, "issue1", "issue2", "issue3")
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
            assertNamesExactlyInOrder(result, "issue2")
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
            assertNamesExactlyInOrder(issues, "issue2")
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
            assertNamesExactlyInOrder(issues, "issue2")
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
            assertNamesExactlyInOrder(exclusive, "issue2")
            assertNamesExactlyInOrder(inclusiveMin, "issue2")
            assertNamesExactlyInOrder(inclusiveMax, "issue2")
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
            assertNamesExactlyInOrder(issues, "issue2")
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
            assertNamesExactlyInOrder(issues, "issue1", "issue2")
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
            assertNamesExactlyInOrder(issuesAscending, "issue1", "issue2", "issue3")
            assertNamesExactlyInOrder(issuesDescending, "issue3", "issue2", "issue1")
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
            val issues = tx.findWithProp(Issues.CLASS, "order")
            val issuesAscending = tx.sort(Issues.CLASS, "order", issues, true)
            val issuesDescending = tx.sort(Issues.CLASS, "order", issues, false)

            // Then
            assertNamesExactlyInOrder(issuesAscending, "issue1", "issue3")
            assertNamesExactlyInOrder(issuesDescending, "issue3", "issue1")
        }
    }

    @Test
    fun `should sort iterable by two properties`() {
        // Given
        val test = givenTestCase()

        orientDb.withSession {
            // Apple -> Appointment -> 3
            test.issue3.setProperty("project", "Apple")
            test.issue3.setProperty("type", "Appointment")

            // Apple -> Billing -> 1
            test.issue1.setProperty("project", "Apple")
            test.issue1.setProperty("type", "Billing")

            // Pear -> Appointment -> 2
            test.issue2.setProperty("project", "Pear")
            test.issue2.setProperty("type", "Appointment")
        }

        // When
        oTransactional { tx ->
            // Sorted by project then by type in ascending order
            val sortedByProject = tx.findWithPropSortedByValue(Issues.CLASS, "project")
            val issues = tx.sort(Issues.CLASS, "type", sortedByProject, true)

            // Then
            // Apple -> Appointment -> 3
            //       -> Billing     -> 1
            // Pear  -> Appointment -> 2
            assertNamesExactlyInOrder(issues, "issue3", "issue1", "issue2")
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
            assertNamesExactlyInOrder(issues, "issue1", "issue2")
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
            assertNamesExactlyInOrder(issues, "issue1", "issue2", "issue3")
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
            assertNamesExactlyInOrder(issuesOnBoard, "issue1", "issue2")
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
            assertNamesExactlyInOrder(issues, "issue1", "issue2", "issue3")
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
            assertNamesExactlyInOrder(issues, "issue2")
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
            assertNamesExactlyInOrder(issues, "issue1", "issue2")
        }
    }

    @Test
    @Ignore
    fun `should sort links by property`() {
        // Given
        val test = givenTestCase()

        // Issues assigned to projects in reverse order
        orientDb.addIssueToProject(test.issue1, test.project1)
        orientDb.addIssueToProject(test.issue2, test.project2)
        orientDb.addIssueToProject(test.issue3, test.project3)

        // When
        oTransactional { tx ->
            val projects = tx.getAll(Projects.CLASS)
            val issues = tx.getAll(Issues.CLASS)

            val projectsAsc = tx.sort(Projects.CLASS, "name", projects, true)
            val issuesAsc = tx.sortLinks(
                Issues.CLASS, // entity class
                projectsAsc, // links sorted asc by name
                false, // is multiple
                Issues.Links.IN_PROJECT, // link name
                issues // entities
            )

            val projectsDesc = tx.sort(Projects.CLASS, "name", projects, false)
            val issuesDesc = tx.sortLinks(
                Issues.CLASS, // entity class
                projectsDesc, // links sorted desc by name
                false, // is multiple
                Issues.Links.IN_PROJECT, // link name
                issues // entities
            )

            // Then
            // As sorted by project name
            // ToDo: should be fixed with https://youtrack.jetbrains.com/issue/XD-1010
            assertNamesExactlyInOrder(issuesAsc, "issue1", "issue2", "issue3")
            assertNamesExactlyInOrder(issuesDesc, "issue3", "issue2", "issue1")
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
            assertNamesExactlyInOrder(issuesAsc, "issue3", "issue2", "issue1")
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
            assertNamesExactlyInOrder(boards.sorted(), "board1", "board1", "board1", "board2")
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
            assertNamesExactlyInOrder(boards.sorted(), "board1", "board2")
        }
    }

    @Test
    fun `select by id range dummy test`() {
        // Given
        val test = givenTestCase()
        oTransactional {
            test.issue1.setProperty(OVertexEntity.BACKWARD_COMPATIBLE_LOCAL_ENTITY_ID_PROPERTY_NAME, 0)
            test.issue2.setProperty(OVertexEntity.BACKWARD_COMPATIBLE_LOCAL_ENTITY_ID_PROPERTY_NAME, 3)
            test.issue3.setProperty(OVertexEntity.BACKWARD_COMPATIBLE_LOCAL_ENTITY_ID_PROPERTY_NAME, 99)
        }

        // When
        oTransactional { tx ->
            val issues = tx.findIds(Issues.CLASS, 2, 100) as OQueryEntityIterableBase
            // Then
            assertNamesExactlyInOrder(issues, test.issue2.getProperty("name").toString(), test.issue3.getProperty("name").toString())
        }
    }
}
