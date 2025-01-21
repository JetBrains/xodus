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
package jetbrains.exodus.entitystore.youtrackdb

import com.google.common.truth.Truth.assertThat
import com.jetbrains.youtrack.db.api.exception.DatabaseException
import com.jetbrains.youtrack.db.api.record.DBRecord
import com.jetbrains.youtrack.db.api.record.Direction
import com.jetbrains.youtrack.db.api.record.Vertex
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal
import jetbrains.exodus.entitystore.EntityRemovedInDatabaseException
import jetbrains.exodus.entitystore.PersistentEntityId
import jetbrains.exodus.entitystore.youtrackdb.iterate.YTDBEntityIterableBase
import jetbrains.exodus.entitystore.youtrackdb.iterate.YTDBEntityOfTypeIterable
import jetbrains.exodus.entitystore.youtrackdb.iterate.link.YTDBLinkToEntityIterable
import jetbrains.exodus.entitystore.youtrackdb.query.YTDBQueryCancellingPolicy
import jetbrains.exodus.entitystore.youtrackdb.query.YTDBQueryTimeoutException
import jetbrains.exodus.entitystore.youtrackdb.testutil.*
import org.junit.Assert
import org.junit.Ignore
import org.junit.Rule
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class YTDBStoreTransactionTest : OTestMixin {

    @Rule
    @JvmField
    val orientDbRule = InMemoryYouTrackDB(true)

    override val youTrackDb = orientDbRule

    @Test
    fun `should find all`() {
        // Given
        givenTestCase()

        // When
        withStoreTx { tx ->
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
        withStoreTx { tx ->
            val result = tx.find(Issues.CLASS, "name", "issue2")

            // Then
            assertNamesExactlyInOrder(result, "issue2")
        }
    }

    @Test
    fun `findLinks should return correct entityType`() {
        val test = givenTestCase()

        withStoreTx { tx ->
            tx.addIssueToProject(test.issue1, test.project1)
            tx.addIssueToBoard(test.issue1, test.board1)
        }

        withStoreTx {
            Assert.assertEquals(
                1,
                it.findLinks(Boards.CLASS, test.issue1, Boards.Links.HAS_ISSUE).size()
            )
            Assert.assertEquals(
                1,
                it.findLinks(Projects.CLASS, test.issue1, Boards.Links.HAS_ISSUE).size()
            )
            Assert.assertEquals(
                2,
                test.issue1.vertex.getEdges(
                    Direction.IN,
                    YTDBVertexEntity.edgeClassName(Boards.Links.HAS_ISSUE)
                )
                    .toList().size
            )
        }
    }

    @Test
    fun `should find property contains`() {
        // Given
        val test = givenTestCase()
        withStoreTx {
            test.issue2.setProperty("case", "Find me if YOU can")
        }

        // When
        withStoreTx { tx ->
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
        withStoreTx { test.issue2.setProperty("case", "Find me if YOU can") }

        // When
        withStoreTx { tx ->
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
        withStoreTx {
            test.issue2.setProperty("value", 3)
        }

        // When
        withStoreTx { tx ->
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

        withStoreTx { test.issue2.setProperty("prop", "test") }

        // When
        withStoreTx { tx ->
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

        youTrackDb.withStoreTx {
            //correct blob (can be found)
            test.issue1.setBlob("myBlob", "Hello".toByteArray().inputStream())

            //blob with content of size 0 (can be found)
            test.issue2.setBlob("myBlob", ByteArray(0).inputStream())

            //blob with removed content (cannot be found)
            test.issue3.setBlob("myBlob", "World".toByteArray().inputStream())
        }

        // When
        youTrackDb.withStoreTx { tx ->
            val issues = tx.findWithBlob(Issues.CLASS, "myBlob")

            // Then
            assertNamesExactlyInOrder(issues, "issue1", "issue2", "issue3")
        }
    }

    @Test
    fun `should sorted by property`() {
        // Given
        val test = givenTestCase()

        withStoreTx {
            test.issue1.setProperty("order", "1")
            test.issue2.setProperty("order", "2")
            test.issue3.setProperty("order", "3")
        }

        // When
        withStoreTx { tx ->
            val issuesAscending = tx.sort(Issues.CLASS, "order", true)
            val issuesDescending = tx.sort(Issues.CLASS, "order", false)

            // Then
            assertNamesExactlyInOrder(issuesAscending, "issue1", "issue2", "issue3")
            assertNamesExactlyInOrder(issuesDescending, "issue3", "issue2", "issue1")
        }
    }

    @Test
    fun `single entity iterable test`() {
        val test = givenTestCase()
        youTrackDb.store.executeInTransaction {
            val issue3 = it.getSingletonIterable(test.issue3).iterator().next()
            Assert.assertEquals(test.issue3, issue3)
        }
    }

    @Test
    fun `should sort iterable by property`() {
        // Given
        val test = givenTestCase()

        withStoreTx {
            test.issue1.setProperty("order", "1")
            test.issue3.setProperty("order", "3")
        }

        // When
        withStoreTx { tx ->
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

        withStoreTx {
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
        withStoreTx { tx ->
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

        withStoreTx { tx ->
            tx.addIssueToProject(testCase.issue1, testCase.project1)
            tx.addIssueToProject(testCase.issue2, testCase.project1)
            tx.addIssueToProject(testCase.issue3, testCase.project2)
        }

        // When
        withStoreTx { tx ->
            val issues = tx.findLinks(Issues.CLASS, testCase.project1, Issues.Links.IN_PROJECT)

            // Then
            assertNamesExactlyInOrder(issues, "issue1", "issue2")
        }
    }

    @Test
    fun `should find links by link iterables`() {
        // Given
        val testCase = givenTestCase()

        withStoreTx { tx ->
            tx.addIssueToProject(testCase.issue1, testCase.project1)
            tx.addIssueToProject(testCase.issue2, testCase.project1)
            tx.addIssueToProject(testCase.issue3, testCase.project2)
        }

        // When
        withStoreTx { tx ->
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

        withStoreTx { tx ->
            tx.addIssueToBoard(test.issue1, test.board1)
            tx.addIssueToBoard(test.issue2, test.board2)
        }

        // When
        withStoreTx { tx ->
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

        withStoreTx { tx ->
            tx.addIssueToProject(testCase.issue1, testCase.project1)
            tx.addIssueToProject(testCase.issue2, testCase.project1)
            tx.addIssueToProject(testCase.issue3, testCase.project2)
        }

        // When
        withStoreTx { tx ->
            // Find all issues that in project1 or project2
            val issuesInProject1 =
                tx.findLinks(Issues.CLASS, testCase.project1, Issues.Links.IN_PROJECT)
            val issuesInProject2 =
                tx.findLinks(Issues.CLASS, testCase.project2, Issues.Links.IN_PROJECT)
            val issues = issuesInProject1.union(issuesInProject2)

            // Then
            assertNamesExactlyInOrder(issues, "issue1", "issue2", "issue3")
        }
    }

    @Test
    fun `should find links and iterable intersect`() {
        // Given
        val test = givenTestCase()

        withStoreTx { tx ->
            tx.addIssueToBoard(test.issue1, test.board1)
            tx.addIssueToBoard(test.issue2, test.board1)
            tx.addIssueToBoard(test.issue2, test.board2)
            tx.addIssueToBoard(test.issue3, test.board3)
        }

        // When
        withStoreTx { tx ->
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

        withStoreTx { tx ->
            tx.addIssueToProject(test.issue1, test.project1)
            tx.addIssueToBoard(test.issue2, test.board2)
            tx.addIssueToBoard(test.issue3, test.board3)
        }

        // When
        withStoreTx { tx ->
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

        // Issues assigned to projects ink reverse order
        withStoreTx { tx ->
            tx.addIssueToProject(test.issue1, test.project1)
            tx.addIssueToProject(test.issue2, test.project2)
            tx.addIssueToProject(test.issue3, test.project3)
        }

        // When
        withStoreTx { tx ->
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
        withStoreTx { tx ->
            tx.addIssueToProject(test.issue1, test.project3)
            tx.addIssueToProject(test.issue1, test.project2)

            tx.addIssueToProject(test.issue2, test.project2)
            tx.addIssueToProject(test.issue2, test.project1)

            tx.addIssueToProject(test.issue3, test.project1)
            tx.addIssueToProject(test.issue3, test.project2)
        }

        // When
        withStoreTx { tx ->
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
        withStoreTx { tx ->
            tx.addIssueToBoard(test.issue1, test.board1)
            tx.addIssueToBoard(test.issue1, test.board2)
            tx.addIssueToBoard(test.issue2, test.board1)
            tx.addIssueToBoard(test.issue3, test.board1)
        }

        // When
        withStoreTx { tx ->
            val issues = tx.getAll(Issues.CLASS) as YTDBEntityIterableBase
            val boards = issues.selectMany(Issues.Links.ON_BOARD)

            // Then
            assertNamesExactlyInOrder(boards.sorted(), "board1", "board1", "board1", "board2")
        }
    }

    @Test
    fun `should select many links distinct`() {
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
            val issues = tx.getAll(Issues.CLASS) as YTDBEntityIterableBase
            val boards = issues.selectDistinct(Issues.Links.ON_BOARD)

            // Then
            assertNamesExactlyInOrder(boards.sorted(), "board1", "board2")
        }
    }

    @Test
    fun `select by id range dummy test`() {
        // Given
        val test = givenTestCase()
        withStoreTx {
            test.issue1.setProperty(YTDBVertexEntity.LOCAL_ENTITY_ID_PROPERTY_NAME, 0L)
            test.issue2.setProperty(YTDBVertexEntity.LOCAL_ENTITY_ID_PROPERTY_NAME, 3L)
            test.issue3.setProperty(YTDBVertexEntity.LOCAL_ENTITY_ID_PROPERTY_NAME, 99L)
        }

        // When
        withStoreTx { tx ->
            val issues = tx.findIds(Issues.CLASS, 2, 100) as YTDBEntityIterableBase
            // Then
            assertNamesExactlyInOrder(
                issues,
                test.issue2.getProperty("name").toString(),
                test.issue3.getProperty("name").toString()
            )
        }
    }

    @Test
    fun `tx lets search for an entity using PersistentEntityId`() {
        val aId = youTrackDb.createIssue("A").id
        val bId = youTrackDb.createIssue("B").id

        // use default ids
        youTrackDb.store.executeInTransaction { tx ->
            val a = tx.getEntity(aId)
            val b = tx.getEntity(bId)

            Assert.assertEquals(aId, a.id)
            Assert.assertEquals(bId, b.id)
        }

        // use legacy ids
        val legacyIdA = PersistentEntityId(aId.typeId, aId.localId)
        val legacyIdB = PersistentEntityId(bId.typeId, bId.localId)
        youTrackDb.store.executeInTransaction { tx ->
            val a = tx.getEntity(legacyIdA)
            val b = tx.getEntity(legacyIdB)

            Assert.assertEquals(aId, a.id)
            Assert.assertEquals(bId, b.id)
        }
    }

    @Test
    fun `getEntity() throws an exception if the entity not found`() {
        val aId = youTrackDb.createIssue("A").id

        // delete the issue
        youTrackDb.withTxSession { oSession ->
            oSession.delete(aId.asOId())
        }

        // entity not found
        youTrackDb.store.executeInTransaction { tx ->
            assertFailsWith<EntityRemovedInDatabaseException> {
                tx.getEntity(aId)
            }
            assertFailsWith<EntityRemovedInDatabaseException> {
                tx.getEntity(PersistentEntityId(300, 300))
            }
            assertFailsWith<EntityRemovedInDatabaseException> {
                tx.getEntity(PersistentEntityId.EMPTY_ID)
            }
            assertFailsWith<EntityRemovedInDatabaseException> {
                tx.getEntity(RIDEntityId.EMPTY_ID)
            }
        }
    }

    @Test
    fun `tx works with both ORIDEntityId and PersistentEntityId representations`() {
        val aId = youTrackDb.createIssue("A").id
        val bId = youTrackDb.createIssue("B").id
        val aIdRepresentation = aId.toString()
        val bIdRepresentation = bId.toString()
        val aLegacyId = PersistentEntityId(aId.typeId, aId.localId)
        val bLegacyId = PersistentEntityId(bId.typeId, bId.localId)
        val aLegacyIdRepresentation = aLegacyId.toString()
        val bLegacyIdRepresentation = bLegacyId.toString()

        youTrackDb.store.executeInTransaction { tx ->
            assertEquals(aId, tx.toEntityId(aIdRepresentation))
            assertEquals(bId, tx.toEntityId(bIdRepresentation))

            assertEquals(aId, tx.toEntityId(aLegacyIdRepresentation))
            assertEquals(bId, tx.toEntityId(bLegacyIdRepresentation))
        }
    }


    @Test
    fun `entity id should be valid and accessible just after creation`() {
        youTrackDb.store.executeInTransaction { tx ->
            val entity = tx.newEntity(Issues.CLASS)
            val orid = (entity.id as YTDBEntityId).asOId()
            Assert.assertTrue(orid.clusterId > 0)
        }
    }

    @Test
    fun `newEntity sets localEntityId`() {
        youTrackDb.store.executeInTransaction { tx ->
            val issue = tx.newEntity(Issues.CLASS)
            assertEquals(issue.id.localId, 0)
        }
    }

    /*
    * This behaviour may change in the future if we support schema changes in transactions
    * */
    @Test
    fun `newEntity() throws exception if the type is not created`() {
        withStoreTx { tx ->
            assertFailsWith<IllegalStateException> {
                tx.newEntity("opca")
            }
        }
    }

    @Test
    fun `read-only transaction forbids changing data in it`() {
        val issue = youTrackDb.createIssue("trista")
        val tx = youTrackDb.store.beginReadonlyTransaction()
        assertFailsWith<IllegalStateException> { tx.newEntity(Issues.CLASS) }
        assertFailsWith<IllegalStateException> { tx.saveEntity(issue) }
    }

    @Test
    fun `should throw timeout exception when timeout is small`() {
        // Given
        val test = givenTestCase()
        test.createManyIssues(1000)

        // When
        youTrackDb.store.executeInTransaction { transaction ->
            transaction.queryCancellingPolicy = YTDBQueryCancellingPolicy.timeout(0)

            val exception = Assert.assertThrows(YTDBQueryTimeoutException::class.java) {
                transaction.getAll(Issues.CLASS).toList()
            }
            assertThat(exception.message).contains("Query execution timed out")
        }
    }

    @Test
    fun `should not return nulls on empty links`() {
        // Given
        val test = givenTestCase()

        withStoreTx { tx ->
            tx.addIssueToBoard(test.issue1, test.board1)
            tx.addIssueToBoard(test.issue2, test.board2)
        }

        withStoreTx { tx ->
            val boards =
                YTDBEntityOfTypeIterable(tx, Issues.CLASS).selectManyDistinct(Issues.Links.ON_BOARD)
                    .toList()
            //selectManyDistinct
            Assert.assertEquals(2, boards.size)
            Assert.assertEquals(
                "Should not contain nulls",
                0,
                boards.filter { board -> board == null }.size
            )
        }
    }

    @Test
    fun `contains should work `() {
        // Given
        val test = givenTestCase()
        withStoreTx { tx ->
            tx.addIssueToBoard(test.issue1, test.board1)
        }

        withStoreTx { tx ->
            val issues = YTDBEntityOfTypeIterable(tx, Issues.CLASS)
            Assert.assertTrue(issues.contains(test.issue1))
            val issuesOnBoard = YTDBLinkToEntityIterable(tx, Issues.Links.ON_BOARD, test.board1.id)
            Assert.assertEquals(1, issuesOnBoard.toList().size)
            Assert.assertFalse(issuesOnBoard.contains(test.issue2))
            Assert.assertTrue(issuesOnBoard.contains(test.issue1))
        }
    }

    @Test
    fun `active session still has an active transaction after flush`() {
        assertFailsWith<DatabaseException> { DatabaseRecordThreadLocal.instance().get() }
        withStoreTx { tx ->
            DatabaseRecordThreadLocal.instance().get().requireActiveTransaction()
            tx.flush()
            DatabaseRecordThreadLocal.instance().get().requireActiveTransaction()
        }
    }

    @Test
    fun `transactionId does not get changed on flush()`() {
        withStoreTx { tx ->
            val oTransactionId =
                DatabaseRecordThreadLocal.instance().get().transaction.id
            assertEquals(oTransactionId, tx.getTransactionId())
            tx.flush()
            assertEquals(oTransactionId, tx.getTransactionId())
        }
    }

    @Test
    fun `deactivate, activate transactions`() {
        // TX1
        val tx1 = beginTransaction()
        val e1 = tx1.createIssue("mamba1")
        tx1.deactivateOnCurrentThread()
        // you cannot suspend an already suspended transaction
        assertFailsWith<IllegalStateException> { tx1.deactivateOnCurrentThread() }

        // TX2
        val tx2 = beginTransaction()
        // there is an active transaction on the current thread, so you cannot activate one more
        assertFailsWith<IllegalStateException> { tx1.activateOnCurrentThread() }
        // tx1 has not been commited yet, se we do not see its changes
        assertFailsWith<EntityRemovedInDatabaseException> { tx2.getEntity(e1.id) }

        val e2 = tx2.createIssue("mamba2")
        tx2.deactivateOnCurrentThread()

        // TX1
        tx1.activateOnCurrentThread()
        // tx2 has not been commited yet, so you cannot see its changes
        assertFailsWith<EntityRemovedInDatabaseException> { tx1.getEntity(e2.id) }
        tx1.commit()

        // you cannot activate a finished transaction
        assertFailsWith<IllegalStateException> { tx1.activateOnCurrentThread() }

        // TX2
        tx2.activateOnCurrentThread()
        // e1 is already visible, yeah, it is not Serializable isolation
        tx2.getEntity(e1.id)
        tx2.commit()

        // TX3
        val tx3 = beginTransaction()
        tx3.getEntity(e2.id)
        tx3.commit()
    }

    @Test
    fun `entities understand when a transaction gets activated and deactivated`() {
        val id = withStoreTx { tx ->
            val e1 = tx.createIssue("trista")
            tx.deactivateOnCurrentThread()
            // no active transaction on the current thread
            assertFailsWith<IllegalStateException> { e1.setProperty("mamba", "mamba") }
            tx.activateOnCurrentThread()
            e1.setProperty("mamba", "mamba")
            e1.id
        }

        withStoreTx { tx ->
            val e1 = tx.getEntity(id)
            tx.deactivateOnCurrentThread()
            // no active transaction on the current thread
            assertFailsWith<IllegalStateException> { e1.setProperty("mamba", "caramba") }
            tx.activateOnCurrentThread()
            e1.setProperty("mamba", "caramba")
        }
    }

    @Test
    fun `getRecord()`() {
        val id = withStoreTx { tx ->
            val e1 = tx.createIssue("opca trista")
            e1.setProperty("mamba", "caramba")
            e1.id
        }

        withStoreTx { tx ->
            val vertex: Vertex = tx.getRecord(id)
            assertEquals("caramba", vertex.getProperty("mamba"))
            val e1 = tx.getEntity(id)
            e1.delete()
        }

        withStoreTx { tx ->
            try {
                tx.getRecord<DBRecord>(id)
                Assert.fail()
            } catch (e: EntityRemovedInDatabaseException) {
                // expected
            }
        }
    }
}