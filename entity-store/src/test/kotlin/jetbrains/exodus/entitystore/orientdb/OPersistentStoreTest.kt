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

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.record.OVertex
import jetbrains.exodus.entitystore.EntityRemovedInDatabaseException
import jetbrains.exodus.entitystore.PersistentEntityId
import jetbrains.exodus.entitystore.orientdb.testutil.InMemoryOrientDB
import jetbrains.exodus.entitystore.orientdb.testutil.Issues.CLASS
import jetbrains.exodus.entitystore.orientdb.testutil.OTestMixin
import jetbrains.exodus.entitystore.orientdb.testutil.createIssue
import org.junit.Assert
import org.junit.Assert.assertEquals
import org.junit.Rule
import org.junit.Test
import kotlin.test.assertFailsWith

class OPersistentStoreTest: OTestMixin {

    @Rule
    @JvmField
    val orientDbRule = InMemoryOrientDB()

    override val orientDb = orientDbRule

    @Test
    fun renameClassTest() {
        val summary = "Hello, your product does not work"
        orientDb.createIssue(summary)
        val store = orientDb.store

        val newClassName = "Other${CLASS}"
        store.renameEntityType(CLASS, newClassName)
        val issueByNewName = store.computeInExclusiveTransaction {
            it as OStoreTransaction
            (it.activeSession as ODatabaseSession).queryEntities("select from $newClassName", store).firstOrNull()
        }
        Assert.assertNotNull(issueByNewName)
        issueByNewName!!
        store.executeInTransaction {
            assertEquals(summary, issueByNewName.getProperty("name"))
        }
    }

    @Test
    fun transactionPropertiesTest() {
        val issue = orientDb.createIssue("Hello, nothing works")
        val store = orientDb.store
        store.computeInTransaction {
            Assert.assertTrue(it.isIdempotent)
            issue.asVertex.reload<OVertex>()
            issue.setProperty("version", "22")
            Assert.assertFalse(it.isIdempotent)
        }
    }

    @Test
    fun `create and increment sequence`() {
        val store = orientDb.store
        val sequence = store.computeInTransaction {
            it.getSequence("first")
        }
        store.executeInTransaction {
            assertEquals(0, sequence.increment())
        }
        store.executeInTransaction {
            assertEquals(0, it.getSequence("first").get())
        }
        store.executeInTransaction {
            assertEquals(1, sequence.increment())
        }
    }

    @Test
    fun `create sequence with starting from`() {
        val store = orientDb.store
        val sequence = store.computeInTransaction {
            it.getSequence("first", 99)
        }
        store.executeInTransaction {
            assertEquals(100, sequence.increment())
        }
    }

    @Test
    fun `can set actual value to sequence`() {
        val store = orientDb.store
        val sequence = store.computeInTransaction {
            it.getSequence("first", 99)
        }
        store.executeInTransaction {
            sequence.set(400)
        }
        store.executeInTransaction {
            assertEquals(401, sequence.increment())
        }
    }

    @Test
    fun `getEntity() works with both ORIDEntityId and PersistentEntityId`() {
        val aId = orientDb.createIssue("A").id
        val bId = orientDb.createIssue("B").id
        val store = orientDb.store

        // use default ids
        orientDb.store.executeInTransaction {
            val a = store.getEntity(aId)
            val b = store.getEntity(bId)

            assertEquals(aId, a.id)
            assertEquals(bId, b.id)
        }

        // use legacy ids
        orientDb.store.executeInTransaction {
            val legacyIdA = PersistentEntityId(aId.typeId, aId.localId)
            val legacyIdB = PersistentEntityId(bId.typeId, bId.localId)
            val a = store.getEntity(legacyIdA)
            val b = store.getEntity(legacyIdB)

            assertEquals(aId, a.id)
            assertEquals(bId, b.id)
        }

    }

    @Test
    fun `getEntity() throw exception the entity is not found`() {
        val aId = orientDb.createIssue("A").id

        // delete the issue
        orientDb.store.databaseProvider.withSession { oSession ->
            oSession.delete(aId.asOId())
        }

        // entity not found
        orientDb.store.executeInTransaction { tx ->
            assertFailsWith<EntityRemovedInDatabaseException> {
                orientDb.store.getEntity(aId)
            }
            assertFailsWith<EntityRemovedInDatabaseException> {
                orientDb.store.getEntity(PersistentEntityId(300, 300))
            }
            assertFailsWith<EntityRemovedInDatabaseException> {
                orientDb.store.getEntity(PersistentEntityId.EMPTY_ID)
            }
            assertFailsWith<EntityRemovedInDatabaseException> {
                orientDb.store.getEntity(ORIDEntityId.EMPTY_ID)
            }
        }
    }

    @Test
    fun `getting OEntityId for not existing EntityId gives EMPTY_ID`() {
        val issueId = orientDb.createIssue("trista").id
        val notExistingEntityId = PersistentEntityId(300, 301)
        val partiallyExistingEntityId1 = PersistentEntityId(issueId.typeId, 301)
        val partiallyExistingEntityId2 = PersistentEntityId(300, issueId.localId)
        val totallyExistingEntityId = PersistentEntityId(issueId.typeId, issueId.localId)
        orientDb.withSession {
            assertEquals(ORIDEntityId.EMPTY_ID, orientDb.store.getOEntityId(notExistingEntityId))
            assertEquals(ORIDEntityId.EMPTY_ID, orientDb.store.getOEntityId(partiallyExistingEntityId1))
            assertEquals(ORIDEntityId.EMPTY_ID, orientDb.store.getOEntityId(partiallyExistingEntityId2))
            assertEquals(issueId, orientDb.store.getOEntityId(totallyExistingEntityId))
        }
    }

    @Test
    fun `requireOEntityId works correctly with different types of EntityId`() {
        val issueId = orientDb.createIssue("trista").id

        orientDb.withSession {
            assertEquals(issueId, orientDb.store.requireOEntityId(issueId))
            assertEquals(issueId, orientDb.store.requireOEntityId(PersistentEntityId(issueId.typeId, issueId.localId)))
            assertEquals(ORIDEntityId.EMPTY_ID, orientDb.store.requireOEntityId(PersistentEntityId.EMPTY_ID))
        }
    }
}
