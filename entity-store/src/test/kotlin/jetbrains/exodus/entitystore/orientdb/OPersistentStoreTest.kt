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

import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.OVertex
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException
import jetbrains.exodus.entitystore.EntityRemovedInDatabaseException
import jetbrains.exodus.entitystore.PersistentEntityId
import jetbrains.exodus.entitystore.StoreTransaction
import jetbrains.exodus.entitystore.orientdb.testutil.InMemoryOrientDB
import jetbrains.exodus.entitystore.orientdb.testutil.Issues
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
    fun `renameEntityType() works only inside a transaction`() {
        // make sure the schema class is created
        orientDb.createIssue("trista")

        assertFailsWith<IllegalStateException> { orientDb.store.renameEntityType(CLASS, "NewName") }

        orientDb.store.executeInTransaction {
            orientDb.store.renameEntityType(CLASS, "NewName")
        }
    }

    @Test
    fun renameClassTest() {
        val summary = "Hello, your product does not work"
        orientDb.createIssue(summary)
        val store = orientDb.store

        val newClassName = "Other${CLASS}"
        store.executeInTransaction {
            store.renameEntityType(CLASS, newClassName)
        }
        val issueByNewName = store.computeInExclusiveTransaction { tx ->
            tx.getAll(newClassName).first()
        }
        Assert.assertNotNull(issueByNewName)
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
        orientDb.withSession { oSession ->
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
        orientDb.store.executeInTransaction {
            assertEquals(ORIDEntityId.EMPTY_ID, orientDb.store.getOEntityId(notExistingEntityId))
            assertEquals(ORIDEntityId.EMPTY_ID, orientDb.store.getOEntityId(partiallyExistingEntityId1))
            assertEquals(ORIDEntityId.EMPTY_ID, orientDb.store.getOEntityId(partiallyExistingEntityId2))
            assertEquals(issueId, orientDb.store.getOEntityId(totallyExistingEntityId))
        }
    }

    @Test
    fun `propertyNames does not count internal properties`() {
        val issue = orientDb.store.computeInTransaction {  txn ->
            txn as OStoreTransaction
            val issue = txn.createIssue("Hello", "Critical")
            val project = txn.createProject("World")
            txn.addIssueToProject(issue, project)
            issue.setBlobString("bober", "bober")
            issue.setBlob("biba", "hello".toByteArray().inputStream())
            issue.setProperty("hello", 1995)
            issue
        }
        orientDb.store.executeInTransaction {
            assertEquals(listOf(Issues.Props.PRIORITY, "name", "hello").sorted(), issue.propertyNames.sorted())
        }
    }

    @Test
    fun `requireOEntityId works correctly with different types of EntityId`() {
        val issueId = orientDb.createIssue("trista").id

        orientDb.store.executeInTransaction {
            assertEquals(issueId, orientDb.store.requireOEntityId(issueId))
            assertEquals(issueId, orientDb.store.requireOEntityId(PersistentEntityId(issueId.typeId, issueId.localId)))
            assertEquals(ORIDEntityId.EMPTY_ID, orientDb.store.requireOEntityId(PersistentEntityId.EMPTY_ID))
        }
    }

    @Test
    fun `computeInTransaction and Co handle exceptions properly`() {
        withSession { session ->
            val t1 = session.getOrCreateVertexClass("type1")
            t1.createProperty("name", OType.STRING)
            t1.createIndex("opca_index", OClass.INDEX_TYPE.UNIQUE, "name")
        }
        fun StoreTransaction.violateIndexRestriction() {
            val e1 = this.newEntity("type1")
            val e2 = this.newEntity("type1")
            e1.setProperty("name", "trista")
            e2.setProperty("name", "trista")
        }
        /**
         * Here we check that nothing happens with the exception on the way up.
         * Our code that finishes the transaction must work correctly if there is no active session.
         */
        val store = orientDb.store
        assertFailsWith<ORecordDuplicatedException> {
            store.computeInTransaction { tx ->
                tx.violateIndexRestriction()
            }
        }
        assertFailsWith<ORecordDuplicatedException> {
            store.executeInTransaction { tx ->
                tx.violateIndexRestriction()
            }
        }
        assertFailsWith<ORecordDuplicatedException> {
            withStoreTx { tx ->
                tx.violateIndexRestriction()
            }
        }
    }
}
