package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.record.OVertex
import jetbrains.exodus.entitystore.PersistentEntityId
import jetbrains.exodus.entitystore.orientdb.testutil.InMemoryOrientDB
import jetbrains.exodus.entitystore.orientdb.testutil.Issues.CLASS
import jetbrains.exodus.entitystore.orientdb.testutil.createIssue
import org.junit.Assert
import org.junit.Assert.assertEquals
import org.junit.Rule
import org.junit.Test

class OPersistentStoreTest {
    @Rule
    @JvmField
    val orientDb = InMemoryOrientDB()

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
        assertEquals(summary, issueByNewName.getProperty("name"))
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
            assertEquals(1, sequence.increment())
        }
        store.executeInTransaction {
            assertEquals(1,it.getSequence("first").get())
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
    fun `can set actual value to sequence`(){
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
    fun `store lets search for an entity using PersistentEntityId`() {
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
