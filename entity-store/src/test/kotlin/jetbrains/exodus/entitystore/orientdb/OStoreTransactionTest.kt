package jetbrains.exodus.entitystore.orientdb

import jetbrains.exodus.entitystore.PersistentEntityId
import jetbrains.exodus.entitystore.orientdb.testutil.InMemoryOrientDB
import jetbrains.exodus.entitystore.orientdb.testutil.createIssue
import org.junit.Assert
import org.junit.Rule
import org.junit.Test

class OStoreTransactionTest {
    @Rule
    @JvmField
    val orientDb = InMemoryOrientDB()

    @Test
    fun `tx lets search for an entity using PersistentEntityId`() {
        val aId = orientDb.createIssue("A").id
        val bId = orientDb.createIssue("B").id

        // use default ids
        orientDb.store.executeInTransaction { tx ->
            val a = tx.getEntity(aId)
            val b = tx.getEntity(bId)

            Assert.assertEquals(aId, a.id)
            Assert.assertEquals(bId, b.id)
        }

        // use legacy ids
        orientDb.store.executeInTransaction { tx ->
            val legacyIdA = PersistentEntityId(aId.typeId, aId.localId)
            val legacyIdB = PersistentEntityId(bId.typeId, bId.localId)
            val a = tx.getEntity(legacyIdA)
            val b = tx.getEntity(legacyIdB)

            Assert.assertEquals(aId, a.id)
            Assert.assertEquals(bId, b.id)
        }
    }
}