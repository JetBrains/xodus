package jetbrains.exodus.entitystore.orientdb

import jetbrains.exodus.entitystore.PersistentEntityId
import jetbrains.exodus.entitystore.orientdb.testutil.InMemoryOrientDB
import jetbrains.exodus.entitystore.orientdb.testutil.createIssue
import org.junit.Assert
import org.junit.Rule
import org.junit.Test
import kotlin.test.assertEquals

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
        val legacyIdA = PersistentEntityId(aId.typeId, aId.localId)
        val legacyIdB = PersistentEntityId(bId.typeId, bId.localId)
        orientDb.store.executeInTransaction { tx ->
            val a = tx.getEntity(legacyIdA)
            val b = tx.getEntity(legacyIdB)

            Assert.assertEquals(aId, a.id)
            Assert.assertEquals(bId, b.id)
        }
    }

    @Test
    fun `tx works with both ORIDEntityId and PersistentEntityId representations`() {
        val aId = orientDb.createIssue("A").id
        val bId = orientDb.createIssue("B").id
        val aIdRepresentation = aId.toString()
        val bIdRepresentation = bId.toString()
        val aLegacyId = PersistentEntityId(aId.typeId, aId.localId)
        val bLegacyId = PersistentEntityId(bId.typeId, bId.localId)
        val aLegacyIdRepresentation = aLegacyId.toString()
        val bLegacyIdRepresentation = bLegacyId.toString()

        orientDb.store.executeInTransaction { tx ->
            assertEquals(aId, tx.toEntityId(aIdRepresentation))
            assertEquals(bId, tx.toEntityId(bIdRepresentation))

            assertEquals(aId, tx.toEntityId(aLegacyIdRepresentation))
            assertEquals(bId, tx.toEntityId(bLegacyIdRepresentation))
        }
    }
}