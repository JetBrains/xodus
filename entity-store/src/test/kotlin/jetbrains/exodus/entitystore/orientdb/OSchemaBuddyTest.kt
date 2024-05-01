package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.metadata.sequence.OSequence
import jetbrains.exodus.entitystore.PersistentEntityId
import jetbrains.exodus.entitystore.orientdb.testutil.InMemoryOrientDB
import jetbrains.exodus.entitystore.orientdb.testutil.createIssue
import org.junit.Test

import org.junit.Assert.*
import org.junit.Rule

class OSchemaBuddyTest {

    @Rule
    @JvmField
    val orientDb = InMemoryOrientDB(initializeIssueSchema = false)

    @Test
    fun `if autoInitialize is false, explicit initialization is required`() {
        val issueId = orientDb.createIssue("trista").id
        val buddy = OSchemaBuddyImpl(orientDb.provider, autoInitialize = false)

        val totallyExistingEntityId = PersistentEntityId(issueId.typeId, issueId.localId)
        orientDb.withSession {
            assertEquals(ORIDEntityId.EMPTY_ID, buddy.getOEntityId(totallyExistingEntityId))
        }

        buddy.initialize()

        orientDb.withSession {
            assertEquals(issueId, buddy.getOEntityId(totallyExistingEntityId))
        }
    }

    @Test
    fun `getOEntityId() works with both existing and not existing EntityId`() {
        val issueId = orientDb.createIssue("trista").id
        val buddy = OSchemaBuddyImpl(orientDb.provider, autoInitialize = true)

        val notExistingEntityId = PersistentEntityId(300, 301)
        val partiallyExistingEntityId1 = PersistentEntityId(issueId.typeId, 301)
        val partiallyExistingEntityId2 = PersistentEntityId(300, issueId.localId)
        val totallyExistingEntityId = PersistentEntityId(issueId.typeId, issueId.localId)
        orientDb.withSession {
            assertEquals(ORIDEntityId.EMPTY_ID, buddy.getOEntityId(notExistingEntityId))
            assertEquals(ORIDEntityId.EMPTY_ID, buddy.getOEntityId(partiallyExistingEntityId1))
            assertEquals(ORIDEntityId.EMPTY_ID, buddy.getOEntityId(partiallyExistingEntityId2))
            assertEquals(issueId, buddy.getOEntityId(totallyExistingEntityId))
        }
    }

    /*
    * SchemaBuddy heavily depends on this invariant for the classId map consistency
    * */
    @Test
    fun `sequence does not roll back already generated values if the transaction is rolled back`() {
        orientDb.withSession { session ->
            val params = OSequence.CreateParams()
            params.start = 0
            session.metadata.sequenceLibrary.createSequence("seq", OSequence.SEQUENCE_TYPE.ORDERED, params)
        }

        orientDb.withTxSession { session ->
            val res = session.metadata.sequenceLibrary.getSequence("seq").next()
            assertEquals(1, res)
            session.rollback()
        }

        orientDb.withTxSession { session ->
            val res = session.metadata.sequenceLibrary.getSequence("seq").next()
            assertEquals(2, res)
            session.rollback()
        }
    }

}