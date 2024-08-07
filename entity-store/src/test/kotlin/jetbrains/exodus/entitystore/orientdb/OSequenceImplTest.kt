package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.exception.ORecordNotFoundException
import com.orientechnologies.orient.core.metadata.sequence.OSequence
import jetbrains.exodus.entitystore.Sequence
import jetbrains.exodus.entitystore.orientdb.iterate.property.getOrCreateSequence
import jetbrains.exodus.entitystore.orientdb.testutil.InMemoryOrientDB
import jetbrains.exodus.entitystore.orientdb.testutil.OTestMixin
import org.junit.Assert
import org.junit.Rule
import org.junit.Test
import kotlin.test.assertFailsWith

class OSequenceImplTest: OTestMixin {
    @Rule
    @JvmField
    val orientDbRule = InMemoryOrientDB()

    override val orientDb = orientDbRule

    @Test
    fun `if session has no active transaction, create the sequence on the current session`() {
        orientDb.withSession { session ->
            val seq = session.getOrCreateSequence("s1", sessionCreator = { throw IllegalStateException() }, initialValue = 300)
            Assert.assertEquals(300, seq.get())
            Assert.assertEquals(301, seq.increment())
        }
        orientDb.withSession { session ->
            val seq = session.getOrCreateSequence("s1", sessionCreator = { throw IllegalStateException() })
            Assert.assertEquals(301, seq.get())
        }
    }

    @Test
    fun `sequence may be created in one session and used in another`() {
        val seq: Sequence = orientDb.withSession { session ->
            session.getOrCreateSequence("s1", sessionCreator = { throw IllegalStateException() }, initialValue = 300)
        }
        orientDb.withSession { session ->
            Assert.assertEquals(300, seq.get())
            Assert.assertEquals(301, seq.increment())
        }
        orientDb.withTxSession { session ->
            Assert.assertEquals(301, seq.get())
            Assert.assertEquals(302, seq.increment())
        }
    }

    @Test
    fun `if the session has an active transaction, another session gets created specially for the sequence creation`() {
        val seq = orientDb.withTxSession { session ->
            val seq = session.getOrCreateSequence("s1", sessionCreator = { orientDb.openSession() }, initialValue = 200)
            Assert.assertEquals(200, seq.get())
            Assert.assertEquals(201, seq.increment())
            seq
        }
        orientDb.withTxSession { session ->
            Assert.assertEquals(201, seq.get())
            Assert.assertEquals(202, seq.increment())
        }
        orientDb.withTxSession { session ->
            val localSeq = session.getOrCreateSequence("s1", sessionCreator = { throw IllegalStateException() }, initialValue = 200)
            Assert.assertEquals(202, localSeq.get())
            Assert.assertEquals(203, localSeq.increment())
        }
    }

    @Test
    fun `sequence_set() resets the current value`() {
        val seq = orientDb.withSession { session ->
            val seq = session.getOrCreateSequence("s1", sessionCreator = { throw IllegalStateException() }, initialValue = 300)
            Assert.assertEquals(300, seq.get())
            Assert.assertEquals(301, seq.increment())
            seq
        }
        orientDb.withSession { session ->
            seq.set(200)
            Assert.assertEquals(200, seq.get())
            Assert.assertEquals(201, seq.increment())
        }
    }

    @Test
    fun `if you create a sequence in a transaction, nothing works`() {
        orientDb.withTxSession { session ->
            val seq = session.metadata.sequenceLibrary.createSequence("s1",
                OSequence.SEQUENCE_TYPE.ORDERED, OSequence.CreateParams().setStart(300).setIncrement(1))
            assertFailsWith<ORecordNotFoundException> { seq.current() }
        }
    }
}