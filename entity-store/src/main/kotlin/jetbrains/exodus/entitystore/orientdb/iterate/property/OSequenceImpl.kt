package jetbrains.exodus.entitystore.orientdb.iterate.property

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.metadata.sequence.OSequence
import com.orientechnologies.orient.core.metadata.sequence.OSequence.CreateParams
import com.orientechnologies.orient.core.metadata.sequence.OSequence.SEQUENCE_TYPE
import com.orientechnologies.orient.core.metadata.sequence.OSequenceLibrary
import jetbrains.exodus.entitystore.Sequence

class OSequenceImpl(
    private val sequenceName: String,
    initialValue: Long = 0
) : Sequence {

    private var sequence = getOrCreateSequence(initialValue)

    override fun increment(): Long {
        return sequence.next()
    }

    override fun get(): Long {
        return sequence.current()
    }

    override fun set(l: Long) {
        val session = ODatabaseSession.getActiveSession()
        val sequenceLibrary: OSequenceLibrary = session.metadata.sequenceLibrary
        sequenceLibrary.dropSequence(sequenceName)
        sequence = getOrCreateSequence(l)
    }


    private fun getOrCreateSequence(start:Long): OSequence {
        val session = ODatabaseSession.getActiveSession()
        val sequenceLibrary: OSequenceLibrary = session.metadata.sequenceLibrary
        var result = sequenceLibrary.getSequence(sequenceName)
        if (result == null) {
            val params = CreateParams().setStart(start).setIncrement(1)
            result = sequenceLibrary.createSequence(sequenceName, SEQUENCE_TYPE.ORDERED, params)
        }
        return result
    }

}
