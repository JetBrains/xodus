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
package jetbrains.exodus.entitystore.orientdb.iterate.property

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.db.document.ODatabaseDocument
import com.orientechnologies.orient.core.metadata.sequence.OSequence
import com.orientechnologies.orient.core.metadata.sequence.OSequence.CreateParams
import com.orientechnologies.orient.core.metadata.sequence.OSequence.SEQUENCE_TYPE
import jetbrains.exodus.entitystore.Sequence
import jetbrains.exodus.entitystore.orientdb.hasActiveTransaction

fun ODatabaseSession.getOrCreateSequence(
    sequenceName: String,
    /**
     * If the sequence has not yet been created and the current session has an active transaction,
     * we will have to create a separate session specially for the sequence creation.
     * Orient goes crazy if you create a sequence in a transaction and use it right away.
     * */
    sessionCreator: () -> ODatabaseDocument,
    initialValue: Long = 0
): Sequence {
    makeSureOSequenceHasBeenCreated(sequenceName, sessionCreator, initialValue)
    return OSequenceImpl(sequenceName)
}

private fun ODatabaseSession.makeSureOSequenceHasBeenCreated(
    sequenceName: String,
    sessionCreator: () -> ODatabaseDocument,
    initialValue: Long
): OSequence {
    var oSequence = this.metadata.sequenceLibrary.getSequence(sequenceName)

    if (oSequence != null) return oSequence

    val params = CreateParams().setStart(initialValue).setIncrement(1)
    if (this.hasActiveTransaction()) {
        // sequences do not like to be created in a transaction, so we create a separate session specially for the sequence creation
        try {
            sessionCreator().use { session ->
                assert(session.isActiveOnCurrentThread) // the session gets activated on the current thread by default
                oSequence = session.metadata.sequenceLibrary.createSequence(sequenceName, SEQUENCE_TYPE.ORDERED, params)
            }
        } finally {
            // the previous session does not get activated on the current thread by default
            assert(!this.isActiveOnCurrentThread)
            this.activateOnCurrentThread()
        }
    } else {
        // the current session has no transactions, so we can create the sequence right away
        oSequence = this.metadata.sequenceLibrary.createSequence(sequenceName, SEQUENCE_TYPE.ORDERED, params)
    }
    return oSequence
}

private class OSequenceImpl(
    private val sequenceName: String,
) : Sequence {
    override fun increment(): Long {
        return getOSequence().next()
    }

    override fun get(): Long {
        return getOSequence().current()
    }

    override fun set(l: Long) {
        getOSequence().updateParams(CreateParams().setCurrentValue(l))
    }

    private fun getOSequence(): OSequence {
        val currentSession = ODatabaseSession.getActiveSession()
        return currentSession.metadata.sequenceLibrary.getSequence(sequenceName)
    }
}
