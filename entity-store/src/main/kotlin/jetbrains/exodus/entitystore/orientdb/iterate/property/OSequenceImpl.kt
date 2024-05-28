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
import com.orientechnologies.orient.core.metadata.sequence.OSequenceLibrary
import jetbrains.exodus.entitystore.Sequence

class OSequenceImpl(
    private val sequenceName: String,
    private val sessionCreator: () -> ODatabaseDocument,
    private val initialValue: Long = 0
) : Sequence {


    override fun increment(): Long {
        return accessSequenceInSession {
            it.next()
        }
    }

    override fun get(): Long {
        return accessSequenceInSession {
            it.current()
        }
    }

    override fun set(l: Long) {
        accessSequenceInSession {
            it.updateParams(CreateParams().setCurrentValue(l))
        }
    }

    private fun <T> accessSequenceInSession(action: (OSequence) -> T): T {
        val currentSession = ODatabaseSession.getActiveSession()
        currentSession?.transaction
        val result = sessionCreator().use { session ->
            session.activateOnCurrentThread().begin()
            val sequenceLibrary: OSequenceLibrary = session.metadata.sequenceLibrary
            var oSequence = sequenceLibrary.getSequence(sequenceName)
            if (oSequence == null) {
                val params = CreateParams().setStart(initialValue).setIncrement(1)
                oSequence = sequenceLibrary.createSequence(sequenceName, SEQUENCE_TYPE.ORDERED, params)
                session.commit()
                session.begin()
            }
            val actionResult = action(oSequence)
            session.commit()
            actionResult
        }
        currentSession?.activateOnCurrentThread()
        return result
    }
}
