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
