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

import com.orientechnologies.orient.core.exception.ORecordNotFoundException
import com.orientechnologies.orient.core.metadata.sequence.OSequence
import jetbrains.exodus.entitystore.Sequence
import jetbrains.exodus.entitystore.orientdb.testutil.InMemoryOrientDB
import jetbrains.exodus.entitystore.orientdb.testutil.OTestMixin
import org.junit.Assert
import org.junit.Rule
import org.junit.Test
import kotlin.test.assertFailsWith

class OSequenceImplTest : OTestMixin {
    @Rule
    @JvmField
    val orientDbRule = InMemoryOrientDB()

    override val orientDb = orientDbRule

    @Test
    fun `if session has no active transaction, create the sequence on the current session`() {
        orientDb.store.executeInTransaction { tx ->
            val seq = tx.getSequence("s1", 300L)
            Assert.assertEquals(300, seq.get())
            Assert.assertEquals(301, seq.increment())
        }
        orientDb.store.executeInTransaction { tx ->
            val seq = tx.getSequence("s1")
            Assert.assertEquals(301, seq.get())
        }
    }

    @Test
    fun `sequence may be created in one session and used in another`() {
        val seq: Sequence = orientDb.store.computeInTransaction { tx ->
            tx.getSequence("s1", 300)
        }
        orientDb.store.executeInTransaction {
            Assert.assertEquals(300, seq.get())
            Assert.assertEquals(301, seq.increment())
        }
        orientDb.store.executeInTransaction {
            Assert.assertEquals(301, seq.get())
            Assert.assertEquals(302, seq.increment())
        }
    }

    @Test
    fun `sequence_set() resets the current value`() {
        val seq = orientDb.store.computeInTransaction { tx ->
            val seq = tx.getSequence("s1", 300)
            Assert.assertEquals(300, seq.get())
            Assert.assertEquals(301, seq.increment())
            seq
        }
        orientDb.store.executeInTransaction {
            seq.set(200)
            Assert.assertEquals(200, seq.get())
            Assert.assertEquals(201, seq.increment())
        }
    }

    @Test
    fun `if you create a sequence in a transaction, nothing works`() {
        orientDb.withTxSession { session ->
            val seq = session.metadata.sequenceLibrary.createSequence(
                "s1",
                OSequence.SEQUENCE_TYPE.ORDERED, OSequence.CreateParams().setStart(300).setIncrement(1)
            )
            assertFailsWith<ORecordNotFoundException> { seq.current() }
        }
    }
}