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
package jetbrains.exodus.entitystore.youtrackdb

import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal
import com.jetbrains.youtrack.db.internal.core.metadata.sequence.DBSequence
import jetbrains.exodus.entitystore.Sequence
import jetbrains.exodus.entitystore.youtrackdb.testutil.InMemoryYouTrackDB
import jetbrains.exodus.entitystore.youtrackdb.testutil.OTestMixin
import org.junit.Assert
import org.junit.Rule
import org.junit.Test
import kotlin.test.assertFailsWith

class YTDBSequenceImplTest : OTestMixin {
    @Rule
    @JvmField
    val orientDbRule = InMemoryYouTrackDB()

    override val youTrackDb = orientDbRule

    @Test
    fun `if session has no active transaction, create the sequence on the current session`() {
        youTrackDb.store.executeInTransaction { tx ->
            val seq = tx.getSequence("s1", 300L)
            Assert.assertEquals(300, seq.get())
            Assert.assertEquals(301, seq.increment())
        }
        youTrackDb.store.executeInTransaction { tx ->
            val seq = tx.getSequence("s1")
            Assert.assertEquals(301, seq.get())
        }
    }

    @Test
    fun `sequence may be created in one session and used in another`() {
        val seq: Sequence = youTrackDb.store.computeInTransaction { tx ->
            tx.getSequence("s1", 300)
        }
        youTrackDb.store.executeInTransaction {
            Assert.assertEquals(300, seq.get())
            Assert.assertEquals(301, seq.increment())
        }
        youTrackDb.store.executeInTransaction {
            Assert.assertEquals(301, seq.get())
            Assert.assertEquals(302, seq.increment())
        }
    }

    @Test
    fun `sequence_set() resets the current value`() {
        val seq = youTrackDb.store.computeInTransaction { tx ->
            val seq = tx.getSequence("s1", 300)
            Assert.assertEquals(300, seq.get())
            Assert.assertEquals(301, seq.increment())
            seq
        }
        youTrackDb.store.executeInTransaction {
            seq.set(200)
            Assert.assertEquals(200, seq.get())
            Assert.assertEquals(201, seq.increment())
        }
    }

    @Test
    fun `if you create a sequence in a transaction, nothing works`() {
        youTrackDb.withTxSession { session ->
            val seq = (session as DatabaseSessionInternal).metadata.sequenceLibrary.createSequence(
                "s1",
                DBSequence.SEQUENCE_TYPE.ORDERED, DBSequence.CreateParams().setStart(300).setIncrement(1)
            )
            assertFailsWith<RecordNotFoundException> { seq.current() }
        }
    }
}