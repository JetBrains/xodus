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

import com.orientechnologies.orient.core.metadata.sequence.OSequence
import jetbrains.exodus.entitystore.PersistentEntityId
import jetbrains.exodus.entitystore.orientdb.testutil.InMemoryOrientDB
import jetbrains.exodus.entitystore.orientdb.testutil.createIssue
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Rule
import org.junit.Test
import kotlin.test.assertFailsWith

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
            assertEquals(ORIDEntityId.EMPTY_ID, buddy.getOEntityId(it, totallyExistingEntityId))
        }

        orientDb.withSession {
            buddy.initialize(it)
        }

        orientDb.withSession {
            assertEquals(issueId, buddy.getOEntityId(it, totallyExistingEntityId))
        }
    }

    @Test
    fun `requireTypeExists() fails if the class is absent`() {
        val buddy = OSchemaBuddyImpl(orientDb.provider)
        val className = "trista"
        orientDb.withSession { session ->
            assertNull(session.getClass(className))
            assertFailsWith<IllegalStateException> { buddy.requireTypeExists(session, className) }
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
            assertEquals(ORIDEntityId.EMPTY_ID, buddy.getOEntityId(it, notExistingEntityId))
            assertEquals(ORIDEntityId.EMPTY_ID, buddy.getOEntityId(it, partiallyExistingEntityId1))
            assertEquals(ORIDEntityId.EMPTY_ID, buddy.getOEntityId(it, partiallyExistingEntityId2))
            assertEquals(issueId, buddy.getOEntityId(it, totallyExistingEntityId))
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
        }
    }

}
