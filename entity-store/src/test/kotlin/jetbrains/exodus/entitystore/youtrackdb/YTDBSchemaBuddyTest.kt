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

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal
import com.jetbrains.youtrack.db.internal.core.metadata.sequence.DBSequence
import jetbrains.exodus.entitystore.PersistentEntityId
import jetbrains.exodus.entitystore.youtrackdb.testutil.InMemoryYouTrackDB
import jetbrains.exodus.entitystore.youtrackdb.testutil.Issues
import jetbrains.exodus.entitystore.youtrackdb.testutil.OTestMixin
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Rule
import org.junit.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class YTDBSchemaBuddyTest : OTestMixin {

    @Rule
    @JvmField
    val orientDbRule = InMemoryYouTrackDB(initializeIssueSchema = false)

    override val youTrackDb = orientDbRule

    @Test
    fun `if autoInitialize is false, explicit initialization is required`() {
        withSession { session ->
            session.getOrCreateVertexClass(Issues.CLASS)
        }
        val issueId = withStoreTx { tx ->
            tx.createIssue("trista").id
        }
        val buddy = YTDBSchemaBuddyImpl(youTrackDb.provider, autoInitialize = false)

        val totallyExistingEntityId = PersistentEntityId(issueId.typeId, issueId.localId)
        withSession {
            assertEquals(RIDEntityId.EMPTY_ID, buddy.getOEntityId(it, totallyExistingEntityId))
        }

        withSession {
            buddy.initialize(it)
        }

        withSession {
            assertEquals(issueId, buddy.getOEntityId(it, totallyExistingEntityId))
        }
    }

    @Test
    fun `requireTypeExists() fails if the class is absent`() {
        val buddy = YTDBSchemaBuddyImpl(youTrackDb.provider)
        val className = "trista"
        withSession { session ->
            assertNull(session.getClass(className))
            assertFailsWith<IllegalStateException> { buddy.requireTypeExists(session, className) }
        }
    }

    @Test
    fun `getOEntityId() works with both existing and not existing EntityId`() {
        withSession { session ->
            session.getOrCreateVertexClass(Issues.CLASS)
        }
        val issueId = withStoreTx { tx ->
            tx.createIssue("trista").id
        }
        val buddy = YTDBSchemaBuddyImpl(youTrackDb.provider, autoInitialize = true)

        val notExistingEntityId = PersistentEntityId(300, 301)
        val partiallyExistingEntityId1 = PersistentEntityId(issueId.typeId, 301)
        val partiallyExistingEntityId2 = PersistentEntityId(300, issueId.localId)
        val totallyExistingEntityId = PersistentEntityId(issueId.typeId, issueId.localId)
        withSession {
            assertEquals(RIDEntityId.EMPTY_ID, buddy.getOEntityId(it, notExistingEntityId))
            assertEquals(RIDEntityId.EMPTY_ID, buddy.getOEntityId(it, partiallyExistingEntityId1))
            assertEquals(RIDEntityId.EMPTY_ID, buddy.getOEntityId(it, partiallyExistingEntityId2))
            assertEquals(issueId, buddy.getOEntityId(it, totallyExistingEntityId))
        }
    }

    /*
    * SchemaBuddy heavily depends on this invariant for the classId map consistency
    * */
    @Test
    fun `sequence does not roll back already generated values if the transaction is rolled back`() {
        withSession { session ->
            val params = DBSequence.CreateParams()
            params.start = 0
            (session as DatabaseSessionInternal).metadata.sequenceLibrary.createSequence(
                "seq",
                DBSequence.SEQUENCE_TYPE.ORDERED,
                params
            )
        }

        youTrackDb.withTxSession { session ->
            val res =
                (session as DatabaseSessionInternal).metadata.sequenceLibrary.getSequence("seq")
                    .next()
            assertEquals(1, res)
            session.rollback()
        }

        youTrackDb.withTxSession { session ->
            val res =
                (session as DatabaseSessionInternal).metadata.sequenceLibrary.getSequence("seq")
                    .next()
            assertEquals(2, res)
        }
    }

    @Test
    fun `can create an edge class in a transaction`() {
        val buddy = YTDBSchemaBuddyImpl(youTrackDb.provider)
        val edgeClassName = YTDBVertexEntity.edgeClassName("trista")

        // the edge class is not there
        withSession { session ->
            session.createVertexClass("issue")
            assertNull(session.getClass(edgeClassName))
        }

        // create the edge class in a transaction
        val issId = withSession { session ->
            session.begin()
            val iss = session.newVertex("issue")
            iss.save()

            val edgeClass = buddy.getOrCreateEdgeClass(session, "trista", "issue", "issue")
            assertNotNull(edgeClass)
            assertTrue(edgeClass.isEdgeType)

            session.commit()
            iss.identity
        }

        // the changes made in the transaction are still there
        withSession { session ->
            assertNotNull(session.loadVertex(issId))
        }
    }

    @Test
    fun `require both classId and localEntityId to create an instance`() {
        val oClass = youTrackDb.provider.withSession { oSession ->
            oSession.createVertexClassWithClassId("type1")
        }
        val typeID = oClass.requireClassId()
        youTrackDb.provider.withSession { oSession ->
            assertEquals("type1", youTrackDb.schemaBuddy.getType(oSession, typeID))
        }

    }

}
