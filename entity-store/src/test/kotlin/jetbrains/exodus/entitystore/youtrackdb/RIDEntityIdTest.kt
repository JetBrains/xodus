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

import com.jetbrains.youtrack.db.api.record.Vertex
import jetbrains.exodus.entitystore.PersistentEntityId
import jetbrains.exodus.entitystore.youtrackdb.testutil.InMemoryYouTrackDB
import jetbrains.exodus.entitystore.youtrackdb.testutil.createIssue
import org.junit.Assert.assertEquals
import org.junit.Rule
import org.junit.Test
import kotlin.test.assertFailsWith

class RIDEntityIdTest {
    @Rule
    @JvmField
    val youTrackDb = InMemoryYouTrackDB()

    @Test
    fun `require both classId and localEntityId to create an instance`() {
        val oClass = youTrackDb.provider.withSession { oSession ->
            oSession.createVertexClass("type1")
        }
        var vertex: Vertex = youTrackDb.withTxSession { oSession ->
            val v = oSession.newVertex(oClass)
            v.save()
            v
        }
        youTrackDb.withTxSession {
            assertFailsWith<IllegalStateException> {
                vertex = it.bindToSession(vertex)
                RIDEntityId.fromVertex(vertex)
            }
        }

        youTrackDb.provider.withSession {
            oClass.setCustom(it, YTDBVertexEntity.CLASS_ID_CUSTOM_PROPERTY_NAME, 300.toString())
        }
        youTrackDb.withTxSession {
            vertex = it.bindToSession(vertex)
            assertFailsWith<IllegalStateException> {
                RIDEntityId.fromVertex(vertex)
            }

            vertex.setProperty(YTDBVertexEntity.LOCAL_ENTITY_ID_PROPERTY_NAME, 200L)
            RIDEntityId.fromVertex(vertex)
        }
    }

    @Test
    fun `id representation is the same as for PersistentEntityId`() {
        val id = youTrackDb.createIssue("trista").id
        val legacyId = PersistentEntityId(id.typeId, id.localId)
        val idRepresentation = id.toString()
        val legacyIdRepresentation = legacyId.toString()

        assertEquals(legacyIdRepresentation, idRepresentation)
    }
}
