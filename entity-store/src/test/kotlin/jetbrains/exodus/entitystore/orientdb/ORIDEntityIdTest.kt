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

import jetbrains.exodus.entitystore.PersistentEntityId
import jetbrains.exodus.entitystore.orientdb.testutil.InMemoryOrientDB
import jetbrains.exodus.entitystore.orientdb.testutil.createIssue
import org.junit.Assert.assertEquals
import org.junit.Rule
import org.junit.Test
import kotlin.test.assertFailsWith

class ORIDEntityIdTest {
    @Rule
    @JvmField
    val orientDb = InMemoryOrientDB()

    @Test
    fun `require both classId and localEntityId to create an instance`() {
        orientDb.withSession { oSession ->
            val oClass = oSession.createVertexClass("type1")
            val vertex = oSession.newVertex(oClass)
            assertFailsWith<IllegalStateException> {
                ORIDEntityId.fromVertex(vertex)
            }

            oClass.setCustom(OVertexEntity.CLASS_ID_CUSTOM_PROPERTY_NAME, 300.toString())

            assertFailsWith<IllegalStateException> {
                ORIDEntityId.fromVertex(vertex)
            }

            vertex.setProperty(OVertexEntity.LOCAL_ENTITY_ID_PROPERTY_NAME, 200L)

            ORIDEntityId.fromVertex(vertex)

        }
    }

    @Test
    fun `id representation is the same as for PersistentEntityId`() {
        val id = orientDb.createIssue("trista").id
        val legacyId = PersistentEntityId(id.typeId, id.localId)
        val idRepresentation = id.toString()
        val legacyIdRepresentation = legacyId.toString()

        assertEquals(legacyIdRepresentation, idRepresentation)
    }
}