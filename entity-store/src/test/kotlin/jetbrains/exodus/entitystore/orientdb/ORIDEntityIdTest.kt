package jetbrains.exodus.entitystore.orientdb

import jetbrains.exodus.entitystore.orientdb.testutil.InMemoryOrientDB
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

            vertex.setProperty(OVertexEntity.BACKWARD_COMPATIBLE_LOCAL_ENTITY_ID_PROPERTY_NAME, 200L)

            ORIDEntityId.fromVertex(vertex)

        }
    }
}