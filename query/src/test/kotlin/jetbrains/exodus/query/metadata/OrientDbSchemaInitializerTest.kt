package jetbrains.exodus.query.metadata

import com.orientechnologies.orient.core.db.ODatabaseSession
import jetbrains.exodus.entitystore.orientdb.InMemoryOrientDB
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test

class OrientDbSchemaInitializerTest {
    @Rule
    @JvmField
    val orientDb = InMemoryOrientDB()

    @Test
    fun `create vertex-class for every entity`() {
        orientDb.withSession { oSession ->
            val model = model {
                entity("type1")
                entity("type2")
            }

            val schemaInit = OrientDbSchemaInitializer(model, oSession)
            schemaInit.apply()

            oSession.assertContainsVertexClass("type1")
            oSession.assertContainsVertexClass("type2")
        }
    }

    private fun ODatabaseSession.assertContainsVertexClass(name: String) {
        assertTrue(getClass(name)!!.superClassesNames.contains("V"))
    }

    private fun model(initialize: ModelMetaDataImpl.() -> Unit): ModelMetaDataImpl {
        val model = ModelMetaDataImpl()
        model.initialize()
        return model
    }

    private fun ModelMetaDataImpl.entity(type: String, superType: String? = null, init: EntityMetaDataImpl.() -> Unit = {}) {
        val entity = EntityMetaDataImpl()
        entity.type = type
        entity.superType = superType
        entity.init()
        addEntityMetaData(entity)
    }
}