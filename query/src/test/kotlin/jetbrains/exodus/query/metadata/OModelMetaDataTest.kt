package jetbrains.exodus.query.metadata

import jetbrains.exodus.entitystore.orientdb.testutil.InMemoryOrientDB
import org.junit.Assert
import org.junit.Rule
import org.junit.Test

class OModelMetaDataTest {
    @Rule
    @JvmField
    val orientDb = InMemoryOrientDB(initializeIssueSchema = false)

    @Test
    fun `prepare() applies the schema to OrientDB`() {
        val model = oModel(orientDb.provider) {
            entity("type1")
            entity("type2")
        }

        orientDb.withSession { session ->
            Assert.assertNull(session.getClass("type1"))
            Assert.assertNull(session.getClass("type2"))
        }

        model.prepare()

        orientDb.withSession { session ->
            session.assertVertexClassExists("type1")
            session.assertVertexClassExists("type2")
        }
    }

    @Test
    fun `addAssociation() implicitly call prepare() and applies the schema to OrientDB`() {
        oModel(orientDb.provider) {
            entity("type1")
            entity("type2") {
                association("ass1", "type1", AssociationEndCardinality._1)
            }
        }

        orientDb.withSession { session ->
            session.assertVertexClassExists("type1")
            session.assertVertexClassExists("type2")
            session.assertAssociationExists("type2", "type1", "ass1", AssociationEndCardinality._1)
        }
    }

    @Test
    fun `removeAssociation()`() {
        val model = oModel(orientDb.provider) {
            entity("type1")
            entity("type2") {
                association("ass1", "type1", AssociationEndCardinality._1)
            }
        }

        model.removeAssociation("type2", "ass1")
        orientDb.withSession { session ->
            session.assertAssociationNotExist("type2", "type1", "ass1", requireEdgeClass = true)
        }
    }

    @Test
    fun indices() {

    }
}