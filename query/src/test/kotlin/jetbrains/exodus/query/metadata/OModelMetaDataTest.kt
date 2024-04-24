package jetbrains.exodus.query.metadata

import jetbrains.exodus.entitystore.orientdb.testutil.InMemoryOrientDB
import org.junit.Assert
import org.junit.Rule
import org.junit.Test
import kotlin.test.assertFailsWith

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
    fun addAssociation() {
        val model = oModel(orientDb.provider) {
            entity("type1")
            entity("type2")
        }

        model.prepare()

        model.addAssociation(
            "type2", "type1", AssociationType.Directed, "ass1", AssociationEndCardinality._1,
            false, false, false, false, null,
            null, false, false, false, false
        )

        orientDb.withSession { session ->
            session.assertAssociationExists("type2", "type1", "ass1", AssociationEndCardinality._1)
        }
    }

    @Test
    fun `if there is an active session on the current thread, the model uses it`() {
        val model = oModel(orientDb.provider) {
            entity("type1")
            entity("type2")
        }

        orientDb.withSession {
            model.prepare()
            model.addAssociation(
                "type2", "type1", AssociationType.Directed, "ass1", AssociationEndCardinality._1,
                false, false, false, false, null,
                null, false, false, false, false
            )
            model.removeAssociation("type2", "ass1")
        }
    }

    @Test
    fun `if there is an active transaction, throw an exception`() {
        val model = oModel(orientDb.provider) {
            entity("type1")
            entity("type2")
        }

        orientDb.withSession { session ->
            session.begin()
            assertFailsWith<AssertionError> {
                model.prepare()
            }
            assertFailsWith<AssertionError> {
                model.addAssociation(
                    "type2", "type1", AssociationType.Directed, "ass1", AssociationEndCardinality._1,
                    false, false, false, false, null,
                    null, false, false, false, false
                )
            }
            assertFailsWith<AssertionError> {
                model.removeAssociation("type2", "ass1")
            }
        }
    }

    @Test
    fun removeAssociation() {
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
    fun `prepare() creates indices`() {
        val model = oModel(orientDb.provider) {
            entity("type1") {
                property("prop1", "int")
                property("prop2", "long")

                index("prop1", "prop2")
            }
        }

        model.prepare()

        orientDb.withSession { session ->
            session.checkIndex("type1", true, "prop1", "prop2")
        }
    }
}