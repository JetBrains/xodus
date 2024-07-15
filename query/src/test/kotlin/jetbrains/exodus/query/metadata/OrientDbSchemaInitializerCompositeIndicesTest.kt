package jetbrains.exodus.query.metadata

import com.orientechnologies.orient.core.record.OVertex
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException
import jetbrains.exodus.entitystore.orientdb.OVertexEntity
import jetbrains.exodus.entitystore.orientdb.setLocalEntityId
import jetbrains.exodus.entitystore.orientdb.testutil.InMemoryOrientDB
import org.junit.Rule
import org.junit.Test
import kotlin.test.assertFailsWith

class OrientDbSchemaInitializerCompositeIndicesTest {
    @Rule
    @JvmField
    val orientDb = InMemoryOrientDB(initializeIssueSchema = false)

    @Test
    fun `unique index prevents duplicates`() {
        val model = model {
            entity("type2")
            entity("type1") {
                index(IndexedField("ass1", isProperty = false))
            }
            association("type1", "ass1", "type2", AssociationEndCardinality._0_n)
        }

        orientDb.withSession { oSession ->
            val indices = oSession.applySchema(model)
            oSession.applyIndices(indices)
        }

        // (no links) == (no links)
        assertFailsWith<ORecordDuplicatedException> {
            orientDb.withTxSession { oSession ->
                val v1 = oSession.createVertexAndSetLocalEntityId("type1")
                val v2 = oSession.createVertexAndSetLocalEntityId("type1")

                v1.save<OVertex>()
                v2.save<OVertex>()
            }
        }

        // ({ v3 }) != (no links)
        val (id1, id2, id3) = orientDb.withTxSession { oSession ->
            val v1 = oSession.createVertexAndSetLocalEntityId("type1")
            val v2 = oSession.createVertexAndSetLocalEntityId("type1")
            val v3 = oSession.createVertexAndSetLocalEntityId("type2")

            v1.addIndexedEdge("ass1", v3)

            v1.save<OVertex>()
            v2.save<OVertex>()
            v3.save<OVertex>()
            Triple(v1.identity, v2.identity, v3.identity)
        }

        // ({ v3 }) == ({ v3 })
        assertFailsWith<ORecordDuplicatedException> {
            orientDb.withTxSession { oSession ->
                val v1 = oSession.getRecord<OVertex>(id1)
                val v3 = oSession.getRecord<OVertex>(id3)

                v1.addIndexedEdge("ass1", v3)

                v1.save<OVertex>()
                v3.save<OVertex>()
            }
        }

        // ({ v2, v3 }) == ({ v3 })
        assertFailsWith<ORecordDuplicatedException> {
            orientDb.withTxSession { oSession ->
                val v1 = oSession.getRecord<OVertex>(id1)
                val v2 = oSession.getRecord<OVertex>(id2)
                val v3 = oSession.getRecord<OVertex>(id3)

                v1.addIndexedEdge("ass1", v2)
                v2.addIndexedEdge("ass1", v3)

                v1.save<OVertex>()
                v2.save<OVertex>()
                v3.save<OVertex>()
            }
        }

        // ({ v2 }) != ({ v3 })
        orientDb.withTxSession { oSession ->
            val v1 = oSession.getRecord<OVertex>(id1)
            val v2 = oSession.getRecord<OVertex>(id2)
            val v3 = oSession.getRecord<OVertex>(id3)

            v1.deleteIndexedEdge("ass1", v3)
            v2.addIndexedEdge("ass1", v3)

            v1.save<OVertex>()
            v2.save<OVertex>()
            v3.save<OVertex>()
        }
    }

    @Test
    fun `composite indices prevent duplicates`() {
        val model = model {
            entity("type2")
            entity("type1") {
                property("prop1", "int")
                index(IndexedField("prop1", isProperty = true), IndexedField("ass1", isProperty = false))
            }
            association("type1", "ass1", "type2", AssociationEndCardinality._0_n)
        }

        orientDb.withSession { oSession ->
            val indices = oSession.applySchema(model)
            oSession.applyIndices(indices)
        }

        // (1, no links) == (1, no links)
        assertFailsWith<ORecordDuplicatedException> {
            orientDb.withTxSession { oSession ->
                val v1 = oSession.createVertexAndSetLocalEntityId("type1")
                val v2 = oSession.createVertexAndSetLocalEntityId("type1")

                v1.setProperty("prop1", 1)
                v2.setProperty("prop1", 1)

                v1.save<OVertex>()
                v2.save<OVertex>()
            }
        }

        // (1, { v3 }) == (1, { v3 }), trying to set in the same transaction
        assertFailsWith<ORecordDuplicatedException> {
            orientDb.withTxSession { oSession ->
                val v1 = oSession.createVertexAndSetLocalEntityId("type1")
                val v2 = oSession.createVertexAndSetLocalEntityId("type1")
                val v3 = oSession.createVertexAndSetLocalEntityId("type2")

                v1.setProperty("prop1", 1)
                v2.setProperty("prop1", 1)

                v1.addIndexedEdge("ass1", v3)
                v2.addIndexedEdge("ass1", v3)

                v1.save<OVertex>()
                v2.save<OVertex>()
                v3.save<OVertex>()
                Triple(v1.identity, v2.identity, v3.identity)
            }
        }

        // (1, { v3 } ) != (1, no links)
        val (id1, id2, id3) = orientDb.withTxSession { oSession ->
            val v1 = oSession.createVertexAndSetLocalEntityId("type1")
            val v2 = oSession.createVertexAndSetLocalEntityId("type1")
            val v3 = oSession.createVertexAndSetLocalEntityId("type2")

            v1.setProperty("prop1", 1)
            v2.setProperty("prop1", 1)

            v1.addIndexedEdge("ass1", v3)

            v1.save<OVertex>()
            v2.save<OVertex>()
            v3.save<OVertex>()
            Triple(v1.identity, v2.identity, v3.identity)
        }

        // (1, { v3 } ) == (1, { v3 } )
        assertFailsWith<ORecordDuplicatedException> {
            orientDb.withTxSession { oSession ->
                val v2 = oSession.getRecord<OVertex>(id2)
                val v3 = oSession.getRecord<OVertex>(id3)

                v2.addIndexedEdge("ass1", v3)

                v2.save<OVertex>()
                v3.save<OVertex>()
            }
        }

        // (1, { v2, v3 } ) != (1, no links)
        orientDb.withTxSession { oSession ->
            val v1 = oSession.getRecord<OVertex>(id1)
            val v2 = oSession.getRecord<OVertex>(id2)

            v1.addIndexedEdge("ass1", v2)

            v1.save<OVertex>()
            v2.save<OVertex>()
        }

        // (1, { v2, v3 } ) == (1, { v3 } ), who could think...
        assertFailsWith<ORecordDuplicatedException> {
            orientDb.withTxSession { oSession ->
                val v2 = oSession.getRecord<OVertex>(id2)
                val v3 = oSession.getRecord<OVertex>(id3)

                v2.addIndexedEdge("ass1", v3)

                v2.save<OVertex>()
                v3.save<OVertex>()
            }
        }
    }

    @Test
    fun `index gets updated if we remove the edge`() {
        val model = model {
            entity("type2")
            entity("type1") {
                property("prop1", "int")
                index(IndexedField("prop1", isProperty = true), IndexedField("ass1", isProperty = false))
            }
            association("type1", "ass1", "type2", AssociationEndCardinality._0_n)
        }

        orientDb.withSession { oSession ->
            val indices = oSession.applySchema(model)
            oSession.applyIndices(indices)
        }

        // (1, { v3 } ) != (1, no links)
        val (id1, id2, id3) = orientDb.withTxSession { oSession ->
            val v1 = oSession.createVertexAndSetLocalEntityId("type1")
            val v2 = oSession.createVertexAndSetLocalEntityId("type1")
            val v3 = oSession.createVertexAndSetLocalEntityId("type2")

            v1.setProperty("prop1", 1)
            v2.setProperty("prop1", 1)

            v1.addIndexedEdge("ass1", v3)

            v1.save<OVertex>()
            v2.save<OVertex>()
            v3.save<OVertex>()
            Triple(v1.identity, v2.identity, v3.identity)
        }

        // (1, no links) != (1, { v3 })
        orientDb.withTxSession { oSession ->
            val v1 = oSession.getRecord<OVertex>(id1)
            val v2 = oSession.getRecord<OVertex>(id2)
            val v3 = oSession.getRecord<OVertex>(id3)

            v1.deleteIndexedEdge("ass1", v3)
            v2.addIndexedEdge("ass1", v3)

            v1.save<OVertex>()
            v2.save<OVertex>()
            v3.save<OVertex>()
        }
    }

    @Test
    fun `composite indices with links via OVertexEntity`() {
        val model = model {
            entity("type2")
            entity("type1") {
                property("prop1", "int")
                index(IndexedField("prop1", isProperty = true), IndexedField("ass1", isProperty = false))
            }
            association("type1", "ass1", "type2", AssociationEndCardinality._0_n)
        }

        orientDb.withSession { oSession ->
            val indices = oSession.applySchema(model)
            oSession.applyIndices(indices)
        }

        // (1, { v3 } ) != (1, no links)
        val (id1, id2, id3) = orientDb.withTxSession { oSession ->
            val v1 = oSession.newVertex("type1")
            val v2 = oSession.newVertex("type1")
            val v3 = oSession.newVertex("type2")

            oSession.setLocalEntityId("type1", v1)
            oSession.setLocalEntityId("type1", v2)
            oSession.setLocalEntityId("type2", v3)

            val e1 = OVertexEntity(v1, orientDb.store)
            val e2 = OVertexEntity(v2, orientDb.store)
            val e3 = OVertexEntity(v3, orientDb.store)
            e1.setProperty("prop1", 1)
            e2.setProperty("prop1", 1)

            e1.addLink("ass1", e3)

            v1.save<OVertex>()
            v2.save<OVertex>()
            v3.save<OVertex>()
            Triple(v1.identity, v2.identity, v3.identity)
        }

        // (1, { v3 } ) == (1, { v3 } )
        assertFailsWith<ORecordDuplicatedException> {
            orientDb.withTxSession { oSession ->
                val v2 = oSession.getRecord<OVertex>(id2)
                val v3 = oSession.getRecord<OVertex>(id3)

                val e2 = OVertexEntity(v2, orientDb.store)
                val e3 = OVertexEntity(v3, orientDb.store)

                e2.addLink("ass1", e3)
            }
        }

        // (1, no links) != (1, { v3 } )
        orientDb.withTxSession { oSession ->
            val v1 = oSession.getRecord<OVertex>(id1)
            val v2 = oSession.getRecord<OVertex>(id2)
            val v3 = oSession.getRecord<OVertex>(id3)

            val e1 = OVertexEntity(v1, orientDb.store)
            val e2 = OVertexEntity(v2, orientDb.store)
            val e3 = OVertexEntity(v3, orientDb.store)

            e1.deleteLink("ass1", e3)
            e2.addLink("ass1", e3)
        }
    }
}