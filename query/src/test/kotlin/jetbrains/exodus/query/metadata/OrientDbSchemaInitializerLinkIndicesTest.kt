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
package jetbrains.exodus.query.metadata

import com.orientechnologies.orient.core.record.ODirection
import com.orientechnologies.orient.core.record.OVertex
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException
import jetbrains.exodus.entitystore.orientdb.OVertexEntity
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.edgeClassName
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.linkTargetEntityIdPropertyName
import jetbrains.exodus.entitystore.orientdb.getTargetLocalEntityIds
import jetbrains.exodus.entitystore.orientdb.setLocalEntityId
import jetbrains.exodus.entitystore.orientdb.testutil.InMemoryOrientDB
import org.junit.Assert.assertEquals
import org.junit.Rule
import org.junit.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class OrientDbSchemaInitializerLinkIndicesTest {
    @Rule
    @JvmField
    val orientDb = InMemoryOrientDB(initializeIssueSchema = false)

    @Test
    fun `the same DeferredIndices are equal`() {
        val index1 = DeferredIndex("trista", setOf("prop1", "prop2"), true)
        val index2 = DeferredIndex("trista", setOf("prop1", "prop2"), true)
        assertTrue(index1.equals(index2))
    }

    @Test
    fun `complementary properties for indexed links are created only once`() {
        val model = model {
            entity("type2") {
                index(IndexedField("indexedAss3", isProperty = false))
            }
            entity("type1") {
                property("prop1", "int")
                index(IndexedField("indexedAss1", isProperty = false))
                index(IndexedField("prop1", isProperty = true), IndexedField("indexedAss2", isProperty = false))
            }
            association("type1", "indexedAss1", "type2", AssociationEndCardinality._0_n)
            association("type1", "indexedAss2", "type2", AssociationEndCardinality._0_n)
            association("type2", "indexedAss3", "type1", AssociationEndCardinality._0_n)
            association("type2", "ass1", "type1", AssociationEndCardinality._0_n)
        }

        val newIndexedLinks = orientDb.withSession { oSession ->
            oSession.applySchema(model).newIndexedLinks
        }

        assertEquals(
            setOf("indexedAss1", "indexedAss2"),
            newIndexedLinks.getValue("type1")
        )
        assertEquals(
            setOf("indexedAss3"),
            newIndexedLinks.getValue("type2")
        )

        val newIndexedLinksAgain = orientDb.withSession { oSession ->
            oSession.applySchema(model).newIndexedLinks
        }
        assertTrue(newIndexedLinksAgain.isEmpty())
    }

    @Test
    fun `when links get indexed, their complementary properties get initialized`() {
        val model = model {
            entity("type2")
            entity("type1")
            association("type1", "ass1", "type2", AssociationEndCardinality._0_n)
            association("type2", "ass2", "type1", AssociationEndCardinality._0_n)
        }

        orientDb.withSession { session ->
            session.applySchema(model)
        }

        val (id11, id12, id21) = orientDb.withTxSession { oSession ->
            val v11 = oSession.createVertexAndSetLocalEntityId("type1")
            val v12 = oSession.createVertexAndSetLocalEntityId("type1")
            val v21 = oSession.createVertexAndSetLocalEntityId("type2")

            v11.addEdge("ass1", v21)
            v21.addEdge("ass2", v11)
            v21.addEdge("ass2", v12)

            v11.save<OVertex>()
            v12.save<OVertex>()
            v21.save<OVertex>()
            Triple(v11.identity, v12.identity, v21.identity)
        }

        orientDb.withTxSession { session ->
            val type1 = session.getClass("type1")
            val type2 = session.getClass("type2")
            assertFalse(type1.existsProperty(linkTargetEntityIdPropertyName("ass1")))
            assertFalse(type2.existsProperty(linkTargetEntityIdPropertyName("ass2")))
        }

        val modelWithIndexes = model {
            entity("type2") {
                index(IndexedField("ass2", isProperty = false))
            }
            entity("type1") {
                index(IndexedField("ass1", isProperty = false))
            }
            association("type1", "ass1", "type2", AssociationEndCardinality._0_n)
            association("type2", "ass2", "type1", AssociationEndCardinality._0_n)
        }

        val (_, newIndexedLinks) = orientDb.withSession { session ->
            session.applySchema(modelWithIndexes)
        }

        orientDb.withSession { session ->
            session.initializeComplementaryPropertiesForNewIndexedLinks(newIndexedLinks)
        }

        orientDb.withTxSession { session ->
            val v11 = session.getRecord<OVertex>(id11)
            val v12 = session.getRecord<OVertex>(id12)
            val v21 = session.getRecord<OVertex>(id21)

            val bag11 = v11.getTargetLocalEntityIds("ass1")
            val bag21 = v21.getTargetLocalEntityIds("ass2")

            assertTrue(bag11.size() == 1)
            assertTrue(bag11.contains(v21))
            assertTrue(bag21.size() == 2)
            assertTrue(bag21.contains(v11))
            assertTrue(bag21.contains(v12))
        }
    }

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
            val (indices, _) = oSession.applySchema(model)
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
            val (indices, _) = oSession.applySchema(model)
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
            val (indices, _) = oSession.applySchema(model)
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
            val (indices, _) = oSession.applySchema(model)
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

    @Test
    fun `link duplicates are allowed if there is no indices`() {
        val model = model {
            entity("type1") {
                property("prop1", "int")
            }
            association("type1", "ass1", "type1", AssociationEndCardinality._0_n)
        }

        orientDb.withSession { oSession ->
            oSession.applySchema(model, indexForEverySimpleProperty = false)
        }

        val edgeClassName = edgeClassName("ass1")
        orientDb.withTxSession { oSession ->
            val oClass = oSession.getClass("type1")!!
            val v1 = oSession.newVertex(oClass)
            oSession.setLocalEntityId("type1", v1)
            v1.setProperty("prop1", 1)
            v1.save<OVertex>()

            val v2 = oSession.newVertex(oClass)
            oSession.setLocalEntityId("type1", v2)
            v2.setProperty("prop1", 2)
            v2.save<OVertex>()

            val entity1 = OVertexEntity(v1, orientDb.store)
            val entity2 = OVertexEntity(v2, orientDb.store)
            entity1.addLink("ass1", entity2)
            entity1.addLink("ass1", entity2)
        }

        orientDb.withTxSession { oSession ->
            val v1 = oSession.browseClass("type1").map { it.toVertex()!! }.first { it.getProperty<Int>("prop1") == 1 }
            val links: MutableIterable<OVertex> = v1.getVertices(ODirection.OUT, edgeClassName)
            assertEquals(2, links.count())
        }
    }

    @Test
    fun `link duplicates are forbidden if indices are created`() {
        val model = model {
            entity("type1") {
                property("prop1", "int")
            }
            association("type1", "ass1", "type1", AssociationEndCardinality._0_n)
        }

        orientDb.withSession { oSession ->
            val (indices, _) = oSession.applySchema(model, indexForEverySimpleProperty = false)
            oSession.applyIndices(indices)
        }

        val edgeClassName = edgeClassName("ass1")
        // trying to add the same edge in a single transaction
        val (id1, id2) = orientDb.withTxSession { oSession ->
            val oClass = oSession.getClass("type1")!!
            val v1 = oSession.newVertex(oClass)
            oSession.setLocalEntityId("type1", v1)
            v1.setProperty("prop1", 1)
            v1.save<OVertex>()

            val v2 = oSession.newVertex(oClass)
            oSession.setLocalEntityId("type1", v2)
            v2.setProperty("prop1", 2)
            v2.save<OVertex>()

            val entity1 = OVertexEntity(v1, orientDb.store)
            val entity2 = OVertexEntity(v2, orientDb.store)
            entity1.addLink("ass1", entity2)
            entity1.addLink("ass1", entity2)
            Pair(v1.identity, v2.identity)
        }

        // trying to add the same edge in another transaction
        orientDb.withTxSession { oSession ->
            val v1 = oSession.getRecord<OVertex>(id1)
            val v2 = oSession.getRecord<OVertex>(id2)
            val entity1 = OVertexEntity(v1, orientDb.store)
            val entity2 = OVertexEntity(v2, orientDb.store)
            entity1.addLink("ass1", entity2)
        }

        orientDb.withTxSession { oSession ->
            val v1 = oSession.browseClass("type1").map { it.toVertex()!! }.first { it.getProperty<Int>("prop1") == 1 }
            val links: MutableIterable<OVertex> = v1.getVertices(ODirection.OUT, edgeClassName)
            assertEquals(1, links.count())
        }
    }
}