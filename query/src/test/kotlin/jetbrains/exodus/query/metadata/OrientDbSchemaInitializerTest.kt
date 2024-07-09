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

import com.orientechnologies.orient.core.db.record.ridbag.ORidBag
import com.orientechnologies.orient.core.metadata.schema.OProperty
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.ODirection
import com.orientechnologies.orient.core.record.OVertex
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException
import jetbrains.exodus.entitystore.orientdb.OVertexEntity
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.CLASS_ID_CUSTOM_PROPERTY_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.LOCAL_ENTITY_ID_PROPERTY_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.STRING_BLOB_CLASS_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.edgeClassName
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.linkTargetEntityIdPropertyName
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.localEntityIdSequenceName
import jetbrains.exodus.entitystore.orientdb.requireClassId
import jetbrains.exodus.entitystore.orientdb.requireLocalEntityId
import jetbrains.exodus.entitystore.orientdb.setLocalEntityId
import jetbrains.exodus.entitystore.orientdb.testutil.InMemoryOrientDB
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull

class OrientDbSchemaInitializerTest {
    @Rule
    @JvmField
    val orientDb = InMemoryOrientDB(initializeIssueSchema = false)

    @Test
    fun `create vertex-class for every entity`() = orientDb.provider.acquireSession().use  { oSession ->
        val model = model {
            entity("type1")
            entity("type2")
        }

        oSession.applySchema(model)

        oSession.assertVertexClassExists("type1")
        oSession.assertVertexClassExists("type2")
    }

    @Test
    fun `set super-classes`() = orientDb.provider.acquireSession().use  { oSession ->
        val model = model {
            entity("type1")
            entity("type2", "type1")
            entity("type3", "type2")
        }

        oSession.applySchema(model)

        oSession.assertHasSuperClass("type2", "type1")
        oSession.assertHasSuperClass("type3", "type2")
    }

    @Test
    fun `simple properties of known types are created`() = orientDb.provider.acquireSession().use  { oSession ->
        val model = model {
            entity("type1") {
                for (type in supportedSimplePropertyTypes) {
                    property("prop$type", type)
                    property("requiredProp$type", type, required = true)
                }
            }
        }

        oSession.applySchema(model)

        val oClass = oSession.getClass("type1")!!
        for (type in supportedSimplePropertyTypes) {
            val requiredProp = oClass.getProperty("requiredProp$type")!!
            val prop = oClass.getProperty("prop$type")!!

            assertEquals(getOType(type), requiredProp.type)
            assertEquals(getOType(type), prop.type)

            requiredProp.check(required = true, notNull = true)
            prop.check(required = false, notNull = false)
        }
    }

    @Test
    fun `simple properties of not-known types cause exception`(): Unit = orientDb.provider.acquireSession().use  { oSession ->
        val model = model {
            entity("type1") {
                property("prop1", "notSupportedType")
            }
        }

        assertFailsWith<IllegalArgumentException>() {
            oSession.applySchema(model)
        }
    }

    @Test
    fun `SchemaBuddy impl can correctly initialize StringBlob and Blob`(){
        val model = model {
            entity("type1") {
                blobProperty("blob1")
                stringBlobProperty("strBlob1")
            }
        }
        orientDb.provider.acquireSession().use  {
            it.applySchema(model)
        }
        orientDb.schemaBuddy.initialize()

        orientDb.provider.acquireSession().use  {
            assertEquals(null, it.metadata.schema.getClass(STRING_BLOB_CLASS_NAME).getCustom(CLASS_ID_CUSTOM_PROPERTY_NAME))
        }
    }

    @Test
    fun `create blob and string-blob properties`(): Unit = orientDb.provider.acquireSession().use  { oSession ->
        val model = model {
            entity("type1") {
                blobProperty("blob1")
                stringBlobProperty("strBlob1")
            }
        }

        oSession.applySchema(model)

        val blobClass = oSession.getClass(OVertexEntity.BINARY_BLOB_CLASS_NAME)!!
        val blobDataProp = blobClass.getProperty(OVertexEntity.DATA_PROPERTY_NAME)!!
        assertEquals(OType.BINARY, blobDataProp.type)

        val strBlobClass = oSession.getClass(STRING_BLOB_CLASS_NAME)!!
        val strBlobDataProp = strBlobClass.getProperty(OVertexEntity.DATA_PROPERTY_NAME)!!
        assertEquals(OType.BINARY, strBlobDataProp.type)

        val entity = oSession.getClass("type1")

        val blobProp = entity.getProperty("blob1")!!
        assertEquals(OType.LINK, blobProp.type)
        assertEquals(OVertexEntity.BINARY_BLOB_CLASS_NAME, blobProp.linkedClass!!.name)

        val strBlobProp = entity.getProperty("strBlob1")!!
        assertEquals(OType.LINK, strBlobProp.type)
        assertEquals(STRING_BLOB_CLASS_NAME, strBlobProp.linkedClass!!.name)
    }

    @Test
    fun `embedded set properties with supported types`() {
        val indices = orientDb.provider.acquireSession().use  { oSession ->
            val model = model {
                entity("type1") {
                    for (type in supportedSimplePropertyTypes) {
                        setProperty("setProp$type", type)
                    }
                }
            }

            val indices = oSession.applySchema(model)

            val oClass = oSession.getClass("type1")!!
            for (type in supportedSimplePropertyTypes) {
                val prop = oClass.getProperty("setProp$type")!!
                assertEquals(OType.EMBEDDEDSET, prop.type)
                assertEquals(getOType(type), prop.linkedType)

                indices.checkIndex("type1", unique = false, "setProp$type")
            }
            indices
        }

        orientDb.provider.acquireSession().use  { oSession ->
            oSession.applyIndices(indices)

            for (type in supportedSimplePropertyTypes) {
                oSession.checkIndex("type1", unique = false, "setProp$type")
            }
        }
    }

    @Test
    fun `embedded set properties with not-supported types cause exception`(): Unit = orientDb.provider.acquireSession().use  { oSession ->
        val model = model {
            entity("type1") {
                setProperty("setProp$type", "cavaBanga")
            }
        }

        assertFailsWith<IllegalArgumentException> {
            oSession.applySchema(model)
        }
    }

    @Test
    fun `one-directional associations`(): Unit = orientDb.provider.acquireSession().use  { oSession ->
        val model = model {
            entity("type1")
            entity("type2")
            for (cardinality in AssociationEndCardinality.entries) {
                association("type2", "prop1$cardinality", "type1", cardinality)
            }
        }

        oSession.applySchema(model)

        for (cardinality in AssociationEndCardinality.entries) {
            oSession.assertAssociationExists("type2", "type1", "prop1$cardinality", cardinality)
        }
    }

    @Test
    fun `two association with the same name to a single type`(): Unit = orientDb.provider.acquireSession().use  { oSession ->
        val model = model {
            entity("type1")
            entity("type2")
            entity("type3")
            association("type2", "link1", "type1", AssociationEndCardinality._0_n)
            association("type3", "link1", "type1", AssociationEndCardinality._0_n)
        }

        oSession.applySchema(model)

        oSession.assertAssociationExists("type2", "type1", "link1", AssociationEndCardinality._0_n)
        oSession.assertAssociationExists("type3", "type1", "link1", AssociationEndCardinality._0_n)
    }

    @Test
    fun `one-directional associations ignore cardinality`(): Unit = orientDb.provider.acquireSession().use  { oSession ->
        val model = model {
            entity("type1")
            entity("type2")
            for (cardinality in AssociationEndCardinality.entries) {
                association("type2", "prop1$cardinality", "type1", cardinality)
            }
        }

        oSession.applySchema(model, applyLinkCardinality = false)

        for (cardinality in AssociationEndCardinality.entries) {
            oSession.assertAssociationExists("type2", "type1", "prop1$cardinality", null)
        }
    }

    @Test
    fun `two-directional associations`(): Unit = orientDb.provider.acquireSession().use  { oSession ->
        val model = model {
            entity("type1")
            entity("type2")

            for (cardinality1 in AssociationEndCardinality.entries) {
                for (cardinality2 in AssociationEndCardinality.entries) {
                    twoDirectionalAssociation(
                        sourceEntity = "type1",
                        sourceName = "prop1${cardinality1}_${cardinality2}",
                        sourceCardinality = cardinality1,
                        targetEntity = "type2",
                        targetName = "prop2${cardinality2}_${cardinality1}",
                        targetCardinality = cardinality2
                    )
                }
            }
        }

        oSession.applySchema(model)

        for (cardinality1 in AssociationEndCardinality.entries) {
            for (cardinality2 in AssociationEndCardinality.entries) {
                oSession.assertAssociationExists("type1", "type2", "prop1${cardinality1}_${cardinality2}", cardinality1)
                oSession.assertAssociationExists("type2", "type1", "prop2${cardinality2}_${cardinality1}", cardinality2)
            }
        }
    }

    @Test
    fun `composite indices with links`() {
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

        val edgeClassName = edgeClassName("ass1")
        val linkTargetLocalEntityIdName = linkTargetEntityIdPropertyName("ass1")

        // (1, no links) == (1, no links)
        assertFailsWith<ORecordDuplicatedException> {
            orientDb.withTxSession { oSession ->
                val v1 = oSession.newVertex("type1")
                val v2 = oSession.newVertex("type1")

                oSession.setLocalEntityId("type1", v1)
                oSession.setLocalEntityId("type1", v2)

                v1.setProperty("prop1", 1)
                v2.setProperty("prop1", 1)

                v1.save<OVertex>()
                v2.save<OVertex>()
            }
        }

        // (1, { v3 } ) != (1, no links)
        val (id1, id2, id3) = orientDb.withTxSession { oSession ->
            val v1 = oSession.newVertex("type1")
            val v2 = oSession.newVertex("type1")
            val v3 = oSession.newVertex("type2")

            oSession.setLocalEntityId("type1", v1)
            oSession.setLocalEntityId("type1", v2)
            oSession.setLocalEntityId("type2", v3)

            v1.setProperty("prop1", 1)
            v2.setProperty("prop1", 1)

            val bag = v1.getProperty<ORidBag>(linkTargetLocalEntityIdName) ?: ORidBag()
            v1.addEdge(v3, edgeClassName)
            bag.add(v3)
            v1.setProperty(linkTargetLocalEntityIdName, bag)

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

                val bag = v2.getProperty<ORidBag>(linkTargetLocalEntityIdName) ?: ORidBag()
                v2.addEdge(v3, edgeClassName)
                bag.add(v3)

                v2.setProperty(linkTargetLocalEntityIdName, bag)

                v2.save<OVertex>()
                v3.save<OVertex>()
            }
        }

        // (1, { v2, v3 } ) != (1, no links)
        orientDb.withTxSession { oSession ->
            val v1 = oSession.getRecord<OVertex>(id1)
            val v2 = oSession.getRecord<OVertex>(id2)

            val bag = v1.getProperty<ORidBag>(linkTargetLocalEntityIdName) ?: ORidBag()
            v1.addEdge(v2, edgeClassName)
            bag.add(v2)
            v1.setProperty(linkTargetLocalEntityIdName, bag)

            v1.save<OVertex>()
            v2.save<OVertex>()
        }

        // (1, { v2, v3 } ) == (1, { v3 } ), who could think...
        assertFailsWith<ORecordDuplicatedException> {
            orientDb.withTxSession { oSession ->
                val v2 = oSession.getRecord<OVertex>(id2)
                val v3 = oSession.getRecord<OVertex>(id3)

                val bag = v2.getProperty<ORidBag>(linkTargetLocalEntityIdName) ?: ORidBag()
                v2.addEdge(v3, edgeClassName)
                bag.add(v3)

                v2.setProperty(linkTargetLocalEntityIdName, bag)

                v2.save<OVertex>()
                v3.save<OVertex>()
            }
        }
    }

    @Test
    fun `own indices`() {
        val indices = orientDb.provider.acquireSession().use  { oSession ->
            val model = model {
                entity("type1") {
                    property("prop1", "int")
                    property("prop2", "long")
                    property("prop3", "string")
                    property("prop4", "string")

                    index("prop1", "prop2")
                    index("prop3")
                }
            }

            val indices = oSession.applySchema(model)

            indices.checkIndex("type1", unique = true, "prop1", "prop2")
            indices.checkIndex("type1", unique = true, "prop3")

            val entity = oSession.getClass("type1")!!
            // indices are not created right away, they are created after data migration
            assertTrue(entity.indexes.isEmpty())

            // indexed properties in Xodus are required and not-nullable
            entity.getProperty("prop1").check(required = true, notNull = true)
            entity.getProperty("prop2").check(required = true, notNull = true)
            entity.getProperty("prop3").check(required = true, notNull = true)
            entity.getProperty("prop4").check(required = false, notNull = false)

            indices
        }

        orientDb.provider.acquireSession().use  { oSession ->
            oSession.applyIndices(indices)

            oSession.checkIndex("type1", true, "prop1", "prop2")
            oSession.checkIndex("type1", true, "prop3")
        }
    }

    @Test
    fun `unique index forbids to create vertices with the same property value`() {
        val model = model {
            entity("type1") {
                property("prop1", "int")
                property("prop2", "long")
                index("prop1")
            }
        }

        orientDb.withSession { oSession ->
            val indices = oSession.applySchema(model, indexForEverySimpleProperty = false)
            oSession.applyIndices(indices)
        }

        assertFailsWith<ORecordDuplicatedException> {
            orientDb.withTxSession { oSession ->
                val oClass = oSession.getClass("type1")!!
                val v1 = oSession.newVertex(oClass)
                oSession.setLocalEntityId("type1", v1)
                v1.requireLocalEntityId()
                v1.setProperty("prop1", 3)
                v1.setProperty("prop2", 4)
                v1.save<OVertex>()

                val v2 = oSession.newVertex(oClass)
                oSession.setLocalEntityId("type1", v2)
                v2.setProperty("prop1", 3L)
                v2.setProperty("prop2", 4L)
                v2.save<OVertex>()
            }
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
            val indices = oSession.applySchema(model, indexForEverySimpleProperty = false)
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

    @Test
    fun `index for every simple property if required`() = orientDb.provider.acquireSession().use { oSession ->
        val model = model {
            entity("type1") {
                property("prop1", "int")
                property("prop2", "long")
                property("prop3", "string")
                property("prop4", "string")
            }
        }

        val indices = oSession.applySchema(model, indexForEverySimpleProperty = true)

        indices.checkIndex("type1", unique = false, "prop1")
        indices.checkIndex("type1", unique = false, "prop2")
        indices.checkIndex("type1", unique = false, "prop3")
        indices.checkIndex("type1", unique = false, "prop4")

        val entity = oSession.getClass("type1")!!
        // indices are not created right away, they are created after data migration
        assertTrue(entity.indexes.isEmpty())
    }

    @Test
    fun `no indices for simple properties by default`() = orientDb.provider.acquireSession().use  { oSession ->
        val model = model {
            entity("type1") {
                property("prop1", "int")
                property("prop2", "long")
                property("prop3", "string")
                property("prop4", "string")
            }
        }

        val indices = oSession.applySchema(model)
        assertTrue(indices.none { (indexName, _) -> indexName.contains("prop")})
    }

    @Test
    fun `addAssociation, removeAssociation`(): Unit = orientDb.provider.acquireSession().use  { session ->
        val model = model {
            entity("type1")
            entity("type2")
        }

        session.applySchema(model)

        for (cardinality in AssociationEndCardinality.entries) {
            session.addAssociation(OAssociationMetadata(
                name = "ass1${cardinality.name}",
                outClassName = "type1",
                inClassName = "type2",
                cardinality = cardinality
            ))
        }

        for (cardinality in AssociationEndCardinality.entries) {
            session.assertAssociationExists("type1", "type2", "ass1${cardinality.name}", cardinality)
        }

        for (cardinality in AssociationEndCardinality.entries) {
            session.removeAssociation(
                sourceClassName = "type1",
                targetClassName = "type2",
                associationName = "ass1${cardinality.name}"
            )
        }

        for (cardinality in AssociationEndCardinality.entries) {
            /*
            * We do not delete the edge class when deleting an association because
            * it (the edge class) may be used by other associations.
            *
            * Maybe it is possible to check an edge class if it is not used anywhere, but
            * we do not do it at the moment. Maybe some day in the future.
            * */
            session.assertAssociationNotExist("type1", "type2", "ass1${cardinality.name}", requireEdgeClass = true)
        }
    }


    // Backward compatible EntityId

    @Test
    fun `classId is a monotonically increasing long`(): Unit = orientDb.provider.acquireSession().use  { oSession ->
        val types = mutableListOf("type0", "type1", "type2")
        val model = model {
            for (type in types) {
                entity(type)
            }
        }

        oSession.applySchema(model)

        val classIds = mutableSetOf<Int>()
        val classIdToClassName = mutableMapOf<Int, String>()
        for (type in types) {
            val classId = oSession.getClass(type).requireClassId()
            classIdToClassName[classId] = type
            classIds.add(classId)
        }
        assertEquals(setOf(0, 1, 2), classIds)


        // emulate the next run of the application with new classes in the codebase
        types.add("type4")
        types.add("type5")
        val anotherModel = model {
            for (type in types) {
                entity(type)
            }
        }

        oSession.applySchema(anotherModel)

        classIds.clear()
        for (type in types) {
            val classId = oSession.getClass(type).requireClassId()
            // classId is not changed if it has been already assigned
            if (classId in classIdToClassName) {
                assertEquals(classIdToClassName.getValue(classId), type)
            }
            classIds.add(classId)
        }
        assertEquals(setOf(0, 1, 2, 3, 4), classIds)
    }

    @Test
    fun `every class gets localEntityId property`(): Unit = orientDb.provider.acquireSession().use  { oSession ->
        val types = mutableListOf("type0", "type1", "type2")
        val model = model {
            for (type in types) {
                entity(type)
            }
        }

        val indices = oSession.applySchema(model)

        val sequences = oSession.metadata.sequenceLibrary
        for (type in types) {
            assertNotNull(oSession.getClass(type).getProperty(LOCAL_ENTITY_ID_PROPERTY_NAME))
            // index for the localEntityId must be created regardless the indexForEverySimpleProperty param
            indices.checkIndex(type, false, LOCAL_ENTITY_ID_PROPERTY_NAME)
            // the index for localEntityId must not be unique, otherwise it will not let the same localEntityId
            // for subtypes of a supertype
            assertTrue(indices.getValue(type).none { it.unique })

            val sequence = sequences.getSequence(localEntityIdSequenceName(type))
            assertNotNull(sequence)
            assertEquals(0, sequence.next())
        }

        // emulate the next run of the application
        oSession.applySchema(model)

        for (type in types) {
            val sequence = sequences.getSequence(localEntityIdSequenceName(type))
            // sequences are the same
            assertEquals(1, sequence.next())
        }
    }

    private fun OProperty.check(required: Boolean, notNull: Boolean) {
        assertEquals(required, isMandatory)
        assertEquals(notNull, isNotNull)
    }

    private val supportedSimplePropertyTypes: List<String> = listOf(
        "boolean",
        "string",
        "byte", "short", "int", "integer", "long",
        "float", "double",
        "datetime",
    )

    private fun getOType(jvmTypeName: String): OType {
        return when (jvmTypeName.lowercase()) {
            "boolean" -> OType.BOOLEAN
            "string" -> OType.STRING

            "byte" -> OType.BYTE
            "short" -> OType.SHORT
            "int",
            "integer" -> OType.INTEGER
            "long" -> OType.LONG

            "float" -> OType.FLOAT
            "double" -> OType.DOUBLE

            "datetime" -> OType.LONG

            else -> throw IllegalArgumentException("$jvmTypeName is not supported. Feel free to support it.")
        }
    }
}
