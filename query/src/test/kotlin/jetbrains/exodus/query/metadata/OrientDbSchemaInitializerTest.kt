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

import com.orientechnologies.orient.core.metadata.schema.OProperty
import com.orientechnologies.orient.core.metadata.schema.OType
import jetbrains.exodus.entitystore.orientdb.OVertexEntity
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.LOCAL_ENTITY_ID_PROPERTY_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.localEntityIdSequenceName
import jetbrains.exodus.entitystore.orientdb.requireClassId
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
    fun `create vertex-class for every entity`() = orientDb.withSession { oSession ->
        val model = model {
            entity("type1")
            entity("type2")
        }

        oSession.applySchema(model)

        oSession.assertVertexClassExists("type1")
        oSession.assertVertexClassExists("type2")
    }

    @Test
    fun `set super-classes`() = orientDb.withSession { oSession ->
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
    fun `simple properties of known types are created`() = orientDb.withSession { oSession ->
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
    fun `simple properties of not-known types cause exception`(): Unit = orientDb.withSession { oSession ->
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
    fun `create blob and string-blob properties`(): Unit = orientDb.withSession { oSession ->
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

        val strBlobClass = oSession.getClass(OVertexEntity.STRING_BLOB_CLASS_NAME)!!
        val strBlobDataProp = strBlobClass.getProperty(OVertexEntity.DATA_PROPERTY_NAME)!!
        assertEquals(OType.BINARY, strBlobDataProp.type)

        val entity = oSession.getClass("type1")

        val blobProp = entity.getProperty("blob1")!!
        assertEquals(OType.LINK, blobProp.type)
        assertEquals(OVertexEntity.BINARY_BLOB_CLASS_NAME, blobProp.linkedClass!!.name)

        val strBlobProp = entity.getProperty("strBlob1")!!
        assertEquals(OType.LINK, strBlobProp.type)
        assertEquals(OVertexEntity.STRING_BLOB_CLASS_NAME, strBlobProp.linkedClass!!.name)
    }

    @Test
    fun `embedded set properties with supported types`() {
        val indices = orientDb.withSession { oSession ->
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

        orientDb.withSession { oSession ->
            oSession.applyIndices(indices)

            for (type in supportedSimplePropertyTypes) {
                oSession.checkIndex("type1", unique = false, "setProp$type")
            }
        }
    }

    @Test
    fun `embedded set properties with not-supported types cause exception`(): Unit = orientDb.withSession { oSession ->
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
    fun `one-directional associations`(): Unit = orientDb.withSession { oSession ->
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
    fun `two association with the same name to a single type`(): Unit = orientDb.withSession { oSession ->
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
    fun `one-directional associations ignore cardinality`(): Unit = orientDb.withSession { oSession ->
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
    fun `two-directional associations`(): Unit = orientDb.withSession { oSession ->
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
    fun `own indices`() {
        val indices = orientDb.withSession { oSession ->
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

        orientDb.withSession { oSession ->
            oSession.applyIndices(indices)

            oSession.checkIndex("type1", true, "prop1", "prop2")
            oSession.checkIndex("type1", true, "prop3")
        }
    }

    @Test
    fun `index for every simple property if required`() = orientDb.withSession { oSession ->
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
    fun `no indices for simple properties by default`() = orientDb.withSession { oSession ->
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
    fun `addAssociation, removeAssociation`(): Unit = orientDb.withSession { session ->
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
    fun `classId is a monotonically increasing long`(): Unit = orientDb.withSession { oSession ->
        val types = mutableListOf("type1", "type2", "type3")
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
        assertEquals(setOf(1, 2, 3), classIds)


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
        assertEquals(setOf(1, 2, 3, 4, 5), classIds)
    }

    @Test
    fun `every class gets localEntityId property`(): Unit = orientDb.withSession { oSession ->
        val types = mutableListOf("type1", "type2", "type3")
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
            assertEquals(1, sequence.next())
        }

        // emulate the next run of the application
        oSession.applySchema(model)

        for (type in types) {
            val sequence = sequences.getSequence(localEntityIdSequenceName(type))
            // sequences are the same
            assertEquals(2, sequence.next())
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

            "datetime" -> OType.DATETIME

            else -> throw IllegalArgumentException("$jvmTypeName is not supported. Feel free to support it.")
        }
    }
}