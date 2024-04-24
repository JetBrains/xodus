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

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.metadata.schema.OProperty
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.ODirection
import com.orientechnologies.orient.core.record.OVertex
import jetbrains.exodus.entitystore.orientdb.OVertexEntity
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.LOCAL_ENTITY_ID_PROPERTY_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.localEntityIdSequenceName
import jetbrains.exodus.entitystore.orientdb.requireClassId
import jetbrains.exodus.entitystore.orientdb.testutil.InMemoryOrientDB
import org.junit.Assert.*
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
                oSession.getClass("type1").checkIndex(unique = false, "setProp$type")
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
            entity("type2") {
                for (cardinality in AssociationEndCardinality.entries) {
                    association("prop1$cardinality", "type1", cardinality)
                }
            }
        }

        oSession.applySchema(model)

        for (cardinality in AssociationEndCardinality.entries) {
            oSession.checkAssociation("prop1$cardinality", "type2", "type1", cardinality)
        }
    }

    @Test
    fun `one-directional associations ignore cardinality`(): Unit = orientDb.withSession { oSession ->
        val model = model {
            entity("type1")
            entity("type2") {
                for (cardinality in AssociationEndCardinality.entries) {
                    association("prop1$cardinality", "type1", cardinality)
                }
            }
        }

        oSession.applySchema(model, applyLinkCardinality = false)

        for (cardinality in AssociationEndCardinality.entries) {
            oSession.checkAssociation("prop1$cardinality", "type2", "type1", null)
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
                oSession.checkAssociation("prop1${cardinality1}_${cardinality2}", "type1", "type2", cardinality1)
                oSession.checkAssociation("prop2${cardinality2}_${cardinality1}", "type2", "type1", cardinality2)
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

            val entity = oSession.getClass("type1")!!
            entity.checkIndex(true, "prop1", "prop2")
            entity.checkIndex(true, "prop3")
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
            session.checkAssociation("ass1${cardinality.name}", "type1", "type2", cardinality)
        }

        for (cardinality in AssociationEndCardinality.entries) {
            session.removeAssociation(
                sourceClassName = "type1",
                targetClassName = "type2",
                associationName = "ass1${cardinality.name}"
            )
        }

        for (cardinality in AssociationEndCardinality.entries) {
            session.checkNoAssociation("ass1${cardinality.name}", "type1", "type2")
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

    private fun OClass.checkIndex(unique: Boolean, vararg fieldNames: String) {
        val indexName = indexName(name, unique, *fieldNames)
        val index = indexes.first { it.name == indexName }
        assertEquals(unique, index.isUnique)

        assertEquals(fieldNames.size, index.definition.fields.size)
        for (fieldName in fieldNames) {
            assertTrue(index.definition.fields.contains(fieldName))
        }
    }

    private fun OProperty.check(required: Boolean, notNull: Boolean) {
        assertEquals(required, isMandatory)
        assertEquals(notNull, isNotNull)
    }

    private fun Map<String, Set<DeferredIndex>>.checkIndex(entityName: String, unique: Boolean, vararg fieldNames: String) {
        val indexName = indexName(entityName, unique, *fieldNames)
        val indices = getValue(entityName)
        val index = indices.first { it.indexName == indexName }

        assertEquals(unique, index.unique)
        assertEquals(entityName, index.ownerVertexName)
        assertEquals(fieldNames.size, index.properties.size)
        assertTrue(index.allFieldsAreSimpleProperty)

        for (fieldName in fieldNames) {
            assertTrue(index.properties.any { it.name == fieldName })
        }
    }

    private fun indexName(entityName: String, unique: Boolean, vararg fieldNames: String): String = "${entityName}_${fieldNames.joinToString("_")}${if (unique) "_unique" else ""}"

    private fun ODatabaseSession.checkNoAssociation(edgeName: String, outClassName: String, inClassName: String) {
        /*
        * We do not delete the edge class when deleting an association because
        * it (the edge class) may be used by other associations.
        *
        * Maybe it is possible to check an edge class if it is not used anywhere, but
        * we do not do it at the moment. Maybe some day in the future.
        * */
        requireEdgeClass(edgeName)

        val inClass = getClass(inClassName)!!
        val outClass = getClass(outClassName)!!

        val outPropName = OVertex.getDirectEdgeLinkFieldName(ODirection.OUT, edgeName)
        assertNull(outClass.getProperty(outPropName))

        val inPropName = OVertex.getDirectEdgeLinkFieldName(ODirection.IN, edgeName)
        assertNull(inClass.getProperty(inPropName))
    }

    private fun ODatabaseSession.checkAssociation(edgeName: String, outClassName: String, inClassName: String, cardinality: AssociationEndCardinality?) {
        val edge = requireEdgeClass(edgeName)
        val inClass = getClass(inClassName)!!
        val outClass = getClass(outClassName)!!

        val outPropName = OVertex.getDirectEdgeLinkFieldName(ODirection.OUT, edgeName)
        val outProp = outClass.getProperty(outPropName)!!
        assertEquals(OType.LINKBAG, outProp.type)
        assertEquals(edge, outProp.linkedClass)
        when (cardinality) {
            AssociationEndCardinality._0_1 -> {
                assertTrue(!outProp.isMandatory)
                assertTrue(outProp.min == "0")
                assertTrue(outProp.max == "1")
            }
            AssociationEndCardinality._1 -> {
                assertTrue(outProp.isMandatory)
                assertTrue(outProp.min == "1")
                assertTrue(outProp.max == "1")
            }
            AssociationEndCardinality._0_n -> {
                assertTrue(!outProp.isMandatory)
                assertTrue(outProp.min == "0")
                assertTrue(outProp.max == null)
            }
            AssociationEndCardinality._1_n -> {
                assertTrue(outProp.isMandatory)
                assertTrue(outProp.min == "1")
                assertTrue(outProp.max == null)
            }
            null -> {
                assertTrue(!outProp.isMandatory)
                assertTrue(outProp.min == null)
                assertTrue(outProp.max == null)
            }
        }

        val inPropName = OVertex.getDirectEdgeLinkFieldName(ODirection.IN, edgeName)
        val inProp = inClass.getProperty(inPropName)!!
        assertEquals(OType.LINKBAG, inProp.type)
        assertEquals(edge, inProp.linkedClass)
    }

    private fun ODatabaseSession.assertVertexClassExists(name: String) {
        assertHasSuperClass(name, "V")
    }

    private fun ODatabaseSession.requireEdgeClass(name: String): OClass {
        val edge = getClass(name)!!
        assertTrue(edge.superClassesNames.contains("E"))
        return edge
    }

    private fun ODatabaseSession.assertHasSuperClass(className: String, superClassName: String) {
        assertTrue(getClass(className)!!.superClassesNames.contains(superClassName))
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
        addEntityMetaData(entity)
        entity.init()
    }

    private fun EntityMetaDataImpl.index(vararg fieldNames: String) {
        val index = IndexImpl()
        index.fields = fieldNames.map { fieldName ->
            val field = IndexFieldImpl()
            field.isProperty = true
            field.name = fieldName
            field
        }
        index.ownerEntityType = this.type
        this.ownIndexes = this.ownIndexes + setOf(index)
    }

    private fun EntityMetaDataImpl.property(name: String, typeName: String, required: Boolean = false) {
        this.propertiesMetaData = listOf(SimplePropertyMetaDataImpl(name, typeName))
        if (required) {
            requiredProperties = requiredProperties + setOf(name)
        }
    }

    private fun EntityMetaDataImpl.blobProperty(name: String) {
        this.propertiesMetaData = listOf(PropertyMetaDataImpl(name, PropertyType.BLOB))
    }

    private fun EntityMetaDataImpl.stringBlobProperty(name: String) {
        this.propertiesMetaData = listOf(PropertyMetaDataImpl(name, PropertyType.TEXT))
    }

    private fun EntityMetaDataImpl.setProperty(name: String, dataType: String) {
        this.propertiesMetaData = listOf(SimplePropertyMetaDataImpl(name, "Set", listOf(dataType)))
    }

    private fun EntityMetaDataImpl.association(associationName: String, targetEntity: String, cardinality: AssociationEndCardinality) {
        modelMetaData.addAssociation(
            this.type,
            targetEntity,
            AssociationType.Directed, // ingored
            associationName,
            cardinality,
            false, false, false ,false, // ignored
            null, null, false, false, false, false
        )
    }

    private fun ModelMetaData.twoDirectionalAssociation(
        sourceEntity: String,
        sourceName: String,
        sourceCardinality: AssociationEndCardinality,
        targetEntity: String,
        targetName: String,
        targetCardinality: AssociationEndCardinality,
    ) {
        addAssociation(
            sourceEntity,
            targetEntity,
            AssociationType.Undirected, // two-directional
            sourceName,
            sourceCardinality,
            false, false, false ,false, // ignored
            targetName,
            targetCardinality,
            false, false, false, false // ignored
        )
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
