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
import jetbrains.exodus.entitystore.orientdb.*
import org.junit.Assert.*

// assertions

internal fun ODatabaseSession.assertAssociationNotExist(
    outClassName: String,
    inClassName: String,
    edgeName: String,
    requireEdgeClass: Boolean = false
) {
    if (requireEdgeClass) {
        requireEdgeClass(edgeName.asEdgeClass)
    }

    val inClass = getClass(inClassName)!!
    val outClass = getClass(outClassName)!!

    val outPropName = OVertex.getEdgeLinkFieldName(ODirection.OUT, edgeName)
    assertNull(outClass.getProperty(outPropName))

    val inPropName = OVertex.getEdgeLinkFieldName(ODirection.IN, edgeName)
    assertNull(inClass.getProperty(inPropName))
}

internal fun ODatabaseSession.assertAssociationExists(
    outClassName: String,
    inClassName: String,
    edgeName: String,
    cardinality: AssociationEndCardinality?
) {
    val edgeClass = edgeName.asEdgeClass
    val inClass = getClass(inClassName)!!
    val outClass = getClass(outClassName)!!

    val outPropName = OVertex.getEdgeLinkFieldName(ODirection.OUT, edgeClass)
    val directOutProp = outClass.getProperty(outPropName)!!
    assertEquals(OType.LINKBAG, directOutProp.type)
    directOutProp.assertCardinality(cardinality)

    val inPropName = OVertex.getEdgeLinkFieldName(ODirection.IN, edgeClass)
    val directInProp = inClass.getProperty(inPropName)!!
    assertEquals(OType.LINKBAG, directInProp.type)
}

private fun OProperty.assertCardinality(cardinality: AssociationEndCardinality?) {
    when (cardinality) {
        AssociationEndCardinality._0_1 -> {
            assertTrue(!this.isMandatory)
            assertTrue(this.min == "0")
            assertTrue(this.max == "1")
        }

        AssociationEndCardinality._1 -> {
            assertTrue(this.isMandatory)
            assertTrue(this.min == "1")
            assertTrue(this.max == "1")
        }

        AssociationEndCardinality._0_n -> {
            assertTrue(!this.isMandatory)
            assertTrue(this.min == "0")
            assertTrue(this.max == null)
        }

        AssociationEndCardinality._1_n -> {
            assertTrue(this.isMandatory)
            assertTrue(this.min == "1")
            assertTrue(this.max == null)
        }

        null -> {
            assertTrue(!this.isMandatory)
            assertTrue(this.min == null)
            assertTrue(this.max == null)
        }
    }
}

internal fun ODatabaseSession.assertVertexClassExists(name: String) {
    assertHasSuperClass(name, "V")
}

internal fun ODatabaseSession.requireEdgeClass(name: String): OClass {
    val edge = getClass(name)!!
    assertTrue(edge.superClassesNames.contains("E"))
    return edge
}

internal fun ODatabaseSession.assertHasSuperClass(className: String, superClassName: String) {
    assertTrue(getClass(className)!!.superClassesNames.contains(superClassName))
}

internal fun ODatabaseSession.checkIndex(className: String, unique: Boolean, vararg fieldNames: String) {
    val entity = getClass(className)!!
    val indexName = indexName(className, unique, *fieldNames)
    val index = entity.indexes.first { it.name == indexName }
    assertEquals(unique, index.isUnique)

    assertEquals(fieldNames.size, index.definition.fields.size)
    for (fieldName in fieldNames) {
        assertTrue(index.definition.fields.contains(fieldName))
    }
}

internal fun Map<String, Set<DeferredIndex>>.checkIndex(
    entityName: String,
    unique: Boolean,
    vararg fieldNames: String
) {
    val indexName = indexName(entityName, unique, *fieldNames)
    val indices = getValue(entityName)
    val index = indices.first { it.indexName == indexName }

    assertEquals(unique, index.unique)
    assertEquals(entityName, index.ownerVertexName)
    assertEquals(fieldNames.size, index.properties.size)

    for (fieldName in fieldNames) {
        assertTrue(index.properties.any { it == fieldName })
    }
}

internal fun indexName(entityName: String, unique: Boolean, vararg fieldNames: String): String =
    "${entityName}_${fieldNames.joinToString("_")}${if (unique) "_unique" else ""}"


// Model

internal fun model(initialize: ModelMetaDataImpl.() -> Unit): ModelMetaDataImpl {
    val model = ModelMetaDataImpl()
    model.initialize()
    return model
}

internal fun oModel(
    databaseProvider: ODatabaseProvider,
    schemaBuddy: OSchemaBuddy = OSchemaBuddyImpl(databaseProvider, autoInitialize = false),
    buildModel: ModelMetaDataImpl.() -> Unit
): OModelMetaData {
    val model = OModelMetaData(databaseProvider, schemaBuddy)
    model.buildModel()
    return model
}

internal fun ModelMetaDataImpl.entity(
    type: String,
    superType: String? = null,
    init: EntityMetaDataImpl.() -> Unit = {}
) {
    val entity = EntityMetaDataImpl()
    entity.type = type
    entity.superType = superType
    addEntityMetaData(entity)
    entity.init()
}

internal fun EntityMetaDataImpl.index(vararg fieldNames: String) {
    index(*fieldNames.map { IndexedField(it, true) }.toTypedArray())
}

data class IndexedField(val name: String, val isProperty: Boolean)

internal fun EntityMetaDataImpl.index(vararg fields: IndexedField) {
    val index = IndexImpl()
    index.fields = fields.map { (fieldName, isProperty) ->
        val field = IndexFieldImpl()
        field.isProperty = isProperty
        field.name = fieldName
        field
    }
    index.ownerEntityType = this.type
    this.ownIndexes = this.ownIndexes + setOf(index)
}

internal fun EntityMetaDataImpl.property(name: String, typeName: String, required: Boolean = false) {
    // regardless of the name, this setter actually ADDS new properties to its internal collection
    this.propertiesMetaData = listOf(SimplePropertyMetaDataImpl(name, typeName))
    if (required) {
        requiredProperties = requiredProperties + setOf(name)
    }
}

internal fun EntityMetaDataImpl.blobProperty(name: String) {
    this.propertiesMetaData = listOf(PropertyMetaDataImpl(name, PropertyType.BLOB))
}

internal fun EntityMetaDataImpl.stringBlobProperty(name: String) {
    this.propertiesMetaData = listOf(PropertyMetaDataImpl(name, PropertyType.TEXT))
}

internal fun EntityMetaDataImpl.setProperty(name: String, dataType: String) {
    this.propertiesMetaData = listOf(SimplePropertyMetaDataImpl(name, "Set", listOf(dataType)))
}

internal fun ModelMetaData.association(
    sourceEntity: String,
    associationName: String,
    targetEntity: String,
    cardinality: AssociationEndCardinality
) {
    addAssociation(
        sourceEntity,
        targetEntity,
        AssociationType.Directed, // ingored
        associationName,
        cardinality,
        false, false, false, false, // ignored
        null, null, false, false, false, false
    )
}

internal fun ModelMetaData.twoDirectionalAssociation(
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
        false, false, false, false, // ignored
        targetName,
        targetCardinality,
        false, false, false, false // ignored
    )
}

internal fun ODatabaseSession.createVertexAndSetLocalEntityId(className: String): OVertex {
    val v = newVertex(className)
    setLocalEntityId(className, v)
    return v
}

internal fun OVertex.addIndexedEdge(linkName: String, target: OVertex) {
    val edgeClassName = OVertexEntity.edgeClassName(linkName)

    val bag = getTargetLocalEntityIds(linkName)
    addEdge(target, edgeClassName)
    bag.add(target)
    setTargetLocalEntityIds(linkName, bag)
}

internal fun OVertex.deleteIndexedEdge(linkName: String, target: OVertex) {
    val edgeClassName = OVertexEntity.edgeClassName(linkName)

    val bag = getTargetLocalEntityIds(linkName)
    deleteEdge(target, edgeClassName)
    bag.remove(target)
    setTargetLocalEntityIds(linkName, bag)
}
