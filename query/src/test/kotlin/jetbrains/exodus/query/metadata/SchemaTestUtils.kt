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

import com.jetbrains.youtrack.db.api.DatabaseSession
import com.jetbrains.youtrack.db.api.record.Direction
import com.jetbrains.youtrack.db.api.record.Edge
import com.jetbrains.youtrack.db.api.record.Vertex
import com.jetbrains.youtrack.db.api.schema.SchemaProperty
import com.jetbrains.youtrack.db.api.schema.SchemaClass
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClassInternal
import jetbrains.exodus.entitystore.orientdb.*
import org.junit.Assert.*

// assertions

internal fun DatabaseSession.assertAssociationNotExist(
    outClassName: String,
    inClassName: String,
    edgeName: String,
    requireEdgeClass: Boolean = false
) {
    val edgeClassName = edgeName.asEdgeClass
    if (requireEdgeClass) {
        val edgeClass = requireEdgeClass(edgeClassName)
        assertTrue(edgeClass.areIndexed(this, Edge.DIRECTION_IN, Edge.DIRECTION_OUT))
    }

    val inClass = getClass(inClassName)!!
    val outClass = getClass(outClassName)!!

    val outPropName = Vertex.getEdgeLinkFieldName(Direction.OUT, edgeClassName)
    assertNull(outClass.getProperty(outPropName))

    val inPropName = Vertex.getEdgeLinkFieldName(Direction.IN, edgeClassName)
    assertNull(inClass.getProperty(inPropName))
}

internal fun DatabaseSession.assertAssociationExists(
    outClassName: String,
    inClassName: String,
    edgeName: String,
    cardinality: AssociationEndCardinality?,
) {
    val edgeClassName = edgeName.asEdgeClass
    val edgeClass = getClass(edgeClassName)
    val inClass = getClass(inClassName)!!
    val outClass = getClass(outClassName)!!

    assertTrue(edgeClass.areIndexed(this, Edge.DIRECTION_IN, Edge.DIRECTION_OUT))

    if (cardinality != null) {
        val outPropName = Vertex.getEdgeLinkFieldName(Direction.OUT, edgeClassName)
        val directOutProp = outClass.getProperty(outPropName)!!
        assertEquals(com.jetbrains.youtrack.db.api.schema.PropertyType.LINKBAG, directOutProp.type)
        directOutProp.assertCardinality(cardinality)

        val inPropName = Vertex.getEdgeLinkFieldName(Direction.IN, edgeClassName)
        val directInProp = inClass.getProperty(inPropName)!!
        assertEquals(com.jetbrains.youtrack.db.api.schema.PropertyType.LINKBAG, directInProp.type)
    }
}

private fun SchemaProperty.assertCardinality(cardinality: AssociationEndCardinality) {
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
    }
}

internal fun DatabaseSession.assertVertexClassExists(name: String) {
    assertHasSuperClass(name, "V")
}

internal fun DatabaseSession.requireEdgeClass(name: String): SchemaClass {
    val edge = getClass(name)!!
    assertTrue(edge.superClassesNames.contains("E"))
    return edge
}

internal fun DatabaseSession.assertHasSuperClass(className: String, superClassName: String) {
    assertTrue(getClass(className)!!.superClassesNames.contains(superClassName))
}

internal fun DatabaseSession.checkIndex(
    className: String,
    unique: Boolean,
    vararg fieldNames: String
) {
    val entity = getClass(className)!!
    val indexName = indexName(className, unique, *fieldNames)
    val index =
        (entity as SchemaClassInternal).getIndexesInternal(this).first { it.name == indexName }
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

internal fun EntityMetaDataImpl.property(
    name: String,
    typeName: String,
    required: Boolean = false
) {
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

internal fun DatabaseSession.createVertexAndSetLocalEntityId(className: String): Vertex {
    val v = newVertex(className)
    setLocalEntityId(className, v)
    v.save()
    return v
}

internal fun Vertex.setPropertyAndSave(propName: String, value: Any) {
    setProperty(propName, value)
    save()
}

internal fun Vertex.addEdge(linkName: String, target: Vertex) {
    val edgeClassName = OVertexEntity.edgeClassName(linkName)
    addEdge(target, edgeClassName)
    save()
    target.save()
}

internal fun Vertex.addIndexedEdge(linkName: String, target: Vertex) {
    val bag = getTargetLocalEntityIds(linkName)
    addEdge(target, OVertexEntity.edgeClassName(linkName))
    bag.add(target.identity)
    setTargetLocalEntityIds(linkName, bag)
    save()
    target.save()
}

internal fun Vertex.deleteIndexedEdge(linkName: String, target: Vertex) {
    val bag = getTargetLocalEntityIds(linkName)
    target.delete()
    bag.remove(target.identity)
    setTargetLocalEntityIds(linkName, bag)
    save()
    target.save()
}
