package jetbrains.exodus.query.metadata

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.ODirection
import com.orientechnologies.orient.core.record.OVertex
import jetbrains.exodus.entitystore.orientdb.ODatabaseProvider
import org.junit.Assert.*

// assertions

internal fun ODatabaseSession.assertAssociationNotExist(
    outClassName: String,
    inClassName: String,
    edgeName: String,
    requireEdgeClass: Boolean = false
) {
    if (requireEdgeClass) {
        requireEdgeClass(edgeName)
    }

    val inClass = getClass(inClassName)!!
    val outClass = getClass(outClassName)!!

    val outPropName = OVertex.getDirectEdgeLinkFieldName(ODirection.OUT, edgeName)
    assertNull(outClass.getProperty(outPropName))

    val inPropName = OVertex.getDirectEdgeLinkFieldName(ODirection.IN, edgeName)
    assertNull(inClass.getProperty(inPropName))
}

internal fun ODatabaseSession.assertAssociationExists(
    outClassName: String,
    inClassName: String,
    edgeName: String,
    cardinality: AssociationEndCardinality?
) {
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


// Model


internal fun model(initialize: ModelMetaDataImpl.() -> Unit): ModelMetaDataImpl {
    val model = ModelMetaDataImpl()
    model.initialize()
    return model
}

internal fun oModel(databaseProvider: ODatabaseProvider, initialize: ModelMetaDataImpl.() -> Unit): OModelMetaData {
    val model = OModelMetaData(databaseProvider)
    model.initialize()
    return model
}

internal fun ModelMetaDataImpl.entity(type: String, superType: String? = null, init: EntityMetaDataImpl.() -> Unit = {}) {
    val entity = EntityMetaDataImpl()
    entity.type = type
    entity.superType = superType
    addEntityMetaData(entity)
    entity.init()
}

internal fun EntityMetaDataImpl.index(vararg fieldNames: String) {
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

internal fun EntityMetaDataImpl.property(name: String, typeName: String, required: Boolean = false) {
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

internal fun EntityMetaDataImpl.association(associationName: String, targetEntity: String, cardinality: AssociationEndCardinality) {
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
        false, false, false ,false, // ignored
        targetName,
        targetCardinality,
        false, false, false, false // ignored
    )
}