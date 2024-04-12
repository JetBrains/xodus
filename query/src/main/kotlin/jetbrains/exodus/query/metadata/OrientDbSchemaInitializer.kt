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

import com.orientechnologies.orient.core.collate.OCaseInsensitiveCollate
import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.metadata.schema.OProperty
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.metadata.sequence.OSequence
import com.orientechnologies.orient.core.record.ODirection
import com.orientechnologies.orient.core.record.OVertex
import jetbrains.exodus.entitystore.orientdb.OVertexEntity
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.CLASS_ID_CUSTOM_PROPERTY_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.CLASS_ID_SEQUENCE_NAME
import mu.KotlinLogging

private val log = KotlinLogging.logger {}

fun ODatabaseSession.applySchema(
    model: ModelMetaDataImpl,
    indexForEverySimpleProperty: Boolean = false,
    applyLinkCardinality: Boolean = true,
    backwardCompatibleEntityId: Boolean = false,
): Map<String, Set<DeferredIndex>> {
    val initializer = OrientDbSchemaInitializer(model, this, indexForEverySimpleProperty, applyLinkCardinality, backwardCompatibleEntityId)
    initializer.apply()
    return initializer.getIndices()
}

internal class OrientDbSchemaInitializer(
    private val dnqModel: ModelMetaDataImpl,
    private val oSession: ODatabaseSession,
    private val indexForEverySimpleProperty: Boolean,
    private val applyLinkCardinality: Boolean,
    private val backwardCompatibleEntityId: Boolean
) {
    private val paddedLogger = PaddedLogger(log)

    private fun withPadding(code: () -> Unit) = paddedLogger.withPadding(4, code)

    private fun append(s: String) = paddedLogger.append(s)

    private fun appendLine(s: String = "") = paddedLogger.appendLine(s)


    private val indices = HashMap<String, MutableSet<DeferredIndex>>()

    private fun addIndex(index: DeferredIndex) {
        indices.getOrPut(index.ownerVertexName) { HashSet() }.add(index)
    }

    private fun simplePropertyIndex(entityName: String, propertyName: String): DeferredIndex {
        val indexField = IndexFieldImpl()
        indexField.isProperty = true
        indexField.name = propertyName
        return DeferredIndex(entityName, listOf(indexField), unique = false)
    }

    fun getIndices(): Map<String, Set<DeferredIndex>> = indices

    fun apply() {
        try {
            if (backwardCompatibleEntityId) {
                createClassIdSequenceIfAbsent()
            }

            appendLine("applying the DNQ schema to OrientDB")
            val sortedEntities = dnqModel.entitiesMetaData.sortedTopologically()

            appendLine("creating classes if absent:")
            withPadding {
                /*
                * We want superclasses be created before subclasses.
                * So, process entities in the topological order.
                * */
                for (dnqEntity in sortedEntities) {
                    createVertexClassIfAbsent(dnqEntity)
                }
            }

            appendLine("creating simple properties if absent:")
            withPadding {
                /*
                * It is necessary to process entities in the topologically sorted order here too.
                *
                * Consider Superclass1 and Subclass1: Superclass1. All the properties of
                * Superclass1 will be both in EntityMetaData of Superclass1 and EntityMetaData of Subclass1.
                *
                * We want to those properties be created for Superclass1 in OrientDB, so we have to
                * process Superclass1 before Subclass1. That is why we have to process entities in
                * the topologically sorted order.
                * */
                for (dnqEntity in sortedEntities) {
                    createSimplePropertiesIfAbsent(dnqEntity)
                }
            }

            appendLine("creating associations if absent:")
            withPadding {
                for (dnqEntity in sortedEntities) {
                    appendLine(dnqEntity.type)
                    withPadding {
                        for (associationEnd in dnqEntity.associationEndsMetaData) {
                            applyAssociation(dnqEntity.type, associationEnd)
                        }
                    }
                }
            }

            // initialize enums and singletons

            appendLine("indices found:")
            withPadding {
                for ((indexOwner, indices) in indices) {
                    appendLine("$indexOwner:")
                    withPadding {
                        for (index in indices) {
                            appendLine(index.indexName)
                        }
                    }
                }
            }
        } finally {
            paddedLogger.flush()
        }
    }

    // ClassId

    private fun createClassIdSequenceIfAbsent() {
        val sequences = oSession.metadata.sequenceLibrary
        if (sequences.getSequence(CLASS_ID_SEQUENCE_NAME) == null) {
            val params = OSequence.CreateParams()
            params.start = 0L
            sequences.createSequence(CLASS_ID_SEQUENCE_NAME, OSequence.SEQUENCE_TYPE.ORDERED, params)
        }
    }

    private fun OClass.setClassIdIfAbsent() {
        if (getCustom(CLASS_ID_CUSTOM_PROPERTY_NAME) == null) {
            val sequences = oSession.metadata.sequenceLibrary
            val sequence: OSequence = sequences.getSequence(CLASS_ID_SEQUENCE_NAME) ?: throw IllegalStateException("$CLASS_ID_SEQUENCE_NAME not found")

            setCustom(CLASS_ID_CUSTOM_PROPERTY_NAME, sequence.next().toString())
        }
    }

    // Vertices and Edges

    private fun createVertexClassIfAbsent(dnqEntity: EntityMetaData) {
        append(dnqEntity.type)
        val oClass = oSession.createVertexClassIfAbsent(dnqEntity.type)
        oClass.applySuperClass(dnqEntity.superType)
        appendLine()

        if (backwardCompatibleEntityId) {
            oClass.setClassIdIfAbsent()
        }

        /*
        * It is more efficient to create indices after the data migration.
        * So, we only remember indices here and let the user create them later.
        * */
        for (index in dnqEntity.ownIndexes.map { DeferredIndex(it, unique = true)}) {
            index.requireAllFieldsAreSimpleProperty()
            addIndex(index)
        }

        /*
        * Interfaces
        *
        * On the one hand, interfaces are in use in the query logic, see jetbrains.exodus.query.Utils.isTypeOf(...).
        * On the other hand, interfaces are not initialized anywhere, so EntityMetaData.interfaceTypes are always empty.
        *
        * So here, we ignore interfaces and do not try to apply them anyhow to OrientDB schema.
        * */
    }

    private fun ODatabaseSession.createVertexClassIfAbsent(name: String): OClass {
        var oClass: OClass? = getClass(name)
        if (oClass == null) {
            oClass = oSession.createVertexClass(name)!!
            append(", created")
        } else {
            append(", already created")
        }
        return oClass
    }

    private fun ODatabaseSession.createEdgeClassIfAbsent(name: String): OClass {
        var oClass: OClass? = getClass(name)
        if (oClass == null) {
            oClass = oSession.createEdgeClass(name)!!
            append(", edge class created")
        } else {
            append(", edge class already created")
        }
        return oClass
    }

    private fun OClass.applySuperClass(superClassName: String?) {
        if (superClassName == null) {
            append(", no super type")
        } else {
            append(", super type is $superClassName")
            val superClass = oSession.getClass(superClassName)
            if (superClasses.contains(superClass)) {
                append(", already set")
            } else {
                addSuperClass(superClass)
                append(", set")
            }
        }
    }


    // Associations

    private fun applyAssociation(className: String, association: AssociationEndMetaData) {
        append(association.name)

        val class1 = oSession.getClass(className) ?: throw IllegalStateException("${association.oppositeEntityMetaData.type} class is not found")
        val class2 = oSession.getClass(association.oppositeEntityMetaData.type) ?: throw IllegalStateException("${association.oppositeEntityMetaData.type} class is not found")

        val edgeClass = oSession.createEdgeClassIfAbsent(association.name)
        appendLine()

        withPadding {
            // class1.prop1 -> edgeClass -> class2
            applyLink(
                edgeClass,
                outClass = class1,
                outCardinality = association.cardinality,
                inClass = class2,
            )
        }
    }

    private fun applyLink(
        edgeClass: OClass,
        outClass: OClass,
        outCardinality: AssociationEndCardinality,
        inClass: OClass,
    ) {
        val propOutName = OVertex.getDirectEdgeLinkFieldName(ODirection.OUT, edgeClass.name)
        append("${outClass.name}.$propOutName")
        val propOut = outClass.createEdgePropertyIfAbsent(propOutName, edgeClass)
        if (applyLinkCardinality) {
            propOut.applyCardinality(outCardinality)
        }
        appendLine()

        val propInName = OVertex.getDirectEdgeLinkFieldName(ODirection.IN, edgeClass.name)
        append("${inClass.name}.$propInName")
        inClass.createEdgePropertyIfAbsent(propInName, edgeClass)

        appendLine()
    }

    private fun OProperty.applyCardinality(cardinality: AssociationEndCardinality) {
        when (cardinality) {
            AssociationEndCardinality._0_1 -> {
                setRequirement(false)
                setMinIfDifferent("0")
                setMaxIfDifferent("1")
            }
            AssociationEndCardinality._1 -> {
                setRequirement(true)
                setMinIfDifferent("1")
                setMaxIfDifferent("1")
            }
            AssociationEndCardinality._0_n -> {
                setRequirement(false)
                setMinIfDifferent("0")
                setMaxIfDifferent(null)
            }
            AssociationEndCardinality._1_n -> {
                setRequirement(true)
                setMinIfDifferent("1")
                setMaxIfDifferent(null)
            }
        }
    }

    private fun OProperty.setMaxIfDifferent(max: String?) {
        append(", max $max")
        if (this.max == max) {
            append(" already set")
        } else {
            setMax(max)
            append(" set")
        }
    }

    private fun OProperty.setMinIfDifferent(min: String?) {
        append(", min $min")
        if (this.min == min) {
            append(" already set")
        } else {
            setMin(min)
            append(" set")
        }
    }


    // Simple properties

    private fun createSimplePropertiesIfAbsent(dnqEntity: EntityMetaData) {
        appendLine(dnqEntity.type)

        val oClass = oSession.getClass(dnqEntity.type)

        withPadding {
            for (propertyMetaData in dnqEntity.propertiesMetaData) {
                if (propertyMetaData is PropertyMetaDataImpl) {
                    val required = propertyMetaData.name in dnqEntity.requiredProperties
                    // Xodus does not let a property be null/empty if it is in an index
                    val requiredBecauseOfIndex = dnqEntity.ownIndexes.any { index -> index.fields.any { it.name == propertyMetaData.name } }
                    oClass.applySimpleProperty(propertyMetaData, required || requiredBecauseOfIndex)
                }
            }
        }
    }

    private fun OClass.applySimpleProperty(
        simpleProp: PropertyMetaDataImpl,
        required: Boolean
    ) {
        val propertyName = simpleProp.name
        append(propertyName)

        when (simpleProp.type) {
            PropertyType.PRIMITIVE -> {
                require(simpleProp is SimplePropertyMetaDataImpl) { "$propertyName is a primitive property but it is not an instance of SimplePropertyMetaDataImpl. Happy fixing!" }
                val primitiveTypeName = simpleProp.primitiveTypeName ?: throw IllegalArgumentException("primitiveTypeName is null")

                if (primitiveTypeName.lowercase() == "set") {
                    append(", is not supported yet")
                    /*
                    * To support sets we have to:
                    * 1. On the Xodus repo level
                    *   1. Add SimplePropertyMetaDataImpl.argumentType: String? property (or list of them, it is easier to extend)
                    * 2. On the XodusDNQ repo level
                    *   1. DNQMetaDataUtil.kt, addEntityMetaData(), 119 line, fill that argumentType param
                    * 3. Support here
                    * */
                    val typeParameter = simpleProp.typeParameterNames?.firstOrNull() ?: throw IllegalStateException("$propertyName is Set but does not contain information about the type parameter")
                    val oProperty = createEmbeddedSetPropertyIfAbsent(propertyName, getOType(typeParameter))

                    /*
                    * If the value is not defined, the property returns true.
                    * It is handled on the DNQ entities level.
                    * But, we still apply the required state just in case.
                    * */
                    oProperty.setRequirement(required)

                    /*
                    * When creating an index on an EMBEDDEDSET field, OrientDB does not create an index for the field itself.
                    * Instead, it creates an index for each individual item in the set.
                    * This is done to enable quick searches for individual elements within the set.
                    *
                    * The same behaviour as the original behaviour of set properties in DNQ.
                    * */
                    val index = makeDeferredIndexForEmbeddedSet(propertyName)
                    addIndex(index)
                } else { // primitive types
                    val oProperty = createPropertyIfAbsent(propertyName, getOType(primitiveTypeName))
                    oProperty.setRequirement(required)
                    if (indexForEverySimpleProperty) {
                        addIndex(simplePropertyIndex(name, propertyName))
                    }
                }

                /*
                * Default values
                *
                * Default values are implemented in DNQ as lambda functions that require
                * the entity itself and an instance of a KProperty to be called.
                *
                * So, it is not as straightforward as one may want to extract the default value out
                * of this lambda.
                *
                * So, a hard decision was made in this regard - ignore the default values on the
                * schema mapping step and handle them on the query processing level.
                *
                * Feel free to support default values in Schema mapping if you want to.
                * */

                /*
                * Constraints
                *
                * There are some typed constraints, and that is good.
                * But there are some anonymous constraints, and that is not good.
                * Most probably, there are constraints we do not know any idea of existing
                * (users can define their own constraints without any restrictions), and that is bad.
                *
                * Despite being able to map SOME constraints to the schema, there still will be
                * constraints we can not map (anonymous or user-defined).
                *
                * So, checking constraints on the query level is required.
                *
                * So, we made one of the hardest decisions in our lives and decided not to map
                * any of them at the schema mapping level.
                *
                * Feel free to do anything you want in this regard.
                * */
            }
            PropertyType.TEXT -> {
                val oProperty = createStringBlobPropertyIfAbsent(propertyName)
                oProperty.setRequirement(required)
            }
            PropertyType.BLOB -> {
                val oProperty = createBinaryBlobPropertyIfAbsent(propertyName)
                oProperty.setRequirement(required)
            }
        }
        appendLine()
    }

    private fun OClass.createBinaryBlobPropertyIfAbsent(propertyName: String): OProperty = createBlobPropertyIfAbsent(propertyName, OVertexEntity.BINARY_BLOB_CLASS_NAME)

    private fun OClass.createStringBlobPropertyIfAbsent(propertyName: String): OProperty = createBlobPropertyIfAbsent(propertyName, OVertexEntity.STRING_BLOB_CLASS_NAME)

    private fun OClass.createBlobPropertyIfAbsent(propertyName: String, blobClassName: String): OProperty {
        val blobClass = oSession.createBlobClassIfAbsent(blobClassName)

        val oProperty = createPropertyIfAbsent(propertyName, OType.LINK)
        if (oProperty.linkedClass != blobClass) {
            oProperty.setLinkedClass(blobClass)
        }
        require(oProperty.linkedClass == blobClass) { "Property linked class is ${oProperty.linkedClass}, but $blobClass was expected" }
        return oProperty
    }

    private fun ODatabaseSession.createBlobClassIfAbsent(className: String): OClass {
        var oClass: OClass? = getClass(className)
        if (oClass == null) {
            oClass = oSession.createVertexClass(className)!!
            append(", $className class created")
            oClass.createProperty(OVertexEntity.DATA_PROPERTY_NAME, OType.BINARY)
            append(", ${OVertexEntity.DATA_PROPERTY_NAME} property created")
        } else {
            append(", $className class already created")
            require(oClass.existsProperty(OVertexEntity.DATA_PROPERTY_NAME)) { "${OVertexEntity.DATA_PROPERTY_NAME} is missing in $className, something went dramatically wrong. Happy debugging!" }
        }
        return oClass
    }

    private fun OProperty.setRequirement(required: Boolean) {
        if (required) {
            append(", required")
            if (!isMandatory) {
                isMandatory = true
            }
            setNotNullIfDifferent(true)
        } else {
            append(", optional")
            if (isMandatory) {
                isMandatory = false
            }
        }
    }

    private fun OProperty.setNotNullIfDifferent(notNull: Boolean) {
        if (notNull) {
            append(", not nullable")
            if (!isNotNull) {
                setNotNull(true)
            }
        } else {
            append(", nullable")
            if (isNotNull) {
                setNotNull(false)
            }
        }
    }

    private fun OClass.createPropertyIfAbsent(propertyName: String, oType: OType): OProperty {
        append(", type is $oType")
        val oProperty = if (existsProperty(propertyName)) {
            append(", already created")
            getProperty(propertyName)
        } else {
            append(", created")
            createProperty(propertyName, oType)
        }
        if (oType == OType.STRING) {
            if (oProperty.collate.name == OCaseInsensitiveCollate.NAME) {
                append(", case-insensitive collate already set")
            } else {
                oProperty.setCollate(OCaseInsensitiveCollate.NAME)
                append(", set case-insensitive collate")
            }
        }
        require(oProperty.type == oType) { "$propertyName type is ${oProperty.type} but $oType was expected instead. Types migration is not supported."  }
        return oProperty
    }

    private fun OClass.createEdgePropertyIfAbsent(propertyName: String, edgeClass: OClass): OProperty {
        append(", edge-class is ${edgeClass.name}")
        val oProperty = if (existsProperty(propertyName)) {
            append(", already created")
            getProperty(propertyName)
        } else {
            append(", created")
            createProperty(propertyName, OType.LINKBAG, edgeClass)
        }
        require(oProperty.type == OType.LINKBAG) { "$propertyName type is ${oProperty.type} but ${OType.LINKBAG} was expected instead. Types migration is not supported."  }
        require(oProperty.linkedClass == edgeClass) { "$propertyName type of the set is ${oProperty.linkedClass.name} but ${edgeClass.name} was expected instead. Types migration is not supported." }
        return oProperty
    }

    private fun OClass.createEmbeddedSetPropertyIfAbsent(propertyName: String, oType: OType): OProperty {
        append(", type of the set is $oType")
        val oProperty = if (existsProperty(propertyName)) {
            append(", already created")
            getProperty(propertyName)
        } else {
            append(", created")
            createProperty(propertyName, OType.EMBEDDEDSET, oType)
        }
        if (oType == OType.STRING) {
            if (oProperty.collate.name == OCaseInsensitiveCollate.NAME) {
                append(", case-insensitive collate already set")
            } else {
                oProperty.setCollate(OCaseInsensitiveCollate.NAME)
                append(", set case-insensitive collate")
            }
        }
        require(oProperty.type == OType.EMBEDDEDSET) { "$propertyName type is ${oProperty.type} but ${OType.EMBEDDEDSET} was expected instead. Types migration is not supported."  }
        require(oProperty.linkedType == oType) { "$propertyName type of the set is ${oProperty.linkedType} but $oType was expected instead. Types migration is not supported." }
        return oProperty
    }

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