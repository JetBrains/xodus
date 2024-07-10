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
import com.orientechnologies.orient.core.record.ODirection
import com.orientechnologies.orient.core.record.OEdge
import com.orientechnologies.orient.core.record.OVertex
import jetbrains.exodus.entitystore.orientdb.OVertexEntity
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.LOCAL_ENTITY_ID_PROPERTY_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.linkTargetEntityIdPropertyName
import jetbrains.exodus.entitystore.orientdb.createClassIdSequenceIfAbsent
import jetbrains.exodus.entitystore.orientdb.createLocalEntityIdSequenceIfAbsent
import jetbrains.exodus.entitystore.orientdb.setClassIdIfAbsent
import mu.KotlinLogging

private val log = KotlinLogging.logger {}

fun ODatabaseSession.applySchema(
    metaData: ModelMetaData,
    indexForEverySimpleProperty: Boolean = false,
    applyLinkCardinality: Boolean = true
): Map<String, Set<DeferredIndex>> =
    applySchema(metaData.entitiesMetaData, indexForEverySimpleProperty, applyLinkCardinality)

fun ODatabaseSession.applySchema(
    entitiesMetaData: Iterable<EntityMetaData>,
    indexForEverySimpleProperty: Boolean = false,
    applyLinkCardinality: Boolean = true
): Map<String, Set<DeferredIndex>> {
    val initializer =
        OrientDbSchemaInitializer(entitiesMetaData, this, indexForEverySimpleProperty, applyLinkCardinality)
    initializer.apply()
    return initializer.getIndices()
}

fun ODatabaseSession.addAssociation(
    className: String,
    association: AssociationEndMetaData,
    applyLinkCardinality: Boolean = true
) {
    addAssociation(association.toOMetadata(className), applyLinkCardinality)
}

fun ODatabaseSession.addAssociation(
    association: OAssociationMetadata,
    applyLinkCardinality: Boolean = true
) {
    val initializer = OrientDbSchemaInitializer(
        listOf(),
        this,
        indexForEverySimpleProperty = false,
        applyLinkCardinality = applyLinkCardinality
    )
    initializer.addAssociation(association)
}

fun ODatabaseSession.removeAssociation(
    sourceClassName: String,
    targetClassName: String,
    associationName: String
) {
    removeAssociation(
        OAssociationMetadata(
            name = associationName,
            outClassName = sourceClassName,
            inClassName = targetClassName,
            // it is ignored
            cardinality = AssociationEndCardinality._1
        )
    )
}

fun ODatabaseSession.removeAssociation(
    association: OAssociationMetadata
) {
    val initializer =
        OrientDbSchemaInitializer(listOf(), this, indexForEverySimpleProperty = false, applyLinkCardinality = false)
    initializer.removeAssociation(association)
}

data class OAssociationMetadata(
    val name: String,
    val outClassName: String,
    val inClassName: String,
    val cardinality: AssociationEndCardinality
)

fun AssociationEndMetaData.toOMetadata(outClassName: String): OAssociationMetadata = OAssociationMetadata(
    name = name,
    outClassName = outClassName,
    inClassName = oppositeEntityMetaData.type,
    cardinality = cardinality
)

internal class OrientDbSchemaInitializer(
    private val entitiesMetaData: Iterable<EntityMetaData>,
    private val oSession: ODatabaseSession,
    private val indexForEverySimpleProperty: Boolean,
    private val applyLinkCardinality: Boolean
) {
    private val paddedLogger = PaddedLogger(log)

    private fun withPadding(code: () -> Unit) = paddedLogger.withPadding(4, code)

    private fun append(s: String) = paddedLogger.append(s)

    private fun appendLine(s: String = "") = paddedLogger.appendLine(s)


    private val indices = HashMap<String, MutableMap<String, DeferredIndex>>()
    private val linksInUniqueIndicesByClassName = HashMap<String, Set<String>>()

    private fun addIndex(index: DeferredIndex) {
        indices.getOrPut(index.ownerVertexName) { HashMap() }[index.indexName] = index
    }

    private fun simplePropertyIndex(entityName: String, propertyName: String): DeferredIndex {
        return DeferredIndex(entityName, listOf(propertyName), unique = false)
    }

    private fun linkUniqueIndex(edgeClassName: String): DeferredIndex {
        return DeferredIndex(edgeClassName, listOf(OEdge.DIRECTION_IN, OEdge.DIRECTION_OUT), unique = true)
    }

    fun getIndices(): Map<String, Set<DeferredIndex>> = indices.map { it.key to it.value.values.toSet() }.toMap()

    fun apply() {
        try {
            oSession.createClassIdSequenceIfAbsent()

            appendLine("applying the DNQ schema to OrientDB")
            val sortedEntities = entitiesMetaData.sortedTopologically()

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
                            this.applyAssociation(associationEnd.toOMetadata(dnqEntity.type))
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
                        for ((indexName, _) in indices) {
                            appendLine(indexName)
                        }
                    }
                }
            }
        } finally {
            paddedLogger.flush()
        }
    }

    fun addAssociation(association: OAssociationMetadata) {
        try {
            appendLine("create association [${association.outClassName} -> ${association.name} -> ${association.inClassName}] if absent:")
            this.applyAssociation(association)
        } finally {
            paddedLogger.flush()
        }
    }

    fun removeAssociation(association: OAssociationMetadata) {
        try {
            appendLine("remove association [${association.outClassName} -> ${association.name} -> ${association.inClassName}] if exists:")
            removeAssociationImpl(association)
        } finally {
            paddedLogger.flush()
        }
    }

    // Vertices and Edges

    private fun createVertexClassIfAbsent(dnqEntity: EntityMetaData) {
        append(dnqEntity.type)
        val oClass = oSession.createVertexClassIfAbsent(dnqEntity.type)
        oClass.applySuperClass(dnqEntity.superType)
        appendLine()

        oSession.setClassIdIfAbsent(oClass)
        oSession.createLocalEntityIdSequenceIfAbsent(oClass)
        /*
        * We do not apply a unique index to the localEntityId property because indices in OrientDB are polymorphic.
        * So, you can not have the same value in a property in an instance of a superclass and in an instance of its subclass.
        * But it exactly what happens in the original Xodus.
        * */

        /*
        * It is more efficient to create indices after the data migration.
        * So, we only remember indices here and let the user create them later.
        * */
        for (index in dnqEntity.ownIndexes) {
            val properties = index.fields.filter { it.isProperty }.map { it.name }
            val links = index.fields.filter { !it.isProperty }.map { it.name }

            /**
             * These links take part in unique indices.
             * OrientDB does not support links in unique indices (due to how links are implemented).
             * So, we do the following workaround for this case.
             * We create an internal property for such links that contains the target entity ids, and use this property in the index.
             */
            for (link in links) {
                val linkSet = linksInUniqueIndicesByClassName.getOrPut(dnqEntity.type) { HashSet() } as MutableSet<String>
                linkSet.add(link)
            }
            addIndex(DeferredIndex(dnqEntity.type, properties + links.map { linkTargetEntityIdPropertyName(it) }, unique = true))
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
        val className = OVertexEntity.edgeClassName(name)
        var oClass: OClass? = getClass(className)
        if (oClass == null) {
            oClass = oSession.createEdgeClass(className)!!
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

    private fun applyAssociation(association: OAssociationMetadata) {
        append(association.name)

        val class1 = oSession.getClass(association.outClassName)
            ?: throw IllegalStateException("${association.outClassName} class is not found")
        val class2 = oSession.getClass(association.inClassName)
            ?: throw IllegalStateException("${association.inClassName} class is not found")

        val edgeClass = oSession.createEdgeClassIfAbsent(association.name)
        appendLine()

        withPadding {
            // class1.prop1 -> edgeClass -> class2
            applyAssociation(
                association.name,
                edgeClass,
                outClass = class1,
                outCardinality = association.cardinality,
                inClass = class2,
            )
        }
    }

    private fun applyAssociation(
        associationName: String,
        edgeClass: OClass,
        outClass: OClass,
        outCardinality: AssociationEndCardinality,
        inClass: OClass,
    ) {
        val linkOutPropName = OVertex.getEdgeLinkFieldName(ODirection.OUT, edgeClass.name)
        append("outProp: ${outClass.name}.$linkOutPropName")
        val outProp = outClass.createLinkPropertyIfAbsent(linkOutPropName, null)
        if (applyLinkCardinality) {
            // applying cardinality only to out direct property
            outProp.applyCardinality(outCardinality)
        }
        appendLine()

        val linkInPropName = OVertex.getEdgeLinkFieldName(ODirection.IN, edgeClass.name)
        append("inProp: ${inClass.name}.$linkInPropName")
        inClass.createLinkPropertyIfAbsent(linkInPropName, null)
        appendLine()

        addIndex(linkUniqueIndex(edgeClass.name))
        /*
        * We do not apply cardinality for the in-properties because, we do not know if there is any restrictions.
        * Because AssociationEndCardinality describes the cardinality of a single end.
        * */

       if (associationName in linksInUniqueIndicesByClassName.getOrDefault(outClass.name, emptySet())) {
            val indexedPropName = linkTargetEntityIdPropertyName(associationName)
            append("prop for composite indices: ${outClass.name}.$indexedPropName")
            outClass.createPropertyIfAbsent(indexedPropName, OType.LINKBAG)
        }

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

    private fun removeAssociationImpl(association: OAssociationMetadata) {
        removeEdge(association.outClassName, association.name, ODirection.OUT)
        removeEdge(association.inClassName, association.name, ODirection.IN)
    }

    private fun removeEdge(className: String, associationName: String, direction: ODirection) {
        append(className)
        val sourceClass = oSession.getClass(className)
        if (sourceClass != null) {
            val propOutName = OVertex.getEdgeLinkFieldName(direction, associationName)
            append(".$propOutName")
            if (sourceClass.existsProperty(propOutName)) {
                sourceClass.dropProperty(propOutName)
                append(" deleted")
            } else {
                append(" not found")
            }
        } else {
            append(" not found")
        }
        appendLine()
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
                    val requiredBecauseOfIndex =
                        dnqEntity.ownIndexes.any { index -> index.fields.any { it.name == propertyMetaData.name } }
                    oClass.applySimpleProperty(propertyMetaData, required || requiredBecauseOfIndex)
                }
            }

            val prop = SimplePropertyMetaDataImpl(LOCAL_ENTITY_ID_PROPERTY_NAME, "long")
            oClass.applySimpleProperty(prop, true)
            // we need this index regardless what we have in indexForEverySimpleProperty
            addIndex(simplePropertyIndex(dnqEntity.type, LOCAL_ENTITY_ID_PROPERTY_NAME))
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
                val primitiveTypeName =
                    simpleProp.primitiveTypeName ?: throw IllegalArgumentException("primitiveTypeName is null")

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
                    val typeParameter = simpleProp.typeParameterNames?.firstOrNull()
                        ?: throw IllegalStateException("$propertyName is Set but does not contain information about the type parameter")
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

    private fun OClass.createBinaryBlobPropertyIfAbsent(propertyName: String): OProperty =
        createBlobPropertyIfAbsent(propertyName, OVertexEntity.BINARY_BLOB_CLASS_NAME)

    private fun OClass.createStringBlobPropertyIfAbsent(propertyName: String): OProperty =
        createBlobPropertyIfAbsent(propertyName, OVertexEntity.STRING_BLOB_CLASS_NAME)

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
        require(oProperty.type == oType) { "$propertyName type is ${oProperty.type} but $oType was expected instead. Types migration is not supported." }
        return oProperty
    }

    /*
    * linkedClass is nullable because sometimes we do not set it.
    *
    * We do not set linkedClass for direct link in-properties
    * because there can be several links with the same name.
    * Consider the following example:
    * type2 -[link1]-> type1
    * type3 -[link1]-> type1
    *
    * What linkedClass should be for type1.directInProperty?
    *
    * But we still can set linkedClassType for direct link out-properties.
    * */
    private fun OClass.createLinkPropertyIfAbsent(propertyName: String, linkedClass: OClass?): OProperty {
        append(", linkedClassType class is ${linkedClass?.name}")
        val oProperty = if (existsProperty(propertyName)) {
            append(", already created")
            getProperty(propertyName)
        } else {
            append(", created")
            createProperty(propertyName, OType.LINKBAG, linkedClass)
        }
        require(oProperty.type == OType.LINKBAG) { "$propertyName type is ${oProperty.type} but ${OType.LINKBAG} was expected instead. Types migration is not supported." }
        require(oProperty.linkedClass == linkedClass) { "$propertyName type of the set is ${oProperty.linkedClass.name} but ${linkedClass?.name} was expected instead. Types migration is not supported." }
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
        require(oProperty.type == OType.EMBEDDEDSET) { "$propertyName type is ${oProperty.type} but ${OType.EMBEDDEDSET} was expected instead. Types migration is not supported." }
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

            "datetime" -> OType.LONG

            else -> throw IllegalArgumentException("$jvmTypeName is not supported. Feel free to support it.")
        }
    }
}
