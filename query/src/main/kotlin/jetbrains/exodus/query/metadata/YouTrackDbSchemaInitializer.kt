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
import com.jetbrains.youtrack.db.api.schema.PropertyType
import com.jetbrains.youtrack.db.api.schema.SchemaClass
import com.jetbrains.youtrack.db.internal.core.collate.CaseInsensitiveCollate
import jetbrains.exodus.entitystore.orientdb.OVertexEntity
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.LOCAL_ENTITY_ID_PROPERTY_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.linkTargetEntityIdPropertyName
import jetbrains.exodus.entitystore.orientdb.createClassIdSequenceIfAbsent
import jetbrains.exodus.entitystore.orientdb.createLocalEntityIdSequenceIfAbsent
import jetbrains.exodus.entitystore.orientdb.setClassIdIfAbsent
import mu.KotlinLogging

private val log = KotlinLogging.logger {}

internal data class SchemaApplicationResult(
    val indices: Map<String, Set<DeferredIndex>>,
    val newIndexedLinks: Map<String, Set<String>> // ClassName -> set of link names
)

internal fun DatabaseSession.applySchema(
    metaData: ModelMetaData,
    indexForEverySimpleProperty: Boolean = false,
    applyLinkCardinality: Boolean = true
): SchemaApplicationResult =
    applySchema(metaData.entitiesMetaData, indexForEverySimpleProperty, applyLinkCardinality)

internal fun DatabaseSession.applySchema(
    entitiesMetaData: Iterable<EntityMetaData>,
    indexForEverySimpleProperty: Boolean = false,
    applyLinkCardinality: Boolean = true
): SchemaApplicationResult {
    val initializer =
        YouTrackDbSchemaInitializer(
            entitiesMetaData,
            this,
            indexForEverySimpleProperty,
            applyLinkCardinality
        )
    return initializer.apply()
}

internal fun DatabaseSession.addAssociation(
    outEntityMetadata: EntityMetaData,
    association: AssociationEndMetaData,
    applyLinkCardinality: Boolean = true
): SchemaApplicationResult {
    val link = association.toLinkMetadata(outEntityMetadata.type)
    return addAssociation(
        link,
        outEntityMetadata.getIndicesContainingLink(link.name),
        applyLinkCardinality
    )
}

internal fun DatabaseSession.addAssociation(
    link: LinkMetadata,
    indicesContainingLink: List<Index>,
    applyLinkCardinality: Boolean = true
): SchemaApplicationResult {
    val initializer = YouTrackDbSchemaInitializer(
        listOf(),
        this,
        indexForEverySimpleProperty = false,
        applyLinkCardinality = applyLinkCardinality
    )
    return initializer.addAssociation(link, indicesContainingLink)
}

internal fun DatabaseSession.removeAssociation(
    sourceClassName: String,
    targetClassName: String,
    associationName: String
) {
    removeAssociation(
        LinkMetadata(
            name = associationName,
            outClassName = sourceClassName,
            inClassName = targetClassName,
            // it is ignored
            cardinality = AssociationEndCardinality._1
        )
    )
}

internal fun DatabaseSession.removeAssociation(
    association: LinkMetadata
) {
    val initializer =
        YouTrackDbSchemaInitializer(
            listOf(),
            this,
            indexForEverySimpleProperty = false,
            applyLinkCardinality = false
        )
    initializer.removeAssociation(association)
}

internal data class LinkMetadata(
    val name: String,
    val outClassName: String,
    val inClassName: String,
    val cardinality: AssociationEndCardinality
)

internal fun AssociationEndMetaData.toLinkMetadata(outClassName: String): LinkMetadata =
    LinkMetadata(
        name = name,
        outClassName = outClassName,
        inClassName = oppositeEntityMetaData.type,
        cardinality = cardinality
    )

private fun EntityMetaData.getIndicesContainingLink(linkName: String): List<Index> {
    return indexes.filter { index -> index.fields.any { field -> field.name == linkName } }
}

internal class YouTrackDbSchemaInitializer(
    private val entitiesMetaData: Iterable<EntityMetaData>,
    private val oSession: DatabaseSession,
    private val indexForEverySimpleProperty: Boolean,
    private val applyLinkCardinality: Boolean
) {
    private val paddedLogger = PaddedLogger.logger(log)

    private fun withPadding(code: () -> Unit) = paddedLogger.withPadding(4, code)

    private fun append(s: String) = paddedLogger.append(s)

    private fun appendLine(s: String = "") = paddedLogger.appendLine(s)


    private val indices = HashMap<String, MutableSet<DeferredIndex>>()

    private val newIndexedLinks = HashMap<String, MutableSet<String>>()

    private fun addIndex(index: DeferredIndex) {
        indices.getOrPut(index.ownerVertexName) { HashSet() }.add(index)
    }

    private fun simplePropertyIndex(entityName: String, propertyName: String): DeferredIndex {
        return DeferredIndex(entityName, setOf(propertyName), unique = false)
    }

    private fun linkUniqueIndex(edgeClassName: String): DeferredIndex {
        return DeferredIndex(
            edgeClassName,
            setOf(Edge.DIRECTION_IN, Edge.DIRECTION_OUT),
            unique = true
        )
    }

    fun apply(): SchemaApplicationResult {
        val start = System.currentTimeMillis()
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
                            this.addLinkImpl(
                                associationEnd.toLinkMetadata(dnqEntity.type),
                                dnqEntity.getIndicesContainingLink(associationEnd.name)
                            )
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

            return SchemaApplicationResult(
                indices = indices,
                newIndexedLinks
            )
        } finally {
            paddedLogger.flush()
            log.info("Schema initialization took ${System.currentTimeMillis() - start}ms")
        }
    }

    fun addAssociation(
        association: LinkMetadata,
        indicesContainingLink: List<Index>
    ): SchemaApplicationResult {
        try {
            appendLine("create association [${association.outClassName} -> ${association.name} -> ${association.inClassName}] if absent:")
            this.addLinkImpl(
                association,
                indicesContainingLink,
            )
            return SchemaApplicationResult(
                indices = indices,
                newIndexedLinks = newIndexedLinks
            )
        } finally {
            paddedLogger.flush()
        }
    }

    fun removeAssociation(association: LinkMetadata) {
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
        *
        * We ignore here any indices that contain links. It is because Xodus adds links
        * when the schema is already initialized. So, having here an index that contains
        * a link that has not yet been added to the schema is a valid case.
        *
        * We add indices containing links when we add a link from the index.
        * */
        for (index in dnqEntity.ownIndexes.filter { it.fields.none { !it.isProperty } }) {
            val properties = index.fields.map { it.name }.toSet()

            addIndex(
                DeferredIndex(
                    dnqEntity.type,
                    properties,
                    unique = true
                )
            )
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

    private fun DatabaseSession.createVertexClassIfAbsent(name: String): SchemaClass {
        var oClass: SchemaClass? = getClass(name)
        if (oClass == null) {
            oClass = oSession.createVertexClass(name)!!
            append(", created")
        } else {
            append(", already created")
        }
        return oClass
    }

    private fun DatabaseSession.createEdgeClassIfAbsent(name: String): SchemaClass {
        val className = OVertexEntity.edgeClassName(name)
        var oClass: SchemaClass? = getClass(className)
        if (oClass == null) {
            oClass = oSession.createEdgeClass(className)!!
            append(", edge class created")
        } else {
            append(", edge class already created")
        }
        return oClass
    }

    private fun SchemaClass.applySuperClass(superClassName: String?) {
        if (superClassName == null) {
            append(", no super type")
        } else {
            append(", super type is $superClassName")
            val superClass = oSession.getClass(superClassName)
            if (superClasses.contains(superClass)) {
                append(", already set")
            } else {
                addSuperClass(oSession, superClass)
                append(", set")
            }
        }
    }


    // Links

    private fun addLinkImpl(
        link: LinkMetadata,
        indicesContainingLink: List<Index>,
    ) {
        append(link.name)

        val outClass = oSession.getClass(link.outClassName)
            ?: throw IllegalStateException("${link.outClassName} class is not found")
        val inClass = oSession.getClass(link.inClassName)
            ?: throw IllegalStateException("${link.inClassName} class is not found")

        val edgeClass = oSession.createEdgeClassIfAbsent(link.name)
        appendLine()

        withPadding {
            if (applyLinkCardinality) {
                applyLinkCardinality(
                    edgeClass,
                    outClass,
                    link.cardinality,
                    inClass,
                )
            }
            applyIndices(
                link.name,
                edgeClass,
                outClass,
                indicesContainingLink
            )
        }
    }

    private fun applyLinkCardinality(
        edgeClass: SchemaClass,
        outClass: SchemaClass,
        outCardinality: AssociationEndCardinality,
        inClass: SchemaClass,
    ) {
        val linkOutPropName = Vertex.getEdgeLinkFieldName(Direction.OUT, edgeClass.name)
        append("outProp: ${outClass.name}.$linkOutPropName")
        val outProp = outClass.createLinkPropertyIfAbsent(linkOutPropName)
        // applying cardinality only to out direct property
        outProp.applyCardinality(outCardinality)
        appendLine()

        val linkInPropName = Vertex.getEdgeLinkFieldName(Direction.IN, edgeClass.name)
        append("inProp: ${inClass.name}.$linkInPropName")
        inClass.createLinkPropertyIfAbsent(linkInPropName)
        appendLine()

        /*
        * We do not apply cardinality for the in-properties because, we do not know if there is any restrictions.
        * Because AssociationEndCardinality describes the cardinality of a single end.
        * */
    }

    private fun applyIndices(
        linkName: String,
        edgeClass: SchemaClass,
        outClass: SchemaClass,
        indicesContainingLink: List<Index>
    ) {
        addIndex(linkUniqueIndex(edgeClass.name))

        if (indicesContainingLink.isNotEmpty()) {
            val indexedPropName = linkTargetEntityIdPropertyName(linkName)
            append("prop for composite indices: ${outClass.name}.$indexedPropName")

            if (!outClass.existsProperty(indexedPropName)) {
                newIndexedLinks.getOrPut(outClass.name) { HashSet() }.add(linkName)
            }
            outClass.createPropertyIfAbsent(indexedPropName, PropertyType.LINKBAG)
        }

        for (index in indicesContainingLink) {
            val simpleProperties = index.fields.filter { it.isProperty }.map { it.name }.toSet()
            val linkComplementaryProperties = index.fields.filter { !it.isProperty }
                .map { linkTargetEntityIdPropertyName(it.name) }.toSet()
            val allIndexedProperties = simpleProperties + linkComplementaryProperties
            // create the index only if all the containing properties are already initialized
            if (allIndexedProperties.all { outClass.existsProperty(it) }) {
                addIndex(
                    DeferredIndex(
                        outClass.name,
                        allIndexedProperties,
                        unique = true
                    )
                )
            }
        }

        appendLine()
    }

    private fun SchemaProperty.applyCardinality(cardinality: AssociationEndCardinality) {
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

    private fun SchemaProperty.setMaxIfDifferent(max: String?) {
        append(", max $max")
        if (this.max == max) {
            append(" already set")
        } else {
            setMax(oSession, max)
            append(" set")
        }
    }

    private fun SchemaProperty.setMinIfDifferent(min: String?) {
        append(", min $min")
        if (this.min == min) {
            append(" already set")
        } else {
            setMin(oSession, min)
            append(" set")
        }
    }

    private fun removeAssociationImpl(association: LinkMetadata) {
        removeEdge(association.outClassName, association.name, Direction.OUT)
        removeEdge(association.inClassName, association.name, Direction.IN)
    }

    private fun removeEdge(className: String, associationName: String, direction: Direction) {
        append(className)
        val sourceClass = oSession.getClass(className)
        val edgeClassName = OVertexEntity.edgeClassName(associationName)
        if (sourceClass != null) {
            val propOutName = Vertex.getEdgeLinkFieldName(direction, edgeClassName)
            append(".$propOutName")
            if (sourceClass.existsProperty(propOutName)) {
                sourceClass.dropProperty(oSession, propOutName)
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
                    /*
                     Xodus does not let a property be null/empty if it is in an index.
                     Check out TransientSessionImpl.checkBeforeSaveChangesConstraints() for details.
                     Xodus explicitly prohibits empty values for indexed simple properties (it throws more or less understandable exception).
                     Xodus implicitly prohibits empty values for indexed links (it crashes with null pointer exception).
                     */
                    val requiredBecauseOfIndex =
                        dnqEntity.ownIndexes.any { index -> index.fields.any { it.name == propertyMetaData.name } }
                    oClass.applySimpleProperty(propertyMetaData, required || requiredBecauseOfIndex)
                }
            }

            val prop = SimplePropertyMetaDataImpl(LOCAL_ENTITY_ID_PROPERTY_NAME, "long")
            oClass.applySimpleProperty(prop, true)
            // we need this index regardless what we have in indexForEverySimpleProperty
            // the index for localEntityId must not be unique, otherwise it will not let the same localEntityId
            // for subtypes of a supertype
            addIndex(simplePropertyIndex(dnqEntity.type, LOCAL_ENTITY_ID_PROPERTY_NAME))
        }
    }

    private fun SchemaClass.applySimpleProperty(
        simpleProp: PropertyMetaDataImpl,
        required: Boolean
    ) {
        val propertyName = simpleProp.name
        append(propertyName)

        when (simpleProp.type) {
            jetbrains.exodus.query.metadata.PropertyType.PRIMITIVE -> {
                require(simpleProp is SimplePropertyMetaDataImpl) { "$propertyName is a primitive property but it is not an instance of SimplePropertyMetaDataImpl. Happy fixing!" }
                val primitiveTypeName =
                    simpleProp.primitiveTypeName
                        ?: throw IllegalArgumentException("primitiveTypeName is null")

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
                    val oProperty =
                        createEmbeddedSetPropertyIfAbsent(propertyName, getOType(typeParameter))

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
                    val oProperty =
                        createPropertyIfAbsent(propertyName, getOType(primitiveTypeName))
                    oProperty.setRequirement(required)
                    if (indexForEverySimpleProperty) {
                        addIndex(simplePropertyIndex(name, propertyName))
                    }
                    if (primitiveTypeName.lowercase() == "boolean" && oProperty.defaultValue == null) {
                        oProperty.setDefaultValue(oSession, "false")
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
                *
                * Booleans must be initialized with "false" by default
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

            jetbrains.exodus.query.metadata.PropertyType.TEXT -> {
                val oProperty = createPropertyIfAbsent(propertyName, PropertyType.LINK)
                oProperty.setRequirement(required)
            }

            jetbrains.exodus.query.metadata.PropertyType.BLOB -> {
                val oProperty = createPropertyIfAbsent(propertyName, PropertyType.LINK)
                oProperty.setRequirement(required)
            }
        }
        appendLine()
    }

    private fun SchemaProperty.setRequirement(required: Boolean) {
        if (required) {
            append(", required")
            if (!isMandatory) {
                setMandatory(oSession, true)
            }
            setNotNullIfDifferent(true)
        } else {
            append(", optional")
            if (isMandatory) {
                setMandatory(oSession, false)
            }
        }
    }

    private fun SchemaProperty.setNotNullIfDifferent(notNull: Boolean) {
        if (notNull) {
            append(", not nullable")
            if (!isNotNull) {
                setNotNull(oSession, true)
            }
        } else {
            append(", nullable")
            if (isNotNull) {
                setNotNull(oSession, false)
            }
        }
    }

    private fun SchemaClass.createPropertyIfAbsent(
        propertyName: String,
        oType: PropertyType
    ): SchemaProperty {
        append(", type is $oType")
        val oProperty = if (existsProperty(propertyName)) {
            append(", already created")
            getProperty(propertyName)
        } else {
            append(", created")
            createProperty(oSession, propertyName, oType)
        }
        if (oType == PropertyType.STRING) {
            if (oProperty.collate.name == CaseInsensitiveCollate.NAME) {
                append(", case-insensitive collate already set")
            } else {
                oProperty.setCollate(oSession, CaseInsensitiveCollate.NAME)
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
    private fun SchemaClass.createLinkPropertyIfAbsent(propertyName: String): SchemaProperty {
        val oProperty = if (existsProperty(propertyName)) {
            append(", already created")
            getProperty(propertyName)
        } else {
            append(", created")
            createProperty(oSession, propertyName, PropertyType.LINKBAG)
        }
        require(oProperty.type == PropertyType.LINKBAG) {
            "$propertyName type is ${oProperty.type} but ${PropertyType.LINKBAG} was expected instead. Types migration is not supported."
        }
        return oProperty
    }

    private fun SchemaClass.createEmbeddedSetPropertyIfAbsent(
        propertyName: String,
        oType: PropertyType
    ): SchemaProperty {
        append(", type of the set is $oType")
        val oProperty = if (existsProperty(propertyName)) {
            append(", already created")
            getProperty(propertyName)
        } else {
            append(", created")
            createProperty(oSession, propertyName, PropertyType.EMBEDDEDSET, oType)
        }
        if (oType == PropertyType.STRING) {
            if (oProperty.collate.name == CaseInsensitiveCollate.NAME) {
                append(", case-insensitive collate already set")
            } else {
                oProperty.setCollate(oSession, CaseInsensitiveCollate.NAME)
                append(", set case-insensitive collate")
            }
        }
        require(oProperty.type == PropertyType.EMBEDDEDSET) { "$propertyName type is ${oProperty.type} but ${PropertyType.EMBEDDEDSET} was expected instead. Types migration is not supported." }
        require(oProperty.linkedType == oType) { "$propertyName type of the set is ${oProperty.linkedType} but $oType was expected instead. Types migration is not supported." }
        return oProperty
    }

    private fun getOType(jvmTypeName: String): PropertyType {
        return when (jvmTypeName.lowercase()) {
            "boolean" -> PropertyType.BOOLEAN
            "string" -> PropertyType.STRING

            "byte" -> PropertyType.BYTE
            "short" -> PropertyType.SHORT
            "int",
            "integer" -> PropertyType.INTEGER

            "long" -> PropertyType.LONG

            "float" -> PropertyType.FLOAT
            "double" -> PropertyType.DOUBLE

            "datetime" -> PropertyType.LONG

            else -> throw IllegalArgumentException("$jvmTypeName is not supported. Feel free to support it.")
        }
    }
}
