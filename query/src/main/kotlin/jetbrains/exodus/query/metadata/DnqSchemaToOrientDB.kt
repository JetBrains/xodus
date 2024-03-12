package jetbrains.exodus.query.metadata

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.metadata.schema.OProperty
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.ODirection
import com.orientechnologies.orient.core.record.OVertex
import mu.KotlinLogging

private val log = KotlinLogging.logger {}

class DnqSchemaToOrientDB(
    private val dnqModel: ModelMetaDataImpl,
    private val oSession: ODatabaseSession,
) {
    companion object {
        val BINARY_BLOB_CLASS_NAME: String = "BinaryBlob"
        val DATA_PROPERTY_NAME = "data"

        val STRING_BLOB_CLASS_NAME: String = "StringBlob"
    }

    private val paddedLogger = PaddedLogger(log)

    val indicesCreator = DeferredIndicesCreator()

    private fun withPadding(code: () -> Unit) = paddedLogger.withPadding(4, code)

    private fun append(s: String) = paddedLogger.append(s)

    private fun appendLine(s: String = "") = paddedLogger.appendLine(s)

    fun apply() {
        try {
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
                for ((indexOwner, indices) in indicesCreator.getIndices()) {
                    appendLine("$indexOwner:")
                    withPadding {
                        for (index in indices) {
                            appendLine(index.indexName)
                        }
                    }
                }
            }
        } catch (e: Throwable) {
            paddedLogger.flush()
            log.error(e) { e.message }
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

        /*
        * It is more efficient to create indices after the data migration.
        * So, we only remember indices here and let the user create them later.
        * */
        for (index in dnqEntity.ownIndexes.map { DeferredIndex(it, unique = true)}) {
            index.requireAllFieldsAreSimpleProperty()
            indicesCreator.add(index)
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

        oSession.createEdgeClassIfAbsent(association.name)
        appendLine()

        withPadding {
            // class1.prop1 -> edgeClass -> class2
            applyLink(
                edgeClassName = association.name,
                outClass = class1,
                outCardinality = association.cardinality,
                inClass = class2,
            )
        }
    }

    private fun applyLink(
        edgeClassName: String,
        outClass: OClass,
        outCardinality: AssociationEndCardinality,
        inClass: OClass,
    ) {
        val propOutName = OVertex.getDirectEdgeLinkFieldName(ODirection.OUT, edgeClassName)
        append("${outClass.name}.$propOutName")
        val propOut = outClass.createPropertyIfAbsent(propOutName, OType.LINKBAG)
        propOut.applyCardinality(outCardinality)
        appendLine()

        val propInName = OVertex.getDirectEdgeLinkFieldName(ODirection.IN, edgeClassName)
        append("${inClass.name}.$propInName")
        inClass.createPropertyIfAbsent(propInName, OType.LINKBAG)

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
                if (propertyMetaData is SimplePropertyMetaDataImpl) {
                    val required = propertyMetaData.name in dnqEntity.requiredProperties
                    // Xodus does not let a property be null/empty if it is in an index
                    val requiredBecauseOfIndex = dnqEntity.ownIndexes.any { index -> index.fields.any { it.name == propertyMetaData.name } }
                    oClass.applySimpleProperty(propertyMetaData, required || requiredBecauseOfIndex)
                }
            }
        }
    }

    private fun OClass.applySimpleProperty(
        simpleProp: SimplePropertyMetaDataImpl,
        required: Boolean
    ) {
        val propertyName = simpleProp.name
        append(propertyName)

        val primitiveTypeName = simpleProp.primitiveTypeName ?: throw IllegalArgumentException("primitiveTypeName is null")

        when (simpleProp.type) {
            PropertyType.PRIMITIVE -> {
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
                    //val typeParameter = simpleProp.typeParameterNames?.firstOrNull() ?: throw IllegalStateException("$propertyName is Set but does not contain information about the type parameter")
                    //val oProperty = createEmbeddedSetPropertyIfAbsent(propertyName, getOType(typeParameter))

                    /*
                    * If the value is not defined, the property returns true.
                    * It is handled on the DNQ entities level.
                    * But, we still apply the required state just in case.
                    * */
                    //oProperty.setRequirement(required)

                    /*
                    * When creating an index on an EMBEDDEDSET field, OrientDB does not create an index for the field itself.
                    * Instead, it creates an index for each individual item in the set.
                    * This is done to enable quick searches for individual elements within the set.
                    *
                    * The same behaviour as the original behaviour of set properties in DNQ.
                    * */
                    //val index = makeDeferredIndexForEmbeddedSet(propertyName)
                    //indicesCreator.add(index)
                } else { // primitive types
                    val oProperty = createPropertyIfAbsent(propertyName, getOType(primitiveTypeName))
                    oProperty.setRequirement(required)
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

    private fun OClass.createBinaryBlobPropertyIfAbsent(propertyName: String): OProperty = createBlobPropertyIfAbsent(propertyName, BINARY_BLOB_CLASS_NAME)

    private fun OClass.createStringBlobPropertyIfAbsent(propertyName: String): OProperty = createBlobPropertyIfAbsent(propertyName, STRING_BLOB_CLASS_NAME)

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
            oClass.createProperty(DATA_PROPERTY_NAME, OType.BINARY)
            append(", $DATA_PROPERTY_NAME property created")
        } else {
            append(", $className class already created")
            require(oClass.existsProperty(DATA_PROPERTY_NAME)) { "$DATA_PROPERTY_NAME is missing in $className, something went dramatically wrong. Happy debugging!" }
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
        require(oProperty.type == oType) { "$propertyName type is ${oProperty.type} but $oType was expected instead. Types migration is not supported."  }
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
        require(oProperty.type == OType.EMBEDDEDSET) { "$propertyName type is ${oProperty.type} but ${OType.EMBEDDEDSET} was expected instead. Types migration is not supported."  }
        require(oProperty.linkedType == oType) { "$propertyName type of the set is ${oProperty.type} but $oType was expected instead. Types migration is not supported." }
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