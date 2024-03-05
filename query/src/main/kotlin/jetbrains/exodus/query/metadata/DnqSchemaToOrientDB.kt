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
            appendLine("topologically sorted entities: ${sortedEntities.joinToString(", ") { it.type }}")
            appendLine("creating classes if absent:")
            withPadding {
                // it is not necessary to process the entities in the topologically sorted order but why not to?
                for (dnqEntity in sortedEntities) {
                    createVertexClassIfAbsent(dnqEntity)
                }
            }
            appendLine("creating properties and connections if absent:")
            withPadding {
                /*
                * It is necessary to process entities in the topologically sorted order.
                *
                * Consider Superclass1 and Subclass1: Superclass1. All the properties of
                * Superclass1 will be both in EntityMetaData of Superclass1 and EntityMetaData of Subclass1.
                *
                * We want to those properties be created for Superclass1 in OrientDB, so we have to
                * process Superclass1 before Subclass1. That is why we have to process entities in
                * the topologically sorted order.
                * */
                for (dnqEntity in sortedEntities) {
                    createPropertiesAndConnectionsIfAbsent(dnqEntity)
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

    private fun createVertexClassIfAbsent(dnqEntity: EntityMetaData) {
        append(dnqEntity.type)
        oSession.createVertexClassIfAbsent(dnqEntity.type)
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

    private fun createPropertiesAndConnectionsIfAbsent(dnqEntity: EntityMetaData) {
        appendLine(dnqEntity.type)

        val oClass = oSession.getClass(dnqEntity.type)

        withPadding {
            // superclass
            oClass.applySuperClass(dnqEntity.superType)

            // simple properties
            appendLine("simple properties:")
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

            // link properties
            appendLine("link properties:")
            withPadding {
                for (propertyMetaData in dnqEntity.propertiesMetaData) {
                    // use associations
                    //oClass.applyLinkProperty(propertyMetaData)
                }
            }
        }
    }

    private fun OClass.applySuperClass(superClassName: String?) {
        if (superClassName == null) {
            append("no super type")
        } else {
            append("super type is $superClassName")
            val superClass = oSession.getClass(superClassName)
            if (superClasses.contains(superClass)) {
                append(", already set")
            } else {
                addSuperClass(superClass)
                append(", set")
            }
        }
        appendLine()
    }

    private fun OClass.applyLinkProperty(linkProperty: PropertyMetaDataImpl) {
        val propertyName = linkProperty.name
        val property = linkProperty.type
        append(propertyName)

        if (false) {
            val outClass = this
            val inClass = oSession.getClass("provide class name here") ?: throw IllegalStateException("Opposite class not found. Happy debugging!")

            val edgeClassName = "${outClass.name}_${inClass.name}_$propertyName"
            oSession.createEdgeClass(edgeClassName)

            val outProperty = outClass.createProperty(
                OVertex.getDirectEdgeLinkFieldName(ODirection.OUT, edgeClassName),
                OType.LINKBAG
            )

            val inProperty = inClass.createProperty(
                OVertex.getDirectEdgeLinkFieldName(ODirection.IN, edgeClassName),
                OType.LINKBAG
            )

            // Person out[0..1] --> Link
            outProperty.setMandatory(false)
            outProperty.setMin("0")
            outProperty.setMax("1")

            // Link --> in[0..1] Car
            inProperty.setMandatory(false)
            inProperty.setMin("0")
            inProperty.setMax("1")
        }
        appendLine()
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
                    // todo
                    /*
                    * To support sets we have to:
                    * 1. On the Xodus repo level
                    *   1. Add SimplePropertyMetaDataImpl.argumentType: String? property (or list of them, it is easier to extend)
                    * 2. On the XodusDNQ repo level
                    *   1. DNQMetaDataUtil.kt, addEntityMetaData(), 119 line, fill that argumentType param
                    * 3. Support here
                    * */
                    //val oProperty = createEmbeddedSetPropertyIfAbsent(propertyName, getOType(unwrappedToDeleteProp.clazz))

                    /*
                    * If the value is not defined, the property returns true.
                    * It is handled on the DNQ entities level.
                    * */
                    //oProperty.setNotNullIfDifferent(false)
                    //oProperty.setRequirement(XdPropertyRequirement.OPTIONAL)

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