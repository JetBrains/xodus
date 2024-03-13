package jetbrains.exodus.query.metadata

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.metadata.schema.OClass
import mu.KotlinLogging

private val log = KotlinLogging.logger {}

class DeferredIndicesCreator {
    private val indicesByOwnerVertexName = HashMap<String, MutableSet<DeferredIndex>>()

    private val logger = PaddedLogger(log)

    fun getIndices(): Map<String, Set<DeferredIndex>> = indicesByOwnerVertexName

    fun add(index: DeferredIndex) {
        indicesByOwnerVertexName.getOrPut(index.ownerVertexName) { HashSet() }.add(index)
    }

    fun createIndices(oSession: ODatabaseSession) {
        try {
            with (logger) {
                appendLine("applying indices to OrientDB")

                appendLine("validating indices...")
                indicesByOwnerVertexName.forEach { (_, indices) -> indices.forEach { it.requireAllFieldsAreSimpleProperty() } }

                appendLine("creating indices if absent:")
                for ((ownerVertexName, indices) in indicesByOwnerVertexName) {
                    val oClass = oSession.getClass(ownerVertexName) ?: throw IllegalStateException("$ownerVertexName not found")
                    appendLine("${oClass.name}:")
                    withPadding {
                        for ((_, indexName, properties, unique) in indices) {
                            append(indexName)
                            if (oClass.getClassIndex(indexName) == null) {
                                val indexType = if (unique) OClass.INDEX_TYPE.UNIQUE else OClass.INDEX_TYPE.NOTUNIQUE
                                val propertiesNames = properties.map { it.name }.toTypedArray()
                                oClass.createIndex(indexName, indexType, *propertiesNames)
                                appendLine(", created")
                            } else {
                                appendLine(", already created")
                            }
                        }
                    }
                }
            }
        } catch (e: Throwable) {
            logger.flush()
        } finally {
            logger.flush()
        }
    }
}

data class DeferredIndex(
    val ownerVertexName: String,
    val indexName: String,
    val properties: List<IndexField>,
    val unique: Boolean
) {
    constructor(ownerVertexName: String, properties: List<IndexField>, unique: Boolean): this(
        ownerVertexName,
        indexName = "${ownerVertexName}_${properties.joinToString("_") { it.name }}",
        properties,
        unique = unique
    )

    constructor(index: Index, unique: Boolean): this(index.ownerEntityType, index.fields, unique)

    val allFieldsAreSimpleProperty: Boolean = properties.all { it.isProperty }
}

fun DeferredIndex.requireAllFieldsAreSimpleProperty() = require(allFieldsAreSimpleProperty) { "Found an index with a link: $indexName. Indices with links are not supported." }

fun OClass.makeDeferredIndexForEmbeddedSet(propertyName: String): DeferredIndex {
    val indexField = IndexFieldImpl()
    indexField.isProperty = true
    indexField.name = propertyName
    return DeferredIndex(
        ownerVertexName = this.name,
        listOf(indexField),
        unique = false
    )
}

