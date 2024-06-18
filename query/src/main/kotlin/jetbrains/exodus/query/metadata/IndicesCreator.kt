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
import mu.KotlinLogging

private val log = KotlinLogging.logger {}

fun ODatabaseSession.applyIndices(indices: Map<String, Set<DeferredIndex>>) {
    IndicesCreator(indices).createIndices(this)
}

internal class IndicesCreator(
    private val indicesByOwnerVertexName: Map<String, Set<DeferredIndex>>
) {
    private val logger = PaddedLogger(log)

    fun createIndices(oSession: ODatabaseSession) {
        try {
            with (logger) {
                appendLine("applying indices to OrientDB")

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
        indexName = "${ownerVertexName}_${properties.joinToString("_") { it.name }}${if (unique) "_unique" else ""}",
        properties,
        unique = unique
    )

    constructor(index: Index, unique: Boolean): this(index.ownerEntityType, index.fields, unique)
}

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

