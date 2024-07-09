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
                                oClass.createIndex(indexName, indexType, *properties.toTypedArray())
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
            throw e
        } finally {
            logger.flush()
        }
    }
}

data class DeferredIndex(
    val ownerVertexName: String,
    val indexName: String,
    val properties: List<String>,
    val unique: Boolean
) {
    constructor(ownerVertexName: String, properties: List<String>, unique: Boolean): this(
        ownerVertexName,
        indexName = "${ownerVertexName}_${properties.joinToString("_")}${if (unique) "_unique" else ""}",
        properties,
        unique = unique
    )
}

fun OClass.makeDeferredIndexForEmbeddedSet(propertyName: String): DeferredIndex {
    return DeferredIndex(
        ownerVertexName = this.name,
        listOf(propertyName),
        unique = false
    )
}

