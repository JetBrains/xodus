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
import com.jetbrains.youtrack.db.api.record.Vertex
import com.jetbrains.youtrack.db.api.schema.SchemaClass
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.edgeClassName
import jetbrains.exodus.entitystore.orientdb.getTargetLocalEntityIds
import jetbrains.exodus.entitystore.orientdb.setTargetLocalEntityIds
import mu.KotlinLogging

private val log = KotlinLogging.logger {}

internal fun DatabaseSession.applyIndices(indices: Map<String, Set<DeferredIndex>>) {
    IndicesCreator(indices).createIndices(this)
}

internal class IndicesCreator(
    private val indicesByOwnerVertexName: Map<String, Set<DeferredIndex>>
) {
    private val logger = PaddedLogger.logger(log)

    fun createIndices(oSession: DatabaseSession) {
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
                            if (!oClass.areIndexed(oSession, *properties.toTypedArray())) {
                                val indexType = if (unique) SchemaClass.INDEX_TYPE.UNIQUE else SchemaClass.INDEX_TYPE.NOTUNIQUE
                                oClass.createIndex(oSession, indexName, indexType, *properties.toTypedArray())
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

internal fun DatabaseSession.initializeIndices(schemaApplicationResult: SchemaApplicationResult) {
    /*
    * The order of operations matter.
    * We want to initialize complementary properties before creating indices,
    * it is more efficient from the performance point of view.
    * */
    initializeComplementaryPropertiesForNewIndexedLinks(schemaApplicationResult.newIndexedLinks)
    applyIndices(schemaApplicationResult.indices)
}


internal fun DatabaseSession.initializeComplementaryPropertiesForNewIndexedLinks(
    newIndexedLinks: Map<String, Set<String>>, // ClassName -> set of link names
    commitEvery: Int = 50
) {
    if (newIndexedLinks.isEmpty()) return

    var counter = 0
    withTx {
        for ((className, indexedLinks) in newIndexedLinks) {
            for (vertex in query("select from $className").vertexStream().map { it as Vertex }) {
                for (indexedLink in indexedLinks) {
                    val edgeClassName = edgeClassName(indexedLink)
                    val targetLocalEntityIds = vertex.getTargetLocalEntityIds(indexedLink)
                    for (target in vertex.getVertices(Direction.OUT, edgeClassName)) {
                        targetLocalEntityIds.add(target)
                    }
                    vertex.setTargetLocalEntityIds(indexedLink, targetLocalEntityIds)
                    vertex.save()

                    counter++
                    if (counter == commitEvery) {
                        counter = 0
                        commit()
                        begin()
                    }
                }
            }
        }
    }
}

internal data class DeferredIndex(
    val ownerVertexName: String,
    val indexName: String,
    val properties: Set<String>,
    val unique: Boolean
) {
    constructor(ownerVertexName: String, properties: Set<String>, unique: Boolean): this(
        ownerVertexName,
        indexName = "${ownerVertexName}_${properties.sorted().joinToString("_")}${if (unique) "_unique" else ""}",
        properties,
        unique = unique
    )
}

internal fun SchemaClass.makeDeferredIndexForEmbeddedSet(propertyName: String): DeferredIndex {
    return DeferredIndex(
        ownerVertexName = this.name,
        setOf(propertyName),
        unique = false
    )
}

