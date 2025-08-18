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
package jetbrains.exodus.entitystore.youtrackdb.gremlin

import com.jetbrains.youtrack.db.api.gremlin.YTDBVertex
import com.jetbrains.youtrack.db.internal.core.gremlin.YTDBVertexInternal
import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.EntityId
import jetbrains.exodus.entitystore.EntityIterator
import jetbrains.exodus.entitystore.youtrackdb.YTDBEntityStore
import jetbrains.exodus.entitystore.youtrackdb.YTDBVertexEntity
import java.lang.AutoCloseable

class GremlinEntityIterator(
    private val gremlinVertices: Iterator<YTDBVertex>,
    private val store: YTDBEntityStore,
    private var closed: Boolean = false,
    private val disposeResources: () -> Unit = {},
) : EntityIterator, AutoCloseable {

    companion object {
        fun vertexToEntity(vertex: YTDBVertex, store: YTDBEntityStore) = YTDBVertexEntity(
            (vertex as YTDBVertexInternal).rawEntity,
            store
        )
    }

    override fun skip(number: Int): Boolean {
        repeat(number) {
            if (!hasNext()) {
                return false
            }
            next()
        }
        return true
    }

    override fun nextId(): EntityId = next().id

    override fun dispose(): Boolean {
        if (closed) {
            return false
        }
        closed = true
        disposeResources()
        return true
    }

    override fun shouldBeDisposed(): Boolean = !closed

    override fun remove() = throw UnsupportedOperationException()

    override fun hasNext(): Boolean {
        val hasNext = gremlinVertices.hasNext()

        if (!hasNext) dispose()

        return hasNext
    };

    // todo: special TimeoutException handling?
    override fun next(): Entity = vertexToEntity(gremlinVertices.next(), store)

    override fun close() {
        dispose()
    }
}