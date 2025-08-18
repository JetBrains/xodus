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
package jetbrains.exodus.query

import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.youtrackdb.gremlin.GremlinEntityIterable
import jetbrains.exodus.entitystore.youtrackdb.gremlin.GremlinBlock
import jetbrains.exodus.query.metadata.ModelMetaData

class GremlinUnaryNode(
    child: NodeBase,
    val shortName: String,
    val op: (GremlinBlock) -> GremlinBlock
) : UnaryNode(child), GremlinNode {

    override fun getBlock(): GremlinBlock? =
        (child as? GremlinNode)?.block?.let { op(it) }

    override fun instantiate(
        entityType: String,
        queryEngine: QueryEngine,
        metaData: ModelMetaData?,
        context: InstantiateContext?
    ): Iterable<Entity> {
        val newQuery = block ?: run {
            throw IllegalStateException("Only GremlinNode instances can be used in the chain")
        }

        // todo: We should operate with GremlinQueries, not blocks at this level
        return GremlinEntityIterable.where(
            entityType,
            queryEngine.oStore.requireActiveTransaction(),
            newQuery
        )
    }

    override fun getClone(): NodeBase = GremlinUnaryNode(child.clone, shortName, op)

    override fun getSimpleName(): String = shortName
}