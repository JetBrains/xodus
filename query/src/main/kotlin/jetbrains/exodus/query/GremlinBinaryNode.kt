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

class GremlinBinaryNode(
    private val left: NodeBase,
    private val right: NodeBase,
    commutative: Boolean,
    private val shortName: String,
    private val combineQuery: (GremlinBlock, GremlinBlock) -> GremlinBlock,
    private val combineInMem: ((Iterable<Entity>, Iterable<Entity>) -> Iterable<Entity>)? = null
) : BinaryOperator(left, right, commutative), GremlinNode {

    override fun getBlock(): GremlinBlock? {
        val leftQ = (left as? GremlinNode)?.getBlock()
        val rightQ = (right as? GremlinNode)?.getBlock()

        return if (leftQ == null || rightQ == null) null
        else combineQuery(leftQ, rightQ)
    }

    override fun instantiate(
        entityType: String,
        queryEngine: QueryEngine,
        metaData: ModelMetaData?,
        context: InstantiateContext?
    ): Iterable<Entity> =
        block?.let {
            // todo: We should operate with GremlinQueries, not blocks at this level
            GremlinEntityIterable.where(
                entityType,
                queryEngine.oStore.requireActiveTransaction(),
                it
            )
        } ?: combineInMem?.let {
            it(
                getLeft().instantiate(entityType, queryEngine, metaData, context),
                getRight().instantiate(entityType, queryEngine, metaData, context)
            )
        } ?: run {
            throw IllegalStateException("Only GremlinNode instances can be used in the chain")
        }

    override fun getClone(): NodeBase =
        GremlinBinaryNode(getLeft().clone, getRight().clone, commutative, shortName, combineQuery, combineInMem)

    override fun getSimpleName(): String = shortName
}
