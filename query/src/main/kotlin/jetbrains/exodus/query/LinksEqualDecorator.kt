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
import jetbrains.exodus.entitystore.EntityIterable
import jetbrains.exodus.entitystore.youtrackdb.gremlin.GremlinBlock
import jetbrains.exodus.entitystore.youtrackdb.gremlin.GremlinEntityIterable
import jetbrains.exodus.entitystore.youtrackdb.gremlin.GremlinQuery
import jetbrains.exodus.query.Utils.safe_equals
import jetbrains.exodus.query.metadata.ModelMetaData

@Suppress("EqualsOrHashCode")
class LinksEqualDecorator(val linkName: String, var decorated: NodeBase, val linkEntityType: String) : NodeBase(),
    GremlinNode {

    override fun instantiate(
        entityType: String,
        queryEngine: QueryEngine,
        metaData: ModelMetaData,
        context: InstantiateContext
    ): Iterable<Entity> {
        val txn = queryEngine.oStore.requireActiveTransaction()
        return query
            ?.let { GremlinEntityIterable.query(txn, it.then(GremlinBlock.HasLabel(entityType))) }
            ?: throw IllegalStateException("Only GremlinNode instances can be used in the chain")
    }

    override fun getQuery(): GremlinQuery? =
        (decorated as GremlinNode).query?.let {
            val condition = it as? GremlinQuery.Where
                ?: throw IllegalStateException("Only Where instances can be used in the chain")
            GremlinQuery.all
                .then(GremlinBlock.OutLink(linkName))
                .then(condition.block)
        }

    override fun getClone(): NodeBase = LinksEqualDecorator(linkName, decorated.clone, linkEntityType)

    override fun optimize(sorts: Sorts, rules: OptimizationPlan) {
        if (decorated !is GremlinLeaf) {
            return
        }
        val query = (decorated as GremlinLeaf).query
        if (query !is GremlinQuery.Where) {
            return
        }


        if (query.block is GremlinBlock.HasNoLink) {
            val hnl = query.block as GremlinBlock.HasNoLink
            // todo: does this even make sense now?
            decorated = GremlinLeaf(
                GremlinQuery.all.difference(
                    GremlinQuery.all.then(GremlinBlock.HasLink(hnl.linkName))
                )
            )
        } else if (query.block is GremlinBlock.PropNull) {
            val pn = query.block as GremlinBlock.PropNull
            decorated = GremlinLeaf(
                GremlinQuery.all.difference(
                    GremlinQuery.all.then(GremlinBlock.PropNotNull(pn.property))
                )
            )
        }
    }

    override fun equals(other: Any?): Boolean {
        if (other === this) {
            return true
        }
        if (other == null) {
            return false
        }
        checkWildcard(other)
        if (other !is LinksEqualDecorator) {
            return false
        }
        val decorator = other
        return if (!safe_equals(linkName, decorator.linkName) || !safe_equals(
                linkEntityType,
                decorator.linkEntityType
            )
        ) {
            false
        } else safe_equals(decorated, decorator.decorated)
    }

    override fun toString(prefix: String): String {
        return """
            ${super.toString(prefix)}
            ${decorated.toString(TREE_LEVEL_INDENT + prefix)}
            """.trimIndent()
    }

    override fun getHandle(sb: StringBuilder): StringBuilder {
        super.getHandle(sb).append('(').append(linkName).append(',').append(linkEntityType).append(')').append('{')
        return decorated.getHandle(sb).append('}')
    }

    override fun getSimpleName() = "led"
}
