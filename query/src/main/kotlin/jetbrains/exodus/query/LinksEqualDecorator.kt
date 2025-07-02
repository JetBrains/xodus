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
import jetbrains.exodus.entitystore.youtrackdb.iterate.YTDBEntityOfTypeIterable
import jetbrains.exodus.query.Utils.safe_equals
import jetbrains.exodus.query.metadata.ModelMetaData

@Suppress("EqualsOrHashCode")
class LinksEqualDecorator(val linkName: String, var decorated: NodeBase, val linkEntityType: String) : NodeBase() {

    override fun instantiate(
        entityType: String,
        queryEngine: QueryEngine,
        metaData: ModelMetaData,
        context: InstantiateContext
    ): Iterable<Entity> {
        val txn = queryEngine.oStore.requireActiveTransaction()
        return YTDBEntityOfTypeIterable(txn, entityType).findLinks(
            decorated.instantiate(linkEntityType, queryEngine, metaData, context), linkName
        )
    }

    override fun getClone(): NodeBase = LinksEqualDecorator(linkName, decorated.clone, linkEntityType)

    override fun optimize(sorts: Sorts, rules: OptimizationPlan) {
        if (decorated is LinkEqual) {
            val linkEqual = decorated as LinkEqual
            if (linkEqual.toId == null) {
                decorated = Minus(NodeFactory.all_old(), LinkNotNull(linkEqual.name))
            }
        } else if (decorated is PropertyEqual) {
            val propEqual = decorated as PropertyEqual
            if (propEqual.value == null) {
                decorated = Minus(NodeFactory.all_old(), PropertyNotNull(propEqual.name))
            }
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
