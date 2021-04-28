/**
 * Copyright 2010 - 2021 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.query

import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.PersistentStoreTransaction
import jetbrains.exodus.entitystore.iterate.EntityIdSetIterable
import jetbrains.exodus.entitystore.iterate.EntityIterableBase
import jetbrains.exodus.query.metadata.ModelMetaData
import java.util.*

@Suppress("EqualsOrHashCode")
class Or(left: NodeBase, right: NodeBase) : CommutativeOperator(left, right) {

    companion object {

        @JvmStatic
        fun or(left: NodeBase, right: NodeBase): NodeBase {
            if (left is GetAll) {
                return left
            }
            if (right is GetAll) {
                return right
            }
            return Or(left, right)
        }
    }

    override fun instantiate(
        entityType: String,
        queryEngine: QueryEngine,
        metaData: ModelMetaData,
        context: InstantiateContext
    ): Iterable<Entity> {
        if (depth >= Utils.reduceUnionsOfLinksDepth && !context.visited.contains(this)) {
            val linkNames = hashMapOf<String, EntityIdSetIterable>()
            val txn = queryEngine.persistentStore.andCheckCurrentTransaction
            if (isUnionOfLinks(txn, linkNames, context)) {
                val all = (queryEngine.instantiateGetAll(txn, entityType) as EntityIterableBase).source
                var result: Iterable<Entity> = EntityIterableBase.EMPTY
                linkNames.forEach { (linkName, ids) ->
                    result = queryEngine.union(result, all.findLinks(ids, linkName))
                }
                return result
            }
        }
        var result: Iterable<Entity> = EntityIterableBase.EMPTY
        val stack = ArrayDeque<NodeBase>()
        stack.push(this)
        while (stack.isNotEmpty()) {
            val node = stack.pop()
            if (node !is Or) {
                result = queryEngine.unionAdjusted(result, node.instantiate(entityType, queryEngine, metaData, context))
            } else {
                stack.push(node.left)
                stack.push(node.right)
            }
        }
        return result
    }

    override fun equals(other: Any?): Boolean {
        if (other === this) {
            return true
        }
        if (other == null) {
            return false
        }
        checkWildcard(other)
        return other is Or && super.equals(other)
    }

    override fun getClone(): NodeBase {
        return Or(left.clone, right.clone)
    }

    override fun getSimpleName(): String {
        return "or"
    }

    private fun isUnionOfLinks(
        txn: PersistentStoreTransaction,
        linkNames: MutableMap<String, EntityIdSetIterable>,
        context: InstantiateContext
    ): Boolean {
        if (context.visited.contains(this)) return false
        val stack = ArrayDeque<Or>()
        stack.push(this)
        while (stack.isNotEmpty()) {
            val or = stack.pop()
            if (!context.visited.add(or)) continue
            or.left.let { left ->
                if (left is Or) {
                    stack.push(left)
                } else {
                    if (left !is LinkEqual) return false
                    linkNames.computeIfAbsent(left.name) { EntityIdSetIterable(txn) }.addTarget(left.toId)
                }
            }
            or.right.let { right ->
                if (right is Or) {
                    stack.push(right)
                } else {
                    if (right !is LinkEqual) return false
                    linkNames.computeIfAbsent(right.name) { EntityIdSetIterable(txn) }.addTarget(right.toId)
                }
            }
        }
        return true
    }
}