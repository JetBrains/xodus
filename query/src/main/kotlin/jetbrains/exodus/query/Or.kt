/**
 * Copyright 2010 - 2018 JetBrains s.r.o.
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

import jetbrains.exodus.entitystore.*
import jetbrains.exodus.entitystore.iterate.EntityIdSet
import jetbrains.exodus.entitystore.iterate.EntityIterableBase
import jetbrains.exodus.entitystore.util.EntityIdSetFactory
import jetbrains.exodus.query.metadata.ModelMetaData

@Suppress("EqualsOrHashCode")
class Or(left: NodeBase, right: NodeBase) : CommutativeOperator(left, right) {

    private var analyzed = false

    override fun instantiate(entityType: String, queryEngine: QueryEngine, metaData: ModelMetaData): Iterable<Entity> {
        if (!analyzed && depth >= Utils.reduceUnionsOfLinksDepth) {
            val linkNames = HashSet<String>()
            val txn = queryEngine.persistentStore.andCheckCurrentTransaction
            val ids = TargetsIterable(txn)
            if (isUnionOfLinks(linkNames, ids)) {
                return queryEngine.adjustEntityIterable((txn.getAll(entityType) as EntityIterableBase).findLinks(ids, linkNames.first()))
            }
        }
        return queryEngine.unionAdjusted(left.instantiate(entityType, queryEngine, metaData), right.instantiate(entityType, queryEngine, metaData))
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

    private fun isUnionOfLinks(linkNames: MutableSet<String>, targets: TargetsIterable): Boolean {
        if (analyzed) return false
        analyzed = true
        val left = left
        if (left is Or) {
            if (!left.isUnionOfLinks(linkNames, targets)) return false
        } else if (left is LinkEqual) {
            linkNames.add(left.name)
            targets.addTarget(left.toId)
        } else {
            return false
        }
        val right = right
        if (right is Or) {
            if (!right.isUnionOfLinks(linkNames, targets)) return false
        } else if (right is LinkEqual) {
            linkNames.add(right.name)
            targets.addTarget(right.toId)
        } else {
            return false
        }
        return linkNames.size == 1
    }
}

private class TargetsIterable(txn: PersistentStoreTransaction) : EntityIterableBase(txn) {

    var ids: EntityIdSet = EntityIdSetFactory.newSet()

    override fun getIteratorImpl(txn: PersistentStoreTransaction): EntityIterator {
        throw NotImplementedError()
    }

    override fun getHandleImpl(): EntityIterableHandle {
        throw NotImplementedError()
    }

    override fun toSet(txn: PersistentStoreTransaction) = ids

    fun addTarget(id: EntityId) {
        ids = ids.add(id)
    }
}