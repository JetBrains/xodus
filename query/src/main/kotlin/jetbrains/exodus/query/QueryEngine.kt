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
import jetbrains.exodus.entitystore.PersistentEntityStore
import jetbrains.exodus.entitystore.StoreTransaction
import jetbrains.exodus.entitystore.iterate.EntityIdSet
import jetbrains.exodus.entitystore.iterate.EntityIterableBase
import jetbrains.exodus.entitystore.iterate.EntityIterableBase.EMPTY
import jetbrains.exodus.entitystore.iterate.SingleEntityIterable
import jetbrains.exodus.entitystore.orientdb.iterate.OQueryEntityIterableBase
import jetbrains.exodus.entitystore.util.EntityIdSetFactory
import jetbrains.exodus.kotlin.notNull
import jetbrains.exodus.query.metadata.ModelMetaData
import mu.KLogging

open class QueryEngine(val modelMetaData: ModelMetaData?, val persistentStore: PersistentEntityStore) : KLogging(),
    IQueryEngine {

    private var _sortEngine: SortEngine? = null

    open var sortEngine: SortEngine?
        get() = _sortEngine
        set(value) {
            _sortEngine = value.notNull.apply { queryEngine = this@QueryEngine }
        }

    open fun queryGetAll(entityType: String): EntityIterable = query(null, entityType, NodeFactory.all())

    open fun query(entityType: String, tree: NodeBase): EntityIterable = query(null, entityType, tree)

    open fun query(instance: Iterable<Entity>?, entityType: String, tree: NodeBase): EntityIterable {
        return if (instance != null) {
            //TODO and here is the worst thing
            (instance as EntityIterable).intersect(tree.instantiate(entityType, this, modelMetaData, NodeBase.InstantiateContext()) as EntityIterable)
        } else {
            tree.instantiate(entityType, this, modelMetaData, NodeBase.InstantiateContext()) as EntityIterable
        }
    }

    open fun intersect(left: Iterable<Entity>, right: Iterable<Entity>): Iterable<Entity> {
        if (left === right) {
            return left
        }
        if (left.isEmpty || right.isEmpty) {
            return EMPTY
        }
        return if (left is EntityIterable || right is EntityIterable) {
            (left as EntityIterable).intersect(right as EntityIterable)
        } else {
            inMemoryIntersect(left, right)
        }
    }

    open fun union(left: Iterable<Entity>, right: Iterable<Entity>): Iterable<Entity> {
        if (left === right) {
            return left
        }
        if (left.isEmpty) {
            return right
        }
        if (right.isEmpty) {
            return left
        }
        return if (left is EntityIterable || right is EntityIterable) {
            (left as EntityIterable).union(right as EntityIterable)
        } else {
            inMemoryUnion(left, right)
        }
    }

    open fun concat(left: Iterable<Entity>, right: Iterable<Entity>): Iterable<Entity> {
        if (left.isEmpty) {
            return right
        }
        if (right.isEmpty) {
            return left
        }

        return if (left is EntityIterable || right is EntityIterable) {
            (left as EntityIterable).concat(right as EntityIterable)
        } else {
            inMemoryConcat(left, right)
        }
    }

    open fun exclude(left: Iterable<Entity>, right: Iterable<Entity>): Iterable<Entity> {
        if (left.isEmpty || left === right) {
            return EMPTY
        }
        if (right.isEmpty) {
            return left
        }
        return if (left is EntityIterable || right is EntityIterable) {
            (left as EntityIterable).minus(right as EntityIterable)
        } else {
            inMemoryExclude(left, right)
        }
    }

    open fun selectDistinct(it: Iterable<Entity>?, linkName: String): Iterable<Entity> {
        return if (it is EntityIterable){
            it.selectDistinct(linkName)
        } else {
            it?.let { inMemorySelectDistinct(it, linkName) } ?: OQueryEntityIterableBase.EMPTY
        }

    }

    open fun selectManyDistinct(it: Iterable<Entity>?, linkName: String): Iterable<Entity> {
        return if (it is EntityIterable){
            it.selectDistinct(linkName)
        } else {
            return it?.let {  inMemorySelectManyDistinct(it, linkName) } ?: OQueryEntityIterableBase.EMPTY
        }
    }

    open fun toEntityIterable(it: Iterable<Entity>): Iterable<Entity> {
        return it
    }

    open fun instantiateGetAll(entityType: String): EntityIterable {
        return instantiateGetAll(persistentStore.andCheckCurrentTransaction, entityType)
    }

    open fun instantiateGetAll(txn: StoreTransaction, entityType: String): EntityIterable {
        return txn.getAll(entityType)
    }

    open fun isPersistentIterable(it: Iterable<Entity>): Boolean = it.isPersistent

    open fun assertOperational() {}

    open fun isWrapped(it: Iterable<Entity>?): Boolean = true

    open fun wrap(entity: Entity): Iterable<Entity> {
        return SingleEntityIterable(persistentStore.andCheckCurrentTransaction, entity.id)
    }

    protected open fun inMemorySelectDistinct(it: Iterable<Entity>, linkName: String): Iterable<Entity> {
        return it.toList().mapNotNull { it.getLink(linkName) }.distinct()
    }

    protected open fun inMemorySelectManyDistinct(it: Iterable<Entity>, linkName: String): Iterable<Entity> {
        return it.toList().flatMap { it.getLinks(linkName) }.filterNotNull().distinct()
    }

    protected open fun inMemoryIntersect(left: Iterable<Entity>, right: Iterable<Entity>): Iterable<Entity> {
        val ids = right.asEntityIdSet
        return if (ids.isEmpty) right else left.filter { it.id in ids }
    }

    protected open fun inMemoryUnion(left: Iterable<Entity>, right: Iterable<Entity>): Iterable<Entity> {
        return left.union(right)
    }

    protected open fun inMemoryConcat(left: Iterable<Entity>, right: Iterable<Entity>): Iterable<Entity> {
        return left.toMutableList().apply { addAll(right) }
    }

    protected open fun inMemoryExclude(left: Iterable<Entity>, right: Iterable<Entity>): Iterable<Entity> {
        val ids = right.asEntityIdSet
        return if (ids.isEmpty) left else left.filter { it.id !in ids }
    }

}

private val Iterable<Entity>?.isEmpty: Boolean
    get() {
        return this == null || this === EMPTY || this is StaticTypedIterableDecorator && decorated === EMPTY
    }

private val Iterable<Entity>?.isPersistent: Boolean get() = this is EntityIterableBase

private val Iterable<Entity>.asEntityIdSet: EntityIdSet
    get() {
        var ids = EntityIdSetFactory.newSet()
        forEach { ids = ids.add(it.id) }
        return ids
    }

