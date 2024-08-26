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
import jetbrains.exodus.entitystore.orientdb.OEntityStore
import jetbrains.exodus.entitystore.orientdb.OQueryEntityIterable
import jetbrains.exodus.entitystore.orientdb.OStoreTransaction
import jetbrains.exodus.entitystore.orientdb.iterate.OQueryEntityIterableBase
import jetbrains.exodus.entitystore.orientdb.iterate.binop.OIntersectionEntityIterable
import jetbrains.exodus.entitystore.orientdb.iterate.link.OMultipleEntitiesIterable
import jetbrains.exodus.entitystore.util.EntityIdSetFactory
import jetbrains.exodus.kotlin.notNull
import jetbrains.exodus.query.metadata.ModelMetaData
import mu.KLogging

open class QueryEngine(val modelMetaData: ModelMetaData?, val persistentStore: PersistentEntityStore) : KLogging() {

    private var _sortEngine: SortEngine? = null

    val oStore: OEntityStore = persistentStore as OEntityStore

    open var sortEngine: SortEngine?
        get() = _sortEngine
        set(value) {
            _sortEngine = value.notNull.apply { queryEngine = this@QueryEngine }
        }

    open fun queryGetAll(entityType: String): EntityIterable = query(null, entityType, NodeFactory.all())

    open fun query(entityType: String, tree: NodeBase): EntityIterable = query(null, entityType, tree)

    open fun query(instance: Iterable<Entity>?, entityType: String, tree: NodeBase): EntityIterable {
        return when (instance) {
            null -> tree.instantiate(entityType, this, modelMetaData, NodeBase.InstantiateContext()) as EntityIterable
            is EntityIterable -> {
                if (tree is Sort){
                    val sorted = tree.applySort(entityType, instance, sortEngine!!)
                    if (sorted !is EntityIterable){
                        InMemoryEntityIterable(sorted, txn = persistentStore.andCheckCurrentTransaction, this)
                    } else {
                        sorted
                    }
                } else {
                    instance.intersect(
                        tree.instantiate(
                            entityType,
                            this,
                            modelMetaData,
                            NodeBase.InstantiateContext()
                        ) as EntityIterable
                    )
                }
            }
            else -> {
                if (tree is Sort) {
                    val sorted = tree.applySort(entityType, instance, sortEngine!!)
                    InMemoryEntityIterable(sorted, txn = persistentStore.andCheckCurrentTransaction, this)
                } else {
                    intersect(
                        instance,
                        tree.instantiate(entityType, this, modelMetaData, NodeBase.InstantiateContext())
                    ) as EntityIterable
                }
            }
        }
    }

    open fun intersect(left: Iterable<Entity>, right: Iterable<Entity>): Iterable<Entity> {
        if (left === right) {
            return left
        }
        if (left.isEmpty || right.isEmpty) {
            return EMPTY
        }
        return if (left is EntityIterable && right is EntityIterable) {
            @Suppress("USELESS_CAST")
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
        return if (left is EntityIterable && right is EntityIterable) {
            @Suppress("USELESS_CAST")
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

        return if (left is EntityIterable && right is EntityIterable) {
            @Suppress("USELESS_CAST")
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
        return if (left is EntityIterable && right is EntityIterable) {
            @Suppress("USELESS_CAST")
            (left as EntityIterable).minus(right as EntityIterable)
        } else {
            inMemoryExclude(left, right)
        }
    }

    open fun selectDistinct(it: Iterable<Entity>?, linkName: String): Iterable<Entity> {
        return if (it is EntityIterable) {
            it.selectDistinct(linkName)
        } else {
            it?.let { inMemorySelectDistinct(it, linkName) } ?: OQueryEntityIterableBase.EMPTY
        }

    }

    open fun selectManyDistinct(it: Iterable<Entity>?, linkName: String): Iterable<Entity> {
        return if (it is EntityIterable) {
            it.selectManyDistinct(linkName)
        } else {
            return it?.let { inMemorySelectManyDistinct(it, linkName) } ?: OQueryEntityIterableBase.EMPTY
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

    internal open fun inMemorySelectDistinct(it: Iterable<Entity>, linkName: String): Iterable<Entity> {
        val result = it.asSequence().mapNotNull { it.getLink(linkName) }.distinct()
        return InMemoryEntityIterable(result.asIterable(), txn = persistentStore.andCheckCurrentTransaction, this)
    }

    internal open fun inMemorySelectManyDistinct(it: Iterable<Entity>, linkName: String): Iterable<Entity> {
        val result = it.asSequence().flatMap { it.getLinks(linkName) }.filterNotNull().distinct()
        return InMemoryEntityIterable(result.asIterable(), txn = persistentStore.andCheckCurrentTransaction, this)
    }

    /*
    Warning all data is in memory
     */
    internal open fun inMemoryIntersect(left: Iterable<Entity>, right: Iterable<Entity>): Iterable<Entity> {
        val ids: EntityIdSet
        val sequence: kotlin.sequences.Sequence<Entity>

        val txn = persistentStore.andCheckCurrentTransaction
        if (left is OQueryEntityIterable) {
            //May be rewrite it. Constant from nowhere
            val rightValues = right.take(20)
            if (rightValues.size < 20) {
                return OIntersectionEntityIterable(
                    txn as OStoreTransaction,
                    left,
                    OMultipleEntitiesIterable(txn, rightValues.toList())
                )
            } else {
                ids = getAsEntityIdSet(left)
                sequence = right.asSequence()
            }
        } else if (right is OQueryEntityIterable) {
            val leftValues = left.take(20)
            if (leftValues.size < 20) {
                return OIntersectionEntityIterable(
                    txn as OStoreTransaction,
                    right,
                    OMultipleEntitiesIterable(txn, leftValues.toList())
                )
            } else {
                ids = getAsEntityIdSet(left)
                sequence = right.asSequence()
            }
        } else {
            // may be there will be some better optimization here
            ids = getAsEntityIdSet(left)
            sequence = right.asSequence()
        }
        val result = if (ids.isEmpty) sequenceOf() else sequence.filter { it.id in ids }

        return InMemoryEntityIterable(result.asIterable(), txn = txn, this)
    }

    internal open fun inMemoryUnion(left: Iterable<Entity>, right: Iterable<Entity>): Iterable<Entity> {
        val result = left.union(right)
        return InMemoryEntityIterable(result.asIterable(), txn = persistentStore.andCheckCurrentTransaction, this)
    }

    internal open fun inMemoryConcat(left: Iterable<Entity>, right: Iterable<Entity>): Iterable<Entity> {
        val result = left.toMutableList().apply { addAll(right) }
        return InMemoryEntityIterable(result.asIterable(), txn = persistentStore.andCheckCurrentTransaction, this)
    }

    internal open fun inMemoryExclude(left: Iterable<Entity>, right: Iterable<Entity>): Iterable<Entity> {
        val ids = getAsEntityIdSet(right)
        val result = if (ids.isEmpty) left.asSequence() else left.asSequence().filter { it.id !in ids }
        return InMemoryEntityIterable(result.asIterable(), txn = persistentStore.andCheckCurrentTransaction, this)
    }

    private fun getAsEntityIdSet(entities: Iterable<Entity>): EntityIdSet {
        var ids = EntityIdSetFactory.newSet()
        entities.forEach {
            ids = ids.add(it.id)
        }
        return ids
    }
}

private val Iterable<Entity>?.isEmpty: Boolean
    get() {
        return this == null || this === EMPTY || this is StaticTypedIterableDecorator && decorated === EMPTY
    }

private val Iterable<Entity>?.isPersistent: Boolean get() = this is EntityIterableBase


