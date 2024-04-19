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
import jetbrains.exodus.entitystore.iterate.EntityIterableBase
import jetbrains.exodus.entitystore.iterate.EntityIterableBase.EMPTY
import jetbrains.exodus.entitystore.iterate.SingleEntityIterable
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
        return (left as EntityIterable).intersect(right as EntityIterable)
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
        return (left as EntityIterable).union(right as EntityIterable)
    }

    open fun concat(left: Iterable<Entity>, right: Iterable<Entity>): Iterable<Entity> {
        if (left.isEmpty) {
            return right
        }
        if (right.isEmpty) {
            return left
        }
        return (left as EntityIterable).concat(right as EntityIterable)
    }

    open fun exclude(left: Iterable<Entity>, right: Iterable<Entity>): Iterable<Entity> {
        if (left.isEmpty || left === right) {
            return EMPTY
        }
        if (right.isEmpty) {
            return left
        }
        return (left as EntityIterable).minus(right as EntityIterable)
    }

    open fun selectDistinct(it: Iterable<Entity>?, linkName: String): Iterable<Entity> {
        return (it as EntityIterable).selectDistinct(linkName)
    }

    open fun selectManyDistinct(it: Iterable<Entity>?, linkName: String): Iterable<Entity> {
        return (it as EntityIterable).selectDistinct(linkName)
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
}

private val Iterable<Entity>?.isEmpty: Boolean
    get() {
        return this == null || this === EMPTY || this is StaticTypedIterableDecorator && decorated === EMPTY
    }

private val Iterable<Entity>?.isPersistent: Boolean get() = this is EntityIterableBase

