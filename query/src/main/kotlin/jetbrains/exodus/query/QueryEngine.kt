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
import jetbrains.exodus.entitystore.EntityIterable
import jetbrains.exodus.entitystore.PersistentEntityStoreImpl
import jetbrains.exodus.entitystore.StoreTransaction
import jetbrains.exodus.entitystore.iterate.EntityIterableBase
import jetbrains.exodus.entitystore.iterate.EntityIterableBase.EMPTY
import jetbrains.exodus.entitystore.iterate.SingleEntityIterable
import jetbrains.exodus.entitystore.util.EntityIdSetFactory
import jetbrains.exodus.kotlin.notNull
import jetbrains.exodus.query.Or.Companion.or
import jetbrains.exodus.query.Utils.isTypeOf
import jetbrains.exodus.query.metadata.ModelMetaData
import jetbrains.exodus.util.doIfTrue
import mu.KLogging

open class QueryEngine(val modelMetaData: ModelMetaData?, val persistentStore: PersistentEntityStoreImpl) : KLogging() {

    private var _sortEngine: SortEngine? = null

    open var sortEngine: SortEngine?
        get() = _sortEngine
        set(value) {
            _sortEngine = value.notNull.apply { queryEngine = this@QueryEngine }
        }

    val uniqueKeyIndicesEngine = MetaDataAwareUniqueKeyIndicesEngine(persistentStore, modelMetaData)

    open fun queryGetAll(entityType: String): TreeKeepingEntityIterable = query(null, entityType, NodeFactory.all())

    open fun query(entityType: String, tree: NodeBase): TreeKeepingEntityIterable = query(null, entityType, tree)

    open fun query(instance: Iterable<Entity>?, entityType: String, tree: NodeBase): TreeKeepingEntityIterable {
        return TreeKeepingEntityIterable(instance, entityType, tree, this)
    }

    open fun intersect(left: Iterable<Entity>, right: Iterable<Entity>): Iterable<Entity> {
        if (left === right) {
            return left
        }
        if (left.isEmpty || right.isEmpty) {
            return EMPTY
        }
        if (left is TreeKeepingEntityIterable) {
            val leftType = left.getEntityType()
            if (right is TreeKeepingEntityIterable) {
                if (left.instance === right.instance) {
                    val rightType = right.getEntityType()
                    if (isTypeOf(leftType, rightType, modelMetaData.notNull)) {
                        return TreeKeepingEntityIterable(
                            right.instance, leftType,
                            And.and(left.tree, right.tree), left.annotatedTree, right.annotatedTree, this
                        )
                    } else if (isTypeOf(rightType, leftType, modelMetaData.notNull)) {
                        return TreeKeepingEntityIterable(
                            right.instance, rightType,
                            And.and(left.tree, right.tree), left.annotatedTree, right.annotatedTree, this
                        )
                    }
                }
            }
        }
        var staticType: String? = null
        if (left is StaticTypedEntityIterable) {
            val leftType = left.getEntityType()
            if (right is StaticTypedEntityIterable) {
                val rightType = right.getEntityType()
                if (isTypeOf(rightType, leftType, modelMetaData.notNull)) {
                    staticType = rightType
                } else if (isTypeOf(leftType, rightType, modelMetaData.notNull)) {
                    staticType = leftType
                }
                if (leftType == rightType) {
                    if (left.isGetAllTree) {
                        return ExcludeNullStaticTypedEntityIterable(rightType, right, this)
                    }
                    if (right.isGetAllTree) {
                        return ExcludeNullStaticTypedEntityIterable(leftType, left, this)
                    }
                }
            }
        }
        val result = intersectNonTrees(instantiateAndAdjust(left), instantiateAndAdjust(right))
        return staticType?.let { StaticTypedIterableDecorator(staticType, result, this) } ?: result
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
        if (left is TreeKeepingEntityIterable) {
            val leftType = left.getEntityType()
            if (right is TreeKeepingEntityIterable) {
                if (left.instance === right.instance && leftType == right.getEntityType()) {
                    return TreeKeepingEntityIterable(
                        right.instance, leftType,
                        or(left.tree, right.tree), left.annotatedTree, right.annotatedTree, this
                    )
                }
            }
        }
        var staticType: String? = null
        if (left is StaticTypedEntityIterable) {
            val leftType = left.getEntityType()
            if (right is StaticTypedEntityIterable) {
                val rightType = right.getEntityType()
                if (leftType == rightType) {
                    staticType = rightType
                    if (left.isGetAllTree) {
                        return AddNullStaticTypedEntityIterable(staticType, left, right, this)
                    }
                    if (right.isGetAllTree) {
                        return AddNullStaticTypedEntityIterable(staticType, right, left, this)
                    }
                }
            }
        }
        val result = unionNonTrees(instantiateAndAdjust(left), instantiateAndAdjust(right))
        return staticType?.let { StaticTypedIterableDecorator(staticType, result, this) } ?: result
    }

    open fun concat(left: Iterable<Entity>, right: Iterable<Entity>): Iterable<Entity> {
        if (left.isEmpty) {
            return right
        }
        if (right.isEmpty) {
            return left
        }
        if (left is TreeKeepingEntityIterable) {
            val leftType = left.getEntityType()
            if (right is TreeKeepingEntityIterable) {
                if (left.instance === right.instance && leftType == right.getEntityType()) {
                    return TreeKeepingEntityIterable(
                        right.instance, leftType,
                        Concat(left.tree, right.tree), left.annotatedTree, right.annotatedTree, this
                    )
                }
            }
        }
        val staticType = retrieveStaticType(left, right)
        val result = concatNonTrees(instantiateAndAdjust(left), instantiateAndAdjust(right))
        return staticType?.let { StaticTypedIterableDecorator(staticType, result, this) } ?: result
    }

    open fun exclude(left: Iterable<Entity>, right: Iterable<Entity>): Iterable<Entity> {
        if (left.isEmpty || left === right) {
            return EMPTY
        }
        if (right.isEmpty) {
            return left
        }
        if (left is TreeKeepingEntityIterable) {
            val leftType = left.getEntityType()
            if (right is TreeKeepingEntityIterable) {
                if (left.instance === right.instance && leftType == right.getEntityType()) {
                    return TreeKeepingEntityIterable(
                        right.instance, leftType,
                        Minus(left.tree, right.tree), left.annotatedTree, right.annotatedTree, this
                    )
                }
            }
        }
        val staticType = retrieveStaticType(left, right)
        val result = excludeNonTrees(instantiateAndAdjust(left), instantiateAndAdjust(right))
        return staticType?.let { StaticTypedIterableDecorator(staticType, result, this) } ?: result
    }

    open fun selectDistinct(it: Iterable<Entity>?, linkName: String): Iterable<Entity> {
        it ?: return EMPTY
        if (modelMetaData != null) {
            if (it is StaticTypedEntityIterable) {
                val entityType = it.getEntityType()
                toEntityIterable(it).let {
                    if (it.isPersistent) {
                        val emd = modelMetaData.getEntityMetaData(entityType)
                        if (emd != null) {
                            emd.getAssociationEndMetaData(linkName)?.let { aemd ->
                                return StaticTypedIterableDecorator(
                                    aemd.oppositeEntityMetaData.type,
                                    selectDistinctImpl(it as EntityIterableBase, linkName), this
                                )
                            }
                        }
                    }
                }
            } else if (it.isPersistent) {
                return selectDistinctImpl(it as EntityIterableBase, linkName)
            }
        }
        return inMemorySelectDistinct(it, linkName)
    }

    open fun selectManyDistinct(it: Iterable<Entity>?, linkName: String): Iterable<Entity> {
        it ?: return EMPTY
        if (modelMetaData != null) {
            if (it is StaticTypedEntityIterable) {
                val entityType = it.getEntityType()
                toEntityIterable(it).let {
                    if (it.isPersistent) {
                        val emd = modelMetaData.getEntityMetaData(entityType)
                        if (emd != null) {
                            emd.getAssociationEndMetaData(linkName)?.let { aemd ->
                                return StaticTypedIterableDecorator(
                                    aemd.oppositeEntityMetaData.type,
                                    selectManyDistinctImpl(it as EntityIterableBase, linkName), this
                                )
                            }
                        }
                    }
                }
            } else if (it.isPersistent) {
                return selectManyDistinctImpl(it as EntityIterableBase, linkName)
            }
        }
        return inMemorySelectManyDistinct(it, linkName)
    }

    open fun toEntityIterable(it: Iterable<Entity>): Iterable<Entity> {
        return adjustEntityIterable(if (it is StaticTypedEntityIterable) it.instantiate() else it)
    }

    open fun intersectNonTrees(left: Iterable<Entity>, right: Iterable<Entity>): Iterable<Entity> {
        return if (left.isPersistent && right.isPersistent) {
            wrap((left as EntityIterableBase).source.intersect((right as EntityIterableBase).source))
        } else inMemoryIntersect(left, right)
    }

    open fun unionNonTrees(left: Iterable<Entity>, right: Iterable<Entity>): Iterable<Entity> {
        return if (left.isPersistent && right.isPersistent) {
            wrap((left as EntityIterableBase).source.union((right as EntityIterableBase).source))
        } else inMemoryUnion(left, right)
    }

    open fun concatNonTrees(left: Iterable<Entity>, right: Iterable<Entity>): Iterable<Entity> {
        return if (left.isPersistent && right.isPersistent) {
            wrap((left as EntityIterableBase).source.concat((right as EntityIterableBase).source))
        } else inMemoryConcat(left, right)
    }

    open fun excludeNonTrees(left: Iterable<Entity>, right: Iterable<Entity>): Iterable<Entity> {
        return if (left.isPersistent && right.isPersistent) {
            wrap((left as EntityIterableBase).source.minus((right as EntityIterableBase).source))
        } else inMemoryExclude(left, right)
        // subtract
    }

    open fun adjustEntityIterable(it: Iterable<Entity>): Iterable<Entity> {
        if (it === EMPTY) {
            return it
        }
        // try to convert collection to entity iterable.
        if (it is Collection<*>) {
            (it as Collection<Entity>).run {
                if (isEmpty()) return EMPTY
                if (size == 1) {
                    wrap(iterator().next())?.let {
                        return it
                    }
                }
            }
        }
        // wrap with transient iterable
        return if (it.isPersistent && !isWrapped(it)) wrap((it as EntityIterableBase).source) else it
    }

    open fun intersectAdjusted(left: Iterable<Entity>, right: Iterable<Entity>): Iterable<Entity> {
        return intersectNonTrees(adjustEntityIterable(left), adjustEntityIterable(right))
    }

    open fun unionAdjusted(left: Iterable<Entity>, right: Iterable<Entity>): Iterable<Entity> {
        return unionNonTrees(adjustEntityIterable(left), adjustEntityIterable(right))
    }

    open fun concatAdjusted(left: Iterable<Entity>, right: Iterable<Entity>): Iterable<Entity> {
        return concatNonTrees(adjustEntityIterable(left), adjustEntityIterable(right))
    }

    open fun excludeAdjusted(left: Iterable<Entity>, right: Iterable<Entity>): Iterable<Entity> {
        return excludeNonTrees(adjustEntityIterable(left), adjustEntityIterable(right))
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

    open fun wrap(it: EntityIterable): EntityIterable = it

    protected open fun inMemorySelectDistinct(it: Iterable<Entity>, linkName: String): Iterable<Entity> {
        reportInMemoryError()
        val ids = EntityIdSetFactory.newSet()
        return it.asSequence().map { it.getLink(linkName) }.filterNotNull()
            .filter { if (ids.contains(it.id)) false else true.apply { ids.add(it.id) } }.asIterable()
    }

    protected open fun inMemorySelectManyDistinct(it: Iterable<Entity>, linkName: String): Iterable<Entity> {
        reportInMemoryError()
        val ids = EntityIdSetFactory.newSet()
        return it.asSequence().flatMap { it.getLinks(linkName) }.filterNotNull()
            .filter { if (ids.contains(it.id)) false else true.apply { ids.add(it.id) } }.asIterable()
    }

    protected open fun inMemoryIntersect(left: Iterable<Entity>, right: Iterable<Entity>): Iterable<Entity> {
        reportInMemoryError()
        return left.intersect(right)
    }

    protected open fun inMemoryUnion(left: Iterable<Entity>, right: Iterable<Entity>): Iterable<Entity> {
        reportInMemoryError()
        return left.union(right)
    }

    protected open fun inMemoryConcat(left: Iterable<Entity>, right: Iterable<Entity>): Iterable<Entity> {
        reportInMemoryError()
        return left.toMutableList().apply { addAll(right) }
    }

    protected open fun inMemoryExclude(left: Iterable<Entity>, right: Iterable<Entity>): Iterable<Entity> {
        reportInMemoryError()
        return left.minus(right)
    }

    protected open fun wrap(entity: Entity): Iterable<Entity>? {
        return SingleEntityIterable(persistentStore.andCheckCurrentTransaction, entity.id)
    }

    private fun instantiateAndAdjust(it: Iterable<Entity>): Iterable<Entity> {
        return adjustEntityIterable(StaticTypedEntityIterable.instantiate(it))
    }

    private fun selectDistinctImpl(it: EntityIterableBase, linkName: String): Iterable<Entity> {
        assertOperational()
        return wrap(it.source.selectDistinct(linkName))
    }

    private fun selectManyDistinctImpl(it: EntityIterableBase, linkName: String): Iterable<Entity> {
        assertOperational()
        return wrap(it.source.selectManyDistinct(linkName))
    }

    private fun reportInMemoryError() {
        doIfTrue("jetbrains.exodus.query.reportInMemoryQueries") {
            logger.error("QueryEngine does in-memory computations", Throwable())
        }
    }
}

private val Iterable<Entity>?.isEmpty: Boolean
    get() {
        return this == null || this === EMPTY || this is StaticTypedIterableDecorator && decorated === EMPTY
    }

private val Iterable<Entity>?.isPersistent: Boolean get() = this is EntityIterableBase

private val StaticTypedEntityIterable?.isGetAllTree: Boolean get() = this is TreeKeepingEntityIterable && tree is GetAll

private fun retrieveStaticType(left: Iterable<Entity>, right: Iterable<Entity>): String? {
    if (left is StaticTypedEntityIterable) {
        val leftType = left.getEntityType()
        if (right is StaticTypedEntityIterable) {
            val rightType = right.getEntityType()
            if (leftType == rightType) {
                return rightType
            }
        }
    }
    return null
}