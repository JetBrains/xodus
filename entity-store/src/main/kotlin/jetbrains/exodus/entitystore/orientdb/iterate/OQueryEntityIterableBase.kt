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
package jetbrains.exodus.entitystore.orientdb.iterate

import jetbrains.exodus.entitystore.*
import jetbrains.exodus.entitystore.orientdb.OEntityStore
import jetbrains.exodus.entitystore.orientdb.OQueryEntityIterable
import jetbrains.exodus.entitystore.orientdb.OStoreTransaction
import jetbrains.exodus.entitystore.orientdb.iterate.binop.OConcatEntityIterable
import jetbrains.exodus.entitystore.orientdb.iterate.binop.OIntersectionEntityIterable
import jetbrains.exodus.entitystore.orientdb.iterate.binop.OMinusEntityIterable
import jetbrains.exodus.entitystore.orientdb.iterate.binop.OUnionEntityIterable
import jetbrains.exodus.entitystore.orientdb.iterate.link.OLinkIterableToEntityIterableFiltered
import jetbrains.exodus.entitystore.orientdb.iterate.link.OLinkSelectEntityIterable
import jetbrains.exodus.entitystore.orientdb.iterate.link.OSingleEntityIterable
import jetbrains.exodus.entitystore.orientdb.query.*
import jetbrains.exodus.entitystore.util.unsupported

abstract class OQueryEntityIterableBase(tx: OStoreTransaction) : OQueryEntityIterable {

    private val oStore: OEntityStore = tx.getOEntityStore()

    companion object {

        val EMPTY = object : OQueryEntityIterable {

            override fun iterator(): EntityIterator = OQueryEntityIterator.EMPTY
            override fun query(): OSelect = unsupported { "Should never be called" }
            override fun getTransaction(): StoreTransaction = unsupported { "Should never be called" }
            override fun isEmpty(): Boolean = true
            override fun indexOf(entity: Entity): Int = -1
            override fun contains(entity: Entity): Boolean = false
            override fun isSortResult(): Boolean = true
            override fun asSortResult(): EntityIterable = this
            override fun size() = 0L
            override fun getRoughSize() = 0L
            override fun count() = 0L
            override fun getRoughCount() = 0L
            override fun union(right: EntityIterable) = right
            override fun concat(right: EntityIterable) = right
            override fun skip(number: Int): EntityIterable = this
            override fun take(number: Int): EntityIterable = this
            override fun intersect(right: EntityIterable) = this
            override fun intersectSavingOrder(right: EntityIterable): EntityIterable = right
            override fun distinct() = this
            override fun minus(right: EntityIterable) = this
            override fun selectManyDistinct(linkName: String) = this
            override fun getFirst(): Entity? = null
            override fun getLast(): Entity? = null
            override fun reverse(): EntityIterable = this
            override fun selectDistinct(linkName: String) = this
        }
    }

    override fun iterator(): EntityIterator {
        val currentTx = oStore.requireActiveTransaction()
        val query = query()
        return OQueryEntityIterator.executeAndCreate(query, currentTx)
    }

    override fun getTransaction(): StoreTransaction {
        return oStore.requireActiveTransaction()
    }

    override fun indexOf(entity: Entity): Int {
        val entityId = entity.id
        var result = 0
        val it = iterator()
        while (it.hasNext()) {
            val nextId = it.nextId()
            if (nextId != null && nextId == entityId) {
                return result
            }
            ++result
        }
        return -1
    }

    override fun isSortResult(): Boolean {
        return false
    }

    override fun asSortResult(): EntityIterable {
        return this
    }

    override fun union(right: EntityIterable): EntityIterable {
        if (right == EMPTY) {
            return this
        }
        return OUnionEntityIterable(transaction as OStoreTransaction, this, right.asOQueryIterable())
    }

    override fun intersectSavingOrder(right: EntityIterable): EntityIterable {
        if (right == EMPTY) {
            return this
        }
        return intersect(right)
    }

    override fun intersect(right: EntityIterable): EntityIterable {
        if (right == EMPTY) {
            return this
        }
        return OIntersectionEntityIterable(transaction as OStoreTransaction, this, right.asOQueryIterable())
    }

    override fun concat(right: EntityIterable): EntityIterable {
        if (right == EMPTY) {
            return this
        }
        return OConcatEntityIterable(transaction as OStoreTransaction, this, right.asOQueryIterable())
    }

    override fun distinct(): EntityIterable {
        return ODistinctEntityIterable(transaction as OStoreTransaction, this)
    }

    override fun minus(right: EntityIterable): EntityIterable {
        if (right == EMPTY) {
            return this
        }
        return OMinusEntityIterable(transaction as OStoreTransaction, this, right.asOQueryIterable())
    }

    override fun take(number: Int): EntityIterable {
        return OTakeEntityIterable(transaction as OStoreTransaction, this, number)
    }

    override fun skip(number: Int): EntityIterable {
        return OSkipEntityIterable(transaction as OStoreTransaction, this, number)
    }

    override fun getFirst(): Entity? {
        val lastSelect = OFirstSelect(query())
        return querySingleEntity(lastSelect)
    }

    override fun getLast(): Entity? {
        val lastSelect = OLastSelect(query())
        return querySingleEntity(lastSelect)
    }

    private fun querySingleEntity(query: OQuery): Entity? {
        val currentTx = oStore.requireActiveTransaction()
        val iterator = OQueryEntityIterator.executeAndCreate(query, currentTx)
        return if (iterator.hasNext()) {
            iterator.next()
        } else {
            null
        }
    }

    /**
     * **Note:** Takes effect only if the iterable is sorted.
     */
    override fun reverse(): EntityIterable {
        return OReversedEntityIterable(transaction as OStoreTransaction, this)
    }

    fun selectMany(linkName: String): EntityIterable {
        return OLinkSelectEntityIterable(transaction as OStoreTransaction, this, linkName)
    }

    override fun selectManyDistinct(linkName: String): EntityIterable {
        return selectMany(linkName).distinct()
    }

    override fun selectDistinct(linkName: String): EntityIterable {
        return selectManyDistinct(linkName)
    }

    fun findLinks(entities: EntityIterable, linkName: String): EntityIterable {
        if (entities == EMPTY) {
            return EMPTY
        }
        return OLinkIterableToEntityIterableFiltered(transaction as OStoreTransaction, entities.asOQueryIterable(), linkName, this)
    }

    fun findLinks(entities: Iterable<Entity?>, linkName: String): EntityIterable {
        if (entities !is OQueryEntityIterable) {
            unsupported { "findLinks with non-OrientDB entity iterable" }
        }
        return findLinks(entities, linkName)
    }

    @Volatile
    private var cachedSize: Long = -1

    override fun size(): Long {
        val currentTx = oStore.requireActiveTransaction()
        val sourceQuery = query()
        val countQuery = OCountSelect(sourceQuery.withOrder(EmptyOrder))
        cachedSize = countQuery.count(currentTx)
        return cachedSize
    }

    override fun getRoughSize(): Long {
        val size = cachedSize
        return if (size != -1L) size else size()
    }

    override fun count(): Long {
        return size()
    }

    override fun getRoughCount(): Long {
        return getRoughSize()
    }

    override fun contains(entity: Entity): Boolean {
        val currentTx = oStore.requireActiveTransaction()
        return OIntersectionEntityIterable(currentTx, this, OSingleEntityIterable(currentTx, entity)).iterator().hasNext()
    }

    override fun unwrap() = this

    override fun isEmpty(): Boolean {
        val iter = iterator()
        val result = iter.hasNext()
        iter.dispose()
        return !result
    }

}
