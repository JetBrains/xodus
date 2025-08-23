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
package jetbrains.exodus.entitystore.youtrackdb.iterate

import jetbrains.exodus.entitystore.*
import jetbrains.exodus.entitystore.youtrackdb.YTDBEntityIterable
import jetbrains.exodus.entitystore.youtrackdb.YTDBEntityStore
import jetbrains.exodus.entitystore.youtrackdb.YTDBStoreTransaction
import jetbrains.exodus.entitystore.youtrackdb.iterate.binop.YTDBConcatEntityIterable
import jetbrains.exodus.entitystore.youtrackdb.iterate.binop.YTDBIntersectionEntityIterable
import jetbrains.exodus.entitystore.youtrackdb.iterate.binop.YTDBMinusEntityIterable
import jetbrains.exodus.entitystore.youtrackdb.iterate.binop.YTDBUnionEntityIterable
import jetbrains.exodus.entitystore.youtrackdb.iterate.link.YTDBLinkIterableToEntityIterableFiltered
import jetbrains.exodus.entitystore.youtrackdb.iterate.link.YTDBLinkSelectEntityIterable
import jetbrains.exodus.entitystore.youtrackdb.iterate.link.YTDBMultipleEntitiesIterable
import jetbrains.exodus.entitystore.youtrackdb.query.*
import jetbrains.exodus.entitystore.util.unsupported
import jetbrains.exodus.entitystore.youtrackdb.gremlin.GremlinEntityIterable.Companion.EMPTY

abstract class YTDBEntityIterableBase(tx: YTDBStoreTransaction) : YTDBEntityIterable {

    private val oStore: YTDBEntityStore = tx.getOEntityStore()

    companion object {

    }

    override fun iterator(): EntityIterator {
        val currentTx = oStore.requireActiveTransaction()
        val query = query()
        return YTDBQueryEntityIterator.executeAndCreate(query, currentTx)
    }

    override fun getTransaction(): StoreTransaction {
        return oStore.requireActiveTransaction()
    }

    override fun indexOf(entity: Entity): Int {
        val entityId = entity.id
        var result = 0
        val it = iterator()
        try {
            while (it.hasNext()) {
                val nextId = it.nextId()
                if (nextId != null && nextId == entityId) {
                    return result
                }
                ++result
            }
        } finally {
            it.dispose()
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
        return YTDBUnionEntityIterable(transaction as YTDBStoreTransaction, this, right.asOQueryIterable())
    }

    override fun intersectSavingOrder(right: EntityIterable): EntityIterable {
        if (right == EMPTY) {
            return EMPTY
        }
        return intersect(right)
    }

    override fun intersect(right: EntityIterable): EntityIterable {
        if (right == EMPTY) {
            return EMPTY
        }
        return YTDBIntersectionEntityIterable(transaction as YTDBStoreTransaction, this, right.asOQueryIterable())
    }

    override fun concat(right: EntityIterable): EntityIterable {
        if (right == EMPTY) {
            return this
        }
        return YTDBConcatEntityIterable(transaction as YTDBStoreTransaction, this, right.asOQueryIterable())
    }

    override fun distinct(): EntityIterable {
        return YTDBDistinctEntityIterable(transaction as YTDBStoreTransaction, this)
    }

    override fun minus(right: EntityIterable): EntityIterable {
        if (right == EMPTY) {
            return this
        }
        return YTDBMinusEntityIterable(transaction as YTDBStoreTransaction, this, right.asOQueryIterable())
    }

    override fun take(number: Int): EntityIterable {
        return YTDBTakeEntityIterable(transaction as YTDBStoreTransaction, this, number)
    }

    override fun skip(number: Int): EntityIterable {
        return YTDBSkipEntityIterable(transaction as YTDBStoreTransaction, this, number)
    }

    override fun getFirst(): Entity? {
        val lastSelect = YTDBFirstSelect(query())
        return querySingleEntity(lastSelect)
    }

    override fun getLast(): Entity? {
        val lastSelect = YTDBLastSelect(query())
        return querySingleEntity(lastSelect)
    }

    private fun querySingleEntity(query: YTDBQuery): Entity? {
        val currentTx = oStore.requireActiveTransaction()
        val iterator = YTDBQueryEntityIterator.executeAndCreate(query, currentTx)
        try {
            return if (iterator.hasNext()) iterator.next() else null
        } finally {
            iterator.dispose()
        }
    }

    /**
     * **Note:** Takes effect only if the iterable is sorted.
     */
    override fun reverse(): EntityIterable {
        return YTDBReversedEntityIterable(transaction as YTDBStoreTransaction, this)
    }

    fun selectMany(linkName: String): EntityIterable {
        return YTDBLinkSelectEntityIterable(transaction as YTDBStoreTransaction, this, linkName)
    }

    override fun selectManyDistinct(linkName: String): EntityIterable {
        return selectMany(linkName).distinct()
    }

    override fun selectDistinct(linkName: String): EntityIterable {
        return selectManyDistinct(linkName)
    }

    override fun findLinks(entities: EntityIterable, linkName: String): EntityIterable {
        if (entities == EMPTY) {
            return EMPTY
        }
        return YTDBLinkIterableToEntityIterableFiltered(transaction as YTDBStoreTransaction, entities.asOQueryIterable(), linkName, this)
    }

    fun findLinks(entities: Iterable<Entity?>, linkName: String): EntityIterable {
        if (entities !is YTDBEntityIterable) {
            unsupported { "findLinks with non-OrientDB entity iterable" }
        }
        return findLinks(entities, linkName)
    }

    @Volatile
    private var cachedSize: Long = -1

    override fun size(): Long {
        cachedSize = YTDBQueryExecution
            .execute(query().count(), oStore.requireActiveTransaction())
            .use { it.next().getLong("count") }!!
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
        val it = YTDBIntersectionEntityIterable(
            currentTx,
            this,
            YTDBMultipleEntitiesIterable(currentTx, listOf(entity))
        ).iterator()
        try {
            return it.hasNext()
        } finally {
            it.dispose()
        }
    }

    override fun unwrap() = this

    override fun isEmpty(): Boolean {
        val iter = iterator()
        try {
            return !iter.hasNext()
        } finally {
            iter.dispose()
        }
    }

}
