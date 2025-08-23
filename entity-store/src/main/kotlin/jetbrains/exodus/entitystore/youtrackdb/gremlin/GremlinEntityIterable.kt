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
package jetbrains.exodus.entitystore.youtrackdb.gremlin

import com.jetbrains.youtrack.db.api.gremlin.YTDBVertex
import jetbrains.exodus.entitystore.*
import jetbrains.exodus.entitystore.util.unsupported
import jetbrains.exodus.entitystore.youtrackdb.YTDBEntityId
import jetbrains.exodus.entitystore.youtrackdb.YTDBEntityStore
import jetbrains.exodus.entitystore.youtrackdb.YTDBStoreTransaction
import jetbrains.exodus.entitystore.youtrackdb.gremlin.GremlinEntityIterable.Companion.EMPTY
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal

interface GremlinEntityIterable : EntityIterable {
    companion object {
        @JvmStatic
        fun where(entityType: String, tx: YTDBStoreTransaction, condition: GremlinBlock): GremlinEntityIterable =
            query(
                tx,
                GremlinQuery.all
                    .then(condition)
                    .then(GremlinBlock.HasLabel(entityType))
            )

        @JvmStatic
        fun query(tx: YTDBStoreTransaction, query: GremlinQuery) =
            GremlinEntityIterableImpl(tx, query);

        val EMPTY = object : GremlinEntityIterable {

            override fun iterator(): EntityIterator = GremlinEntityIterator.EMPTY
            override fun selectMany(linkName: String): EntityIterable = this
            override val query: GremlinQuery get() = unsupported { "Should never be called" }
            override fun unwrap(): EntityIterable = this
            override fun getTransaction(): StoreTransaction = unsupported { "Should never be called" }
            override fun isEmpty(): Boolean = true
            override fun indexOf(entity: Entity): Int = -1
            override fun contains(entity: Entity): Boolean = false
            override fun isSortResult(): Boolean = true
            override fun asSortResult(): EntityIterable = this
            override fun findLinks(entities: EntityIterable, linkName: String): EntityIterable = this
            override fun size() = 0L
            override fun getRoughSize() = 0L
            override fun count() = 0L
            override fun getRoughCount() = 0L
            override fun union(right: EntityIterable) = right
            override fun concat(right: EntityIterable) = right
            override fun skip(number: Int): EntityIterable = this
            override fun take(number: Int): EntityIterable = this
            override fun intersect(right: EntityIterable) = this
            override fun intersectSavingOrder(right: EntityIterable): EntityIterable = this
            override fun distinct() = this
            override fun minus(right: EntityIterable) = this
            override fun selectManyDistinct(linkName: String) = this
            override fun getFirst(): Entity? = null
            override fun getLast(): Entity? = null
            override fun reverse(): EntityIterable = this
            override fun selectDistinct(linkName: String) = this
        }
    }

    fun selectMany(linkName: String): EntityIterable

    val query: GremlinQuery
}

class GremlinEntityIterableImpl(
    private val tx: YTDBStoreTransaction,
    override val query: GremlinQuery
) : GremlinEntityIterable {


    private val oStore: YTDBEntityStore = tx.getOEntityStore()

    @Volatile
    private var cachedSize: Long = -1

    private fun modify(block: GremlinBlock): GremlinEntityIterableImpl =
        GremlinEntityIterableImpl(tx, this.query.then(block))

    private fun iterator(traversal: GraphTraversal<*, YTDBVertex>): GremlinEntityIterator =
        GremlinEntityIterator.of(traversal, oStore)

    private fun traversal(): GraphTraversal<*, YTDBVertex> =
        query.start(oStore.requireActiveTransaction().g())

    override fun iterator(): GremlinEntityIterator = iterator(traversal())

    override fun getTransaction(): StoreTransaction = oStore.requireActiveTransaction()

    override fun isEmpty(): Boolean {
        val iter = iterator()
        try {
            return !iter.hasNext()
        } finally {
            iter.dispose()
        }
    }

    override fun size(): Long {
        cachedSize = traversal().count().use { it.next() }
        return cachedSize
    }

    override fun count(): Long = size()

    override fun getRoughCount(): Long = roughSize

    override fun getRoughSize(): Long {
        val size = cachedSize
        return if (size != -1L) size else size()
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

    override fun contains(entity: Entity): Boolean = traversal()
        .hasId((entity.id as YTDBEntityId).asOId())
        .use { it.hasNext() }

    override fun intersect(right: EntityIterable): EntityIterable =
        if (right === EMPTY) EMPTY
        else GremlinEntityIterableImpl(tx, query.intersect(right.asGremlinIterable().query))

    override fun intersectSavingOrder(right: EntityIterable): EntityIterable {
        TODO("Not yet implemented")
    }

    override fun union(right: EntityIterable): EntityIterable =
        if (right === EMPTY) this
        else GremlinEntityIterableImpl(tx, query.union(right.asGremlinIterable().query))

    override fun minus(right: EntityIterable): EntityIterable =
        if (right === EMPTY) this
        else GremlinEntityIterableImpl(tx, query.difference(right.asGremlinIterable().query))

    override fun concat(right: EntityIterable): EntityIterable =
        if (right === EMPTY) this
        else GremlinEntityIterableImpl(tx, query.unionAll(right.asGremlinIterable().query))

    override fun skip(number: Int): EntityIterable = modify(GremlinBlock.Skip(number.toLong()))

    override fun take(number: Int): EntityIterable = modify(GremlinBlock.Limit(number.toLong()))

    override fun distinct(): EntityIterable = modify(GremlinBlock.Dedup)

    override fun selectDistinct(linkName: String): EntityIterable = selectManyDistinct(linkName)

    override fun selectMany(linkName: String): EntityIterable =
        GremlinEntityIterable.query(
            tx,
            GremlinQuery.FollowLink(
                this.query,
                GremlinQuery.LinkDirection.OUT,
                linkName,
            )
        )

    override fun selectManyDistinct(linkName: String): EntityIterable =
        selectMany(linkName).distinct()

    override fun getFirst(): Entity? =
        iterator(traversal().limit(1)).use {
            if (it.hasNext()) it.next() else null
        }

    override fun getLast(): Entity? =
        iterator(traversal().tail()).use {
            if (it.hasNext()) return it.next() else null
        }

    override fun reverse(): EntityIterable = modify(GremlinBlock.Reverse)

    override fun isSortResult(): Boolean = false

    override fun asSortResult(): EntityIterable = this

    override fun unwrap(): EntityIterable = this

    override fun findLinks(
        entities: EntityIterable,
        linkName: String
    ): EntityIterable {
        return GremlinEntityIterableImpl(
            this.tx,
            entities
                .asGremlinIterable()
                .query
                .then(GremlinBlock.InLink(linkName))
        )
            .distinct()
    }
}