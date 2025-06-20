package jetbrains.exodus.entitystore.youtrackdb.gremlin

import com.jetbrains.youtrack.db.api.gremlin.YTDBVertex
import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.EntityIterable
import jetbrains.exodus.entitystore.StoreTransaction
import jetbrains.exodus.entitystore.youtrackdb.YTDBEntityId
import jetbrains.exodus.entitystore.youtrackdb.YTDBEntityStore
import jetbrains.exodus.entitystore.youtrackdb.YTDBStoreTransaction
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal

interface GremlinEntityIterable : EntityIterable {

    companion object {
        @JvmStatic
        fun create(entityType: String, tx: YTDBStoreTransaction, query: GremlinQuery) =
            GremlinEntityIterableImpl(tx, GremlinQuery.HasLabel(entityType).andThen(query));
    }

    fun selectMany(linkName: String): EntityIterable
}

class GremlinEntityIterableImpl(
    private val tx: YTDBStoreTransaction,
    private val query: GremlinQuery
) : GremlinEntityIterable {


    private val oStore: YTDBEntityStore = tx.getOEntityStore()

    @Volatile
    private var cachedSize: Long = -1

    private fun modify(query: GremlinQuery): GremlinEntityIterableImpl =
        GremlinEntityIterableImpl(tx, this.query.andThen(query))

    private fun iterator(traversal: GraphTraversal<*, YTDBVertex>): GremlinEntityIterator =
        GremlinEntityIterator(
            traversal.iterator(),
            oStore,
            disposeResources = { traversal.close() }
        )

    private fun traversal(): GraphTraversal<*, YTDBVertex> =
        query.traverse(oStore.requireActiveTransaction().traversal())

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

    override fun intersect(right: EntityIterable): EntityIterable {
        TODO("Not yet implemented")
    }

    override fun intersectSavingOrder(right: EntityIterable): EntityIterable {
        TODO("Not yet implemented")
    }

    override fun union(right: EntityIterable): EntityIterable {
        TODO("Not yet implemented")
    }

    override fun minus(right: EntityIterable): EntityIterable {
        TODO("Not yet implemented")
    }

    override fun concat(right: EntityIterable): EntityIterable {
        TODO("Not yet implemented")
    }

    override fun skip(number: Int): EntityIterable = modify(GremlinQuery.Skip(number.toLong()))

    override fun take(number: Int): EntityIterable = modify(GremlinQuery.Limit(number.toLong()))

    override fun distinct(): EntityIterable = modify(GremlinQuery.Dedup)

    override fun selectDistinct(linkName: String): EntityIterable = selectManyDistinct(linkName)

    override fun selectMany(linkName: String): EntityIterable =
        modify(GremlinQuery.Link(linkName))

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

    override fun reverse(): EntityIterable = modify(GremlinQuery.Reverse)

    override fun isSortResult(): Boolean = false

    override fun asSortResult(): EntityIterable = this

    override fun unwrap(): EntityIterable = this

    override fun findLinks(
        entities: EntityIterable,
        linkName: String
    ): EntityIterable? {
        TODO("Not yet implemented")
    }
}