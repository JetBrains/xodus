package jetbrains.exodus.entitystore.youtrackdb.gremlin

import com.jetbrains.youtrack.db.api.gremlin.YTDBVertex
import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.EntityIterable
import jetbrains.exodus.entitystore.EntityIterator
import jetbrains.exodus.entitystore.StoreTransaction
import jetbrains.exodus.entitystore.youtrackdb.YTDBEntityStore
import jetbrains.exodus.entitystore.youtrackdb.YTDBStoreTransaction
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.structure.Vertex

interface GremlinEntityIterable : EntityIterable {

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

    private fun iterator(traversal: GraphTraversal<*, YTDBVertex>): EntityIterator {

        val gremlinVertices = traversal.iterator()

        return GremlinEntityIterator(
            gremlinVertices,
            oStore,
            disposeResources = { traversal.close() }
        )
    }

    private fun traversal(): GraphTraversal<*, YTDBVertex> =
        query.traverse(oStore.requireActiveTransaction().traversal())

    override fun iterator(): EntityIterator = iterator(traversal())

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

    override fun contains(entity: Entity): Boolean {
        TODO("Not yet implemented")
    }

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

    override fun skip(number: Int): EntityIterable = modify(GremlinQuerySkip(number.toLong()))

    override fun take(number: Int): EntityIterable = modify(GremlinQueryLimit(number.toLong()))

    override fun distinct(): EntityIterable {
        TODO("Not yet implemented")
    }

    override fun selectDistinct(linkName: String): EntityIterable {
        TODO("Not yet implemented")
    }

    override fun selectManyDistinct(linkName: String): EntityIterable {
        TODO("Not yet implemented")
    }

    override fun getFirst(): Entity? {
        TODO("Not yet implemented")
    }

    override fun getLast(): Entity? {
        TODO("Not yet implemented")
    }

    override fun reverse(): EntityIterable {
        TODO("Not yet implemented")
    }

    override fun isSortResult(): Boolean {
        TODO("Not yet implemented")
    }

    override fun asSortResult(): EntityIterable {
        TODO("Not yet implemented")
    }

    override fun unwrap(): EntityIterable {
        TODO("Not yet implemented")
    }

    override fun findLinks(
        entities: EntityIterable,
        linkName: String
    ): EntityIterable? {
        TODO("Not yet implemented")
    }
}