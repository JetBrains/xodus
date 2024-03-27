package jetbrains.exodus.entitystore.orientdb.iterate

import jetbrains.exodus.entitystore.EntityIterable
import jetbrains.exodus.entitystore.EntityIterableHandle
import jetbrains.exodus.entitystore.EntityIterator
import jetbrains.exodus.entitystore.PersistentStoreTransaction
import jetbrains.exodus.entitystore.iterate.EntityIterableBase
import jetbrains.exodus.entitystore.orientdb.OEntityIterable
import jetbrains.exodus.entitystore.orientdb.OEntityIterableHandle
import jetbrains.exodus.entitystore.orientdb.iterate.OQueryEntityIterator.Companion.create
import jetbrains.exodus.entitystore.orientdb.iterate.binop.OConcatEntityIterable
import jetbrains.exodus.entitystore.orientdb.iterate.binop.OIntersectionIterable
import jetbrains.exodus.entitystore.orientdb.iterate.binop.OUnionIterable
import jetbrains.exodus.entitystore.orientdb.query.OClassSelect
import jetbrains.exodus.entitystore.orientdb.query.OCountSelect

abstract class OEntityIterableBase(tx: PersistentStoreTransaction?) : EntityIterableBase(tx), OEntityIterable {

    override fun isSortedById() = false
    override fun canBeCached() = false


    private val query by lazy { query() }

    override fun getIteratorImpl(txn: PersistentStoreTransaction): EntityIterator {
        val query = query()
        return create(query, txn)
    }

    override fun getHandleImpl(): EntityIterableHandle {
        return OEntityIterableHandle(query().sql())
    }

    override fun union(right: EntityIterable): EntityIterable {
        if (right is OEntityIterableBase) {
            return OUnionIterable(transaction, this, right)
        }
        return super.union(right)
    }

    override fun intersect(right: EntityIterable): EntityIterable {
        if (right is OEntityIterableBase) {
            return OIntersectionIterable(transaction, this, right)
        }
        return super.intersect(right)
    }

    override fun concat(right: EntityIterable): EntityIterable {
        if (right is OEntityIterableBase) {
            return OConcatEntityIterable(transaction, this, right)
        }
        return super.intersect(right)
    }

    override fun size(): Long {
        val sourceQuery = query
        // ToDo: maybe increase boundary for query return type to OClassSelect
        check(sourceQuery is OClassSelect) { "OEntityIterableBase should be created with OClassSelect" }
        val countQuery = OCountSelect(sourceQuery)

        // ToDo: use session from transaction instead?
        return countQuery.count()
    }

    override fun count(): Long {
        return size()
    }

    override fun getRoughCount(): Long {
        return count()
    }

    override fun getRoughSize(): Long {
        val count = this.count()
        return if (count > -1) count else size()
    }

    override fun asProbablyCached(): EntityIterableBase? {
        return this
    }
}
