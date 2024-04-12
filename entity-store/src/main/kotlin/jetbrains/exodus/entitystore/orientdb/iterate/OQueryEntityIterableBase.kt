package jetbrains.exodus.entitystore.orientdb.iterate

import jetbrains.exodus.entitystore.EntityIterable
import jetbrains.exodus.entitystore.EntityIterableHandle
import jetbrains.exodus.entitystore.EntityIterator
import jetbrains.exodus.entitystore.StoreTransaction
import jetbrains.exodus.entitystore.asOStoreTransaction
import jetbrains.exodus.entitystore.iterate.EntityIterableBase
import jetbrains.exodus.entitystore.orientdb.OEntityIterableHandle
import jetbrains.exodus.entitystore.orientdb.OQueryEntityIterable
import jetbrains.exodus.entitystore.orientdb.OStoreTransaction
import jetbrains.exodus.entitystore.orientdb.iterate.OQueryEntityIterator.Companion.create
import jetbrains.exodus.entitystore.orientdb.iterate.binop.OConcatEntityIterable
import jetbrains.exodus.entitystore.orientdb.iterate.binop.OIntersectionEntityIterable
import jetbrains.exodus.entitystore.orientdb.iterate.binop.OMinusEntityIterable
import jetbrains.exodus.entitystore.orientdb.iterate.binop.OUnionEntityIterable
import jetbrains.exodus.entitystore.orientdb.iterate.link.OLinkSelectEntityIterable
import jetbrains.exodus.entitystore.orientdb.query.OCountSelect
import jetbrains.exodus.entitystore.util.unsupported
import java.util.concurrent.Executors

abstract class OQueryEntityIterableBase(tx: StoreTransaction?) : EntityIterableBase(tx), OQueryEntityIterable {

    private val oStoreTransaction: OStoreTransaction? = tx?.asOStoreTransaction()

    override fun getIteratorImpl(txn: StoreTransaction): EntityIterator {
        val query = query()
        return create(query, txn.asOStoreTransaction())
    }

    override fun getHandleImpl(): EntityIterableHandle {
        return OEntityIterableHandle(query().sql())
    }

    override fun union(right: EntityIterable): EntityIterable {
        if (right is OQueryEntityIterable) {
            return OUnionEntityIterable(transaction, this, right)
        } else {
            unsupported { "Union with non-OrientDB entity iterable" }
        }
    }

    override fun intersectSavingOrder(right: EntityIterable): EntityIterable {
        return intersect(right)
    }

    override fun intersect(right: EntityIterable): EntityIterable {
        if (right is OQueryEntityIterable) {
            return OIntersectionEntityIterable(transaction, this, right)
        } else {
            unsupported { "Intersecting with non-OrientDB entity iterable" }
        }
    }

    override fun concat(right: EntityIterable): EntityIterable {
        if (right is OQueryEntityIterable) {
            return OConcatEntityIterable(transaction, this, right)
        } else {
            unsupported { "Concat with non-OrientDB entity iterable" }
        }
    }

    override fun distinct(): EntityIterable {
        return this
        //return ODistinctEntityIterable(transaction, this)
    }

    override fun minus(right: EntityIterable): EntityIterable {
        if (right is OQueryEntityIterable) {
            return OMinusEntityIterable(transaction, this, right)
        } else {
            unsupported { "Minus with non-OrientDB entity iterable" }
        }
    }

    override fun selectMany(linkName: String): EntityIterable {
        return OLinkSelectEntityIterable(transaction, this, linkName)
    }

    override fun selectManyDistinct(linkName: String): EntityIterable {
        return selectMany(linkName).distinct()
    }

    override fun selectDistinct(linkName: String): EntityIterable {
        return selectManyDistinct(linkName)
    }

    override fun asSortResult(): EntityIterable {
        return this
    }

    @Volatile
    private var cachedCount: Long = -1

    // ToDo: get executor from persistent store
    val executor = Executors.newSingleThreadExecutor()

    override fun size(): Long {
        val sourceQuery = query()
        val countQuery = OCountSelect(sourceQuery)
        return countQuery.count(oStoreTransaction?.activeSession)
    }

    override fun count(): Long {
        val count = cachedCount
        if (count == -1L) {
            executor.submit {
                cachedCount = size()
            }
        }
        return count
    }

    override fun getRoughCount(): Long {
        return cachedCount
    }

    override fun getRoughSize(): Long {
        val count = cachedCount
        return if (count > -1) count else size()
    }

    override fun isSortedById(): Boolean {
        return false
    }

    override fun canBeCached(): Boolean {
        return false
    }

    override fun asProbablyCached(): EntityIterableBase? {
        return this
    }
}
