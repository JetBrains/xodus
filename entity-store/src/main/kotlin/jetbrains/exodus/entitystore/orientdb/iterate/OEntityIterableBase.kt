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
import jetbrains.exodus.entitystore.orientdb.iterate.binop.OIntersectionEntityIterable
import jetbrains.exodus.entitystore.orientdb.iterate.binop.OMinusEntityIterable
import jetbrains.exodus.entitystore.orientdb.iterate.binop.OUnionEntityIterable
import jetbrains.exodus.entitystore.util.unsupported

abstract class OEntityIterableBase(tx: PersistentStoreTransaction?) : EntityIterableBase(tx), OEntityIterable {

    override fun getIteratorImpl(txn: PersistentStoreTransaction): EntityIterator {
        val query = query()
        return create(query, txn)
    }

    override fun getHandleImpl(): EntityIterableHandle {
        return OEntityIterableHandle(query().sql())
    }

    override fun union(right: EntityIterable): EntityIterable {
        if (right is OEntityIterableBase) {
            return OUnionEntityIterable(transaction, this, right)
        } else {
            unsupported { "Union with non-OrientDB entity iterable" }
        }
    }

    override fun intersectSavingOrder(right: EntityIterable): EntityIterable {
        return intersect(right)
    }

    override fun intersect(right: EntityIterable): EntityIterable {
        if (right is OEntityIterableBase) {
            return OIntersectionEntityIterable(transaction, this, right)
        } else {
            unsupported { "Intersecting with non-OrientDB entity iterable" }
        }
    }

    override fun concat(right: EntityIterable): EntityIterable {
        if (right is OEntityIterableBase) {
            return OConcatEntityIterable(transaction, this, right)
        } else {
            unsupported { "Concat with non-OrientDB entity iterable" }
        }
    }

    override fun distinct(): EntityIterable {
        return ODistinctEntityIterable(transaction, this)
    }

    override fun minus(right: EntityIterable): EntityIterable {
        if (right is OEntityIterableBase) {
            return OMinusEntityIterable(transaction, this, right)
        } else {
            unsupported { "Minus with non-OrientDB entity iterable" }
        }
    }

    override fun asSortResult(): EntityIterable {
        return this
    }

    override fun size(): Long {
        unsupported()
    }

    override fun count(): Long {
        return -1
    }

    override fun getRoughCount(): Long {
        return -1
    }

    override fun getRoughSize(): Long {
        unsupported()
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
